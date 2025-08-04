import os
import io
import csv
import psycopg2
import pyodbc
from dotenv import load_dotenv
from typing import List

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.documents import Document
from langchain_core.tools import tool
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
import os
import shelve


# Init
load_dotenv()
# Always resolve vector store relative to project root, not src/
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
rag_directory = os.path.join(project_root, "db", "chroma_db_firecrawl")
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

if not os.path.exists(rag_directory):
    print("Vector store not found. Initializing vector store using webrawler.py first.")
    exit(1)

else:
    print("Vector store exists. Loading existing vector store.")
    db = Chroma(persist_directory=rag_directory, embedding_function=embeddings)

retriever = db.as_retriever(search_type="similarity", search_kwargs={"k": 5})



# 🔧 Definice run_sqlserver_query_as_csv Tool metody
@tool
def run_sqlserver_query_as_csv(query: str, params: tuple = (), max_rows: int = 50) -> str:
    """
    Executes a SQL Server query and returns up to `max_rows` results as CSV string.
    This method allows you query_store or other views to get information about metadata and statistics.
    Avoid calling the same query multiple times
    """
    conn = None
    try:
        server = os.getenv("MSSQL_HOST")       # e.g. bayer.cs.vsb.cz\SQLDB
        database = os.getenv("MSSQL_DB")       # e.g. sqlbench
        username = os.getenv("MSSQL_USER")     # e.g. sqlbench
        password = os.getenv("MSSQL_PASSWORD") # your password

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=yes;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        try:
            cursor.execute(query, params)
        except pyodbc.ProgrammingError as e:
            return f"Query execution error: {e}"

        while cursor.description is None:
            if not cursor.nextset():
                return "The query executed but returned no tabular results."

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchmany(max_rows)

        if not rows:
            return ""

        output = io.StringIO()
        writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(columns)
        writer.writerows(rows)

        result = output.getvalue()

         # limit maximum lebgth of CSV to prevent excessive output
        MAX_CSV_LENGTH = 100000
        if len(result) > MAX_CSV_LENGTH:
            return result[:MAX_CSV_LENGTH] + "\n...[TRUNCATED]"

        return result
    finally:
        if conn:
            conn.close()

# class SubmitResultInput(BaseModel):
#     short_name: str = Field(..., description="Short name of the recommendation.", max_length=100)
#     recommendation: str = Field(..., description="The text of the recommendation.")
#     recommendation_category: str = Field(..., description="The category/type of the recommendation.")

@tool
def submit_result(short_name: str, recommendation: str, recommendation_category: str ):
    """
    Submit a recommendation to the agent_result database as a triplet (short_name, recommendation, recommendation_type).
    `short_name` parameter should be a short name of the recommendation.
    `recommendation` parameter should be a string in MarkDown format.
    `recommendation_category` parameter should be a string describing the type of recommendation.
    """

    if not short_name.strip() or not recommendation.strip() or not recommendation_category.strip():
        return "submit_result error: Both recommendation and recommendation_type must be non-empty strings."

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("AGENT_RESULTS_DB"),
            user=os.getenv("AGENT_RESULTS_USER"),
            password=os.getenv("AGENT_RESULTS_PASSWORD"),
            host=os.getenv("AGENT_RESULTS_HOST"),
            port=os.getenv("AGENT_RESULTS_PORT", 5432),
        )
        run_name = os.getenv("RUN_NAME", "default_run")
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO recommendation (run_name, short_name, recommendation, recommendation_type) VALUES (%s, %s, %s, %s)",
                (run_name, short_name, recommendation, recommendation_category)
            )
            conn.commit()
            return "Recommendation submitted successfully."

    except Exception as e:
        return f"submit_result, error running query: {e}"
    finally:
        if conn:
            conn.close()

@tool
def query_knowledge_base(query: str) -> str:
    """
    Hledá vektorově odpovědi na dotaz v knowledge base.
    Vrací sloučený text nejrelevantnějších dokumentů.
    Obsahuje zejména dokumenty z webu Brent Ozar, které se týkají SQL Serveru a jeho optimalizace.
    """
    relevant_docs: List[Document] = retriever.invoke(query)
    if not relevant_docs:
        return "No relevant information found in knowledge base."

    result = "\n\n---\n\n".join(doc.page_content for doc in relevant_docs)
    return result



prompt_template = ChatPromptTemplate.from_messages([
    ("system", "Odpovez na otazku jak nejlepe umis."),
    MessagesPlaceholder(variable_name="chat_history"),
    ("user", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad")
])

tools = [run_sqlserver_query_as_csv,
         query_knowledge_base]

llm = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)

# 🔗 Agent
agent = create_tool_calling_agent(llm=llm, tools=tools, prompt=prompt_template)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, max_iterations=2000)

initial_user_question_template = """
You are an expert in SQL Server performance tuning.

Here is one row from the {} output that needs to be interpreted and analyzed:\n
{}\n

The output is from the SQL Server database `{}`. You have access to:\n
- System catalog views (read-only)\n
- Brent Ozar’s diagnostic procedures: `sp_BlitzFirst`, `sp_BlitzCache`, `sp_BlitzIndex`, `sp_Blitz`, `sp_Who`\n
- A tool for running SQL SELECT queries to inspect metadata and performance-related views\n
- A tool for querying a curated knowledge base related to SQL Server performance\n

Your task:\n
1. Analyze the finding in detail using all available metadata and knowledge.\n
2. Propose an actionable recommendation to improve SQL Server performance.\n
3. Use `query_knowledge_base()` and `run_sqlserver_query_as_csv()` tools to gather evidence before final recommendation.\n

Your output must:\n
- Be in Markdown format\n
- Include a **clear explanation** of the root cause and impact\n
- Include **specific runnable SQL commands** (in `sql` code blocks), formatted with each clause on its own line\n
- Provide enough context and justification for a DBA to confidently apply the recommendation
"""

# TODO testing
if __name__ == "__main__":

    one_blitzindex_row = """
    redundand index found: [schema].[table].[index] (indexid=1)
                         """
    url = "https://www.brentozar.com/go/duplicateindex"

    with shelve.open("url_store") as key_value:
        user_question = initial_user_question_template.format(
            "sp_BlitzIndex",
            one_blitzindex_row,
            url,
            key_value[url],
            os.getenv("MSSQL_DB")
        )
        print(user_question)

        result = agent_executor.invoke({
            "input": user_question,
            "chat_history": []
        })
        print(result["output"])
