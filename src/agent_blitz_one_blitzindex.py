from typing import List
import os
import shelve
import io
import csv
import pyodbc
import sys
from dotenv import load_dotenv

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.documents import Document
from langchain_core.tools import tool
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma

# Import the centralized connection function
from .db_connection import get_connection


# Init
load_dotenv()
# Always resolve vector store relative to project root, not src/
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
rag_directory = os.path.join(project_root, "db", "chroma_db_firecrawl")
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")

if not os.path.exists(rag_directory):
    print("Vector store not found. Initializing vector store using webrawler.py first.")
    sys.exit(1)

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
    try:
        # Use the centralized connection function
        with get_connection() as conn:
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

            sqlserver_result = output.getvalue()

             # limit maximum lebgth of CSV to prevent excessive output
            max_csv_len = 100000
            if len(sqlserver_result) > max_csv_len:
                return sqlserver_result[:max_csv_len] + "\n...[TRUNCATED]"

            return sqlserver_result
    except pyodbc.Error as e:
        return f"Database connection error: {e}"


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

    vector_result = "\n\n---\n\n".join(doc.page_content for doc in relevant_docs)
    return vector_result



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

INITIAL_USER_QUESTION_TEMPLATE = """
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

    ONE_BLITZINDEX_ROW = """
    redundand index found: [schema].[table].[index] (indexid=1)
                         """
    TEST_URL = "https://www.brentozar.com/go/duplicateindex"

    with shelve.open("url_store") as key_value:
        user_question = INITIAL_USER_QUESTION_TEMPLATE.format(
            "sp_BlitzIndex",
            ONE_BLITZINDEX_ROW,
            TEST_URL,
            key_value[TEST_URL],
            os.getenv("MSSQL_DB")
        )
        print(user_question)

        result = agent_executor.invoke({
            "input": user_question,
            "chat_history": []
        })
        print(result["output"])
