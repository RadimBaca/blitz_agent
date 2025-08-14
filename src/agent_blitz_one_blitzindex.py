from typing import List
import os
import io
import csv
import pyodbc
import sys

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.documents import Document
from langchain_core.tools import tool
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma

# Import the centralized connection function
from .db_connection import get_connection

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



# Tool: run_sqlserver_query_as_csv
@tool
def run_sqlserver_query_as_csv(query: str, params: tuple = (), max_rows: int = 50) -> str:
    """
    Executes a SQL Server query and returns up to `max_rows` results as CSV string.
    This method allows you query_store or other views to get information about metadata and statistics.
    Avoid calling the same query multiple times
    """
    try:
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

tools = [run_sqlserver_query_as_csv, query_knowledge_base]

llm = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)

# Agent
agent = create_tool_calling_agent(llm=llm, tools=tools, prompt=prompt_template)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, max_iterations=2000)




def _load_prompt_for(procedure: str, finding: str, database: str) -> str:
    """
    Load a prompt template for a given procedure and populate placeholders.

    Behavior is driven by the `VERSION` in the `.env` file at project root.
    VERSION=1 -> use `db/prompts/general_sp_blitz.txt`
    VERSION=2 -> prefer `db/prompts/{procedure}.txt`, fallback to general
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    version = int(os.getenv("VERSION", '1'))

    prompts_dir = os.path.join(project_root, "db", "prompts")
    specific_prompt_file = os.path.join(prompts_dir, f"{procedure}.txt")
    general_prompt_file = os.path.join(project_root, "prompts", "general_sp_blitz.txt")

    template = None
    if version == 2:
        # Try procedure-specific prompt first
        try:
            if os.path.exists(specific_prompt_file):
                with open(specific_prompt_file, "r") as f:
                    template = f.read()
            elif os.path.exists(general_prompt_file):
                with open(general_prompt_file, "r") as f:
                    template = f.read()
        except Exception:
            return None
    if version == 1:
        # Version 1: use the generic prompt
        try:
            if os.path.exists(general_prompt_file):
                with open(general_prompt_file, "r") as f:
                    template = f.read()
        except Exception:
            return None

    if not template:
        return None

    return template.format(procedure=procedure, finding=finding, database=database)


if __name__ == "__main__":
    # Quick smoke test
    ONE_BLITZINDEX_ROW = "redundand index found: [schema].[table].[index] (indexid=1)"
    user_question = _load_prompt_for(
        "sp_BlitzIndex",
        ONE_BLITZINDEX_ROW,
        os.getenv("MSSQL_DB") or "<unknown_db>"
    )
    print(user_question)
