from typing import List, Dict, Any
import os
import io
import csv
import pyodbc
import sys
import time
import logging
import json
from httpx import RemoteProtocolError

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.documents import Document
from langchain_core.tools import tool
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma

# Avoid importing `procedure` from app to prevent circular import when app imports this module.
# The code below will use the local `procedure_name` parameter instead where needed.

# Import the centralized connection function
from .db_connection import get_connection
from .models import DBIndexRecord
from . import result_DAO as dao

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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


# Global context for current analysis session
_current_context = {"procedure": None, "record_id": None}


def set_analysis_context(procedure: str, record_id: int):
    """Set the current analysis context for tools"""
    _current_context["procedure"] = procedure
    _current_context["record_id"] = record_id


@tool
def add_recommendation(description: str, sql_command: str = None) -> str:
    """
    Add a recommendation based on the current analysis. Use this when you have identified
    a specific actionable recommendation that should be stored and referenced later.

    Args:
        description: Clear description of the recommendation
        sql_command: Optional SQL command to implement the recommendation

    Returns:
        Link to view the recommendation or error message
    """
    try:
        # Get current context
        procedure = _current_context.get("procedure")
        record_id = _current_context.get("record_id")

        if not procedure or record_id is None:
            return "Error: No analysis context available. Cannot create recommendation."

        # Map procedure to foreign key field
        fk_mapping = {
            "sp_Blitz": "pb_id",
            "sp_BlitzIndex": "pbi_id",
            "sp_BlitzCache": "pbc_id"
        }

        if procedure not in fk_mapping:
            return f"Error: Unsupported procedure '{procedure}' for recommendations."

        fk_field = fk_mapping[procedure]
        kwargs = {fk_field: record_id}

        # Insert recommendation
        recommendation_id = dao.insert_recommendation(
            description=description,
            sql_command=sql_command,
            **kwargs
        )

        # Return link to recommendation
        app_url = os.getenv("APP_URL", "http://localhost:5001")
        recommendation_link = f"{app_url}/recommendation/{recommendation_id}"

        return f"Recommendation created successfully! View it here: {recommendation_link}"

    except Exception as e:
        return f"Error creating recommendation: {str(e)}"


prompt_template = ChatPromptTemplate.from_messages([
    ("system", "Odpovez na otazku jak nejlepe umis."),
    MessagesPlaceholder(variable_name="chat_history"),
    ("user", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad")
])

tools = [run_sqlserver_query_as_csv, query_knowledge_base, add_recommendation]

# Configure LLM with more robust settings for network stability
llm = ChatOpenAI(
    model_name="gpt-4o-mini",
    temperature=0,
    request_timeout=60,  # 60 second timeout
    max_retries=3,       # Built-in retry logic
    streaming=False      # Disable streaming to avoid connection issues
)

# Agent
agent = create_tool_calling_agent(llm=llm, tools=tools, prompt=prompt_template)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, max_iterations=2000)


def execute(procedure: str, record_id: int, user_input: str, chat_history: list) -> dict:
    """
    Execute agent analysis with proper context for recommendations.
    Includes retry logic for network issues.

    Args:
        procedure: The procedure name (e.g., 'sp_Blitz', 'sp_BlitzIndex', 'sp_BlitzCache')
        record_id: The record ID for the current analysis
        user_input: The user's input/question
        chat_history: The chat history for context

    Returns:
        The agent execution result
    """
    # Set context for tools
    set_analysis_context(procedure, record_id)

    max_retries = 3
    retry_delay = 2  # seconds

    for attempt in range(max_retries):
        try:
            logger.info("Attempting to execute agent (attempt %d/%d)", attempt + 1, max_retries)

            # Execute agent
            result = agent_executor.invoke({
                "input": user_input,
                "chat_history": chat_history
            })

            logger.info("Agent execution successful")
            return result

        except (RemoteProtocolError, ConnectionError, TimeoutError) as e:
            logger.warning("Network error on attempt %d/%d: %s", attempt + 1, max_retries, str(e))

            if attempt < max_retries - 1:
                logger.info("Retrying in %d seconds...", retry_delay)
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("All retry attempts failed")
                return {
                    "output": f"Network error occurred after {max_retries} attempts. Please try again later. Error: {str(e)}"
                }

        except Exception as e:  # pylint: disable=broad-except
            logger.error("Unexpected error during agent execution: %s", str(e))
            return {
                "output": f"An unexpected error occurred: {str(e)}"
            }

    # This should never be reached, but just in case
    return {
        "output": "Failed to execute agent after all retry attempts."
    }



def execute_more_info_query(more_info_sql: str) -> List[Dict[str, Any]]:
    """
    Execute the more_info SQL command from BlitzIndex to get detailed index information.
    Returns a list of dictionaries representing the query results.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(more_info_sql)

            # Get column names
            columns = [column[0] for column in cursor.description] if cursor.description else []

            # Fetch all rows
            rows = cursor.fetchall()

            # Convert to list of dictionaries
            result = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    if i < len(columns):
                        row_dict[columns[i]] = value
                result.append(row_dict)

            return result
    except pyodbc.Error as e:
        print(f"Error executing more_info query: {e}")
        return []


def load_specialized_prompt(procedure_name, record, database: str) -> str:
    """
    Load specialized prompt templates based on the finding type and populate with index data.
    Supports over-indexing, redundant indexes, and heap analysis findings.
    """

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    version = int(os.getenv("VERSION", '1'))
    finding = json.loads(record.raw_record)

    general_prompt_file = os.path.join(project_root, "prompts", "general_sp_blitz.txt")

    if version == 1:
        try:
            with open(general_prompt_file, "r", encoding="utf-8") as f:
                template = f.read()
        except (IOError, OSError) as e:
            print(f"Error loading prompt file: {e}")
            return f"Error loading {general_prompt_file} file"

        return template.format(finding=finding)

    elif version == 2:
        if procedure_name == "sp_BlitzIndex" and record.more_info is not None:
            try:
                # dao.process_over_indexing_analysis(record)
                db_indexes = dao.get_db_indexes(record.pbi_id)

                # Determine which type of analysis this is
                record_finding = record.finding
                is_over_indexing = record_finding.startswith("Over-Indexing")
                is_redundant_indexes = record_finding.startswith("Redundant Indexes")
                is_heap_analysis = record_finding.startswith("Indexes Worth Reviewing")

                # Use specialized prompts for version 2
                if is_over_indexing:
                    prompt_file = os.path.join(project_root, "db", "prompts", "over_indexing.txt")
                    format_func = _format_index_data_for_prompt
                elif is_redundant_indexes:
                    prompt_file = os.path.join(project_root, "db", "prompts", "redundant_indexes.txt")
                    format_func = _format_index_data_for_prompt
                elif is_heap_analysis:
                    prompt_file = os.path.join(project_root, "db", "prompts", "heap_analysis.txt")
                    format_func = _format_heap_data_for_prompt
                else:
                    # Fallback to over-indexing prompt for unknown types
                    prompt_file = os.path.join(project_root, "db", "prompts", "sp_BlitzIndex_analysis.txt")
                    format_func = _format_index_data_for_prompt

                try:
                    with open(prompt_file, "r", encoding="utf-8") as f:
                        template = f.read()
                except (IOError, OSError) as e:
                    print(f"Error loading {prompt_file} file: {e}")
                    return f"Error loading {prompt_file} file"

                # Format the index data for the prompt
                index_analysis_data = format_func(record, db_indexes)
                return template.format(
                    finding=finding,
                    index_analysis_data=index_analysis_data
                )
            except (pyodbc.Error, ValueError, KeyError) as e:
                print(f"Error processing indexing analysis: {e}")

        if procedure_name == "sp_BlitzIndex" or  procedure_name == "sp_BlitzCache" or procedure_name == "sp_Blitz":
            # Use the local parameter `procedure_name` to select the specific prompt file
            specific_prompt_file = os.path.join(project_root, "db", "prompts", f"{procedure_name}.txt")

            try:
                if os.path.exists(specific_prompt_file):
                    with open(specific_prompt_file, "r") as f:
                        template = f.read()
                elif os.path.exists(general_prompt_file):
                    with open(general_prompt_file, "r") as f:
                        template = f.read()
            except Exception:
                return f"Error loading prompt file"


            return template.format(finding=finding)

    return "Version of the application is incorrectly set. Possible values are 1 or 2"


def _format_index_data_for_prompt(record, db_indexes: List[DBIndexRecord]) -> str:
    """
    Format the index data into a readable structure for the AI prompt.
    Focus on relevant attributes for over-indexing analysis:
    - List of all indexes with their definitions (columns, includes, filters)
    - Usage statistics (seeks, scans, lookups, updates)
    - Size information
    - Foreign key relationships
    - Last access times
    """
    if not db_indexes:
        return f"No detailed index data available for {record.finding}"

    # Format the data directly from db_indexes
    formatted_data = []
    formatted_data.append(f"OVER-INDEXING ANALYSIS for: {record.finding}")
    formatted_data.append(f"Details: {record.details_schema_table_index_indexid}")
    formatted_data.append("")
    formatted_data.append("INDEX ANALYSIS DATA:")
    formatted_data.append("=" * 50)

    for i, index in enumerate(db_indexes, 1):
        formatted_data.append(f"\nINDEX {i}: {index.db_schema_object_indexid}")

        # Index definitions (columns, includes, filters)
        formatted_data.append(f"Definition: {index.index_definition or 'N/A'}")
        formatted_data.append(f"Secret Columns: {index.secret_columns or 'N/A'}")

        # Usage statistics (seeks, scans, lookups, updates)
        formatted_data.append(f"Usage Summary: {index.index_usage_summary or 'N/A'}")
        formatted_data.append(f"Operation Stats: {index.index_op_stats or 'N/A'}")

        # Size information
        formatted_data.append(f"Size Summary: {index.index_size_summary or 'N/A'}")

        # Foreign key relationships
        formatted_data.append(f"Referenced by FK: {'Yes' if index.is_referenced_by_foreign_key else 'No'}")
        formatted_data.append(f"FK Coverage: {index.fks_covered_by_index or 'N/A'}")

        # Last access times
        formatted_data.append(f"Last User Seek: {index.last_user_seek or 'Never'}")
        formatted_data.append(f"Last User Scan: {index.last_user_scan or 'Never'}")
        formatted_data.append(f"Last User Lookup: {index.last_user_lookup or 'Never'}")
        formatted_data.append(f"Last User Update: {index.last_user_update or 'Never'}")

        # Creation/modification dates
        formatted_data.append(f"Created: {index.create_date or 'N/A'}")
        formatted_data.append(f"Modified: {index.modify_date or 'N/A'}")

        formatted_data.append("-" * 30)

    return "\n".join(formatted_data)


def _format_heap_data_for_prompt(record, db_indexes: List[DBIndexRecord]) -> str:
    """
    Format the index data into a readable structure for heap analysis AI prompt.
    Focus on relevant attributes for heap table analysis:
    - Current heap table structure
    - Nonclustered primary key details
    - All existing nonclustered indexes with their definitions
    - Usage statistics to identify clustering candidates
    - Access patterns and performance characteristics
    """
    if not db_indexes:
        return f"No detailed index data available for {record.finding}"

    # Format the data specifically for heap analysis
    formatted_data = []
    formatted_data.append(f"HEAP TABLE ANALYSIS for: {record.finding}")
    formatted_data.append(f"Details: {record.details_schema_table_index_indexid}")
    formatted_data.append("")
    formatted_data.append("CURRENT HEAP TABLE STRUCTURE:")
    formatted_data.append("=" * 50)

    # Identify the primary key index and other indexes
    primary_key_index = None
    other_indexes = []

    for index in db_indexes:
        # Look for primary key indicators in the definition
        if ("[PK]" in str(index.index_definition) or
            "PRIMARY KEY" in str(index.index_definition).upper() or
            "PK_" in str(index.db_schema_object_indexid)):
            primary_key_index = index
        else:
            other_indexes.append(index)

    # Display primary key information first
    if primary_key_index:
        formatted_data.append(f"\nPRIMARY KEY (Nonclustered): {primary_key_index.db_schema_object_indexid}")
        formatted_data.append(f"Definition: {primary_key_index.index_definition or 'N/A'}")
        formatted_data.append(f"Usage Summary: {primary_key_index.index_usage_summary or 'N/A'}")
        formatted_data.append(f"Size Summary: {primary_key_index.index_size_summary or 'N/A'}")
        formatted_data.append(f"Operation Stats: {primary_key_index.index_op_stats or 'N/A'}")
        formatted_data.append("")

    # Display other nonclustered indexes
    formatted_data.append("OTHER NONCLUSTERED INDEXES:")
    formatted_data.append("-" * 40)

    if not other_indexes:
        formatted_data.append("No other nonclustered indexes found on this table.")
    else:
        for i, index in enumerate(other_indexes, 1):
            formatted_data.append(f"\nINDEX {i}: {index.db_schema_object_indexid}")
            formatted_data.append(f"Definition: {index.index_definition or 'N/A'}")
            formatted_data.append(f"Secret Columns: {index.secret_columns or 'N/A'}")
            formatted_data.append(f"Usage Summary: {index.index_usage_summary or 'N/A'}")
            formatted_data.append(f"Operation Stats: {index.index_op_stats or 'N/A'}")
            formatted_data.append(f"Size Summary: {index.index_size_summary or 'N/A'}")

            # Access patterns - important for clustering decisions
            formatted_data.append(f"Last User Seek: {index.last_user_seek or 'Never'}")
            formatted_data.append(f"Last User Scan: {index.last_user_scan or 'Never'}")
            formatted_data.append(f"Last User Lookup: {index.last_user_lookup or 'Never'}")
            formatted_data.append(f"Last User Update: {index.last_user_update or 'Never'}")

            # Foreign key information
            formatted_data.append(f"Referenced by FK: {'Yes' if index.is_referenced_by_foreign_key else 'No'}")
            formatted_data.append(f"FK Coverage: {index.fks_covered_by_index or 'N/A'}")

            formatted_data.append("-" * 30)

    return "\n".join(formatted_data)




if __name__ == "__main__":
    # Quick smoke test
    ONE_BLITZINDEX_ROW = "redundand index found: [schema].[table].[index] (indexid=1)"
    user_question = load_specialized_prompt(
        "sp_BlitzIndex",
        ONE_BLITZINDEX_ROW,
        os.getenv("MSSQL_DB") or "<unknown_db>"
    )
    print(user_question)
