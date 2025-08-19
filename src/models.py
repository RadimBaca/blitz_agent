from pydantic import BaseModel
from typing import Optional


class BlitzRecord(BaseModel):
    pb_id: Optional[int] = None
    finding: Optional[str] = None
    details: Optional[str] = None
    priority: Optional[int] = None
    raw_record: Optional[str] = None
    procedure_order: int
    pc_id: int
    _analyzed: bool = False

    @property
    def _rec_id(self) -> int:
        """Return the procedure_order as the record ID for compatibility"""
        return self.procedure_order

    class Config:
        from_attributes = True


class BlitzIndexRecord(BaseModel):
    pbi_id: Optional[int] = None
    finding: Optional[str] = None
    details_schema_table_index_indexid: Optional[str] = None
    priority: Optional[int] = None
    more_info: Optional[str] = None
    raw_record: Optional[str] = None
    procedure_order: int
    pc_id: int
    _analyzed: bool = False

    @property
    def _rec_id(self) -> int:
        """Return the procedure_order as the record ID for compatibility"""
        return self.procedure_order

    class Config:
        from_attributes = True


class BlitzCacheRecord(BaseModel):
    pbc_id: Optional[int] = None
    query_text: Optional[str] = None
    avg_cpu_ms: Optional[float] = None
    total_cpu_ms: Optional[float] = None
    executions: Optional[int] = None
    total_reads: Optional[int] = None
    last_execution: Optional[str] = None  # Using string for timestamp compatibility
    warnings: Optional[str] = None
    raw_record: Optional[str] = None
    procedure_order: int
    pc_id: int
    _analyzed: bool = False

    @property
    def _rec_id(self) -> int:
        """Return the procedure_order as the record ID for compatibility"""
        return self.procedure_order

    class Config:
        from_attributes = True


class ChatRecord(BaseModel):
    ch_id: Optional[int] = None
    response: str
    type: str
    chat_order: int
    record_id: int  # This will be pb_id, pbi_id, or pbc_id depending on the table

    class Config:
        from_attributes = True


class DBIndexRecord(BaseModel):
    di_id: Optional[int] = None
    pbi_id: int
    db_schema_object_indexid: Optional[str] = None
    index_definition: Optional[str] = None
    secret_columns: Optional[str] = None
    fill_factor: Optional[int] = None
    index_usage_summary: Optional[str] = None
    index_op_stats: Optional[str] = None
    index_size_summary: Optional[str] = None
    partition_compression_detail: Optional[str] = None
    index_lock_wait_summary: Optional[str] = None
    is_referenced_by_foreign_key: Optional[int] = None
    fks_covered_by_index: Optional[int] = None
    last_user_seek: Optional[str] = None
    last_user_scan: Optional[str] = None
    last_user_lookup: Optional[str] = None
    last_user_update: Optional[str] = None
    create_date: Optional[str] = None
    modify_date: Optional[str] = None
    page_latch_wait_count: Optional[int] = None
    page_latch_wait_time: Optional[str] = None
    page_io_latch_wait_count: Optional[int] = None
    page_io_latch_wait_time: Optional[str] = None
    create_tsql: Optional[str] = None
    drop_tsql: Optional[str] = None

    class Config:
        from_attributes = True


class Recommendation(BaseModel):
    id_recom: Optional[int] = None
    description: str
    sql_command: Optional[str] = None
    pb_id: Optional[int] = None
    pbi_id: Optional[int] = None
    pbc_id: Optional[int] = None
    created_at: Optional[str] = None
    pb_procedure_order: Optional[int] = None
    pbi_procedure_order: Optional[int] = None
    pbc_procedure_order: Optional[int] = None

    class Config:
        from_attributes = True



# Mapping of procedure names to their corresponding models and display keys
PROCEDURE_MODELS = {
    "sp_Blitz": BlitzRecord,
    "sp_BlitzIndex": BlitzIndexRecord,
    "sp_BlitzCache": BlitzCacheRecord,
}

PROCEDURE_DISPLAY_KEYS = {
    "sp_Blitz": ["finding", "details", "priority", "raw_record"],
    "sp_BlitzIndex": ["finding", "details_schema_table_index_indexid", "priority", "raw_record"],
    "sp_BlitzCache": ["query_text", "avg_cpu_ms", "total_cpu_ms", "executions", "total_reads", "last_execution", "warnings", "raw_record"],
}

PROCEDURE_TABLE_NAMES = {
    "sp_Blitz": "Procedure_blitz",
    "sp_BlitzIndex": "Procedure_blitzindex",
    "sp_BlitzCache": "Procedure_blitzcache",
}

PROCEDURE_CHAT_TABLE_NAMES = {
    "sp_Blitz": "Chat_blitz",
    "sp_BlitzIndex": "Chat_blitzindex",
    "sp_BlitzCache": "Chat_blitzcache",
}

PROCEDURE_ID_FIELDS = {
    "sp_Blitz": "pb_id",
    "sp_BlitzIndex": "pbi_id",
    "sp_BlitzCache": "pbc_id",
}

# Map procedure names to recommendation foreign key fields
RECOMMENDATION_FK_MAPPING = {
    "sp_Blitz": "pb_id",
    "sp_BlitzIndex": "pbi_id",
    "sp_BlitzCache": "pbc_id"
}

# Mapping from raw column names to model field names
COLUMN_MAPPING = {
    "sp_Blitz": {
        "Finding": "finding",
        "Details": "details",
        "Priority": "priority",
        "raw_record": "raw_record",
    },
    "sp_BlitzIndex": {
        "Finding": "finding",
        "Details: schema.table.index(indexid)": "details_schema_table_index_indexid",
        "Priority": "priority",
        "More Info": "more_info",
        "raw_record": "raw_record",
    },
    "sp_BlitzCache": {
        "Query Text": "query_text",
        "Avg CPU (ms)": "avg_cpu_ms",
        "Total CPU (ms)": "total_cpu_ms",
        "# Executions": "executions",
        "Total Reads": "total_reads",
        "Last Execution": "last_execution",
        "Warnings": "warnings",
        "raw_record": "raw_record",
    },
}
