from pydantic import BaseModel
from typing import Optional


class BlitzRecord(BaseModel):
    pb_id: Optional[int] = None
    finding: Optional[str] = None
    details: Optional[str] = None
    priority: Optional[int] = None
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
    warnings: Optional[str] = None
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


# Mapping of procedure names to their corresponding models and display keys
PROCEDURE_MODELS = {
    "sp_Blitz": BlitzRecord,
    "sp_BlitzIndex": BlitzIndexRecord,
    "sp_BlitzCache": BlitzCacheRecord,
}

PROCEDURE_DISPLAY_KEYS = {
    "sp_Blitz": ["finding", "details", "priority"],
    "sp_BlitzIndex": ["finding", "details_schema_table_index_indexid", "priority"],
    "sp_BlitzCache": ["query_text", "avg_cpu_ms", "total_cpu_ms", "warnings"],
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

# Mapping from raw column names to model field names
COLUMN_MAPPING = {
    "sp_Blitz": {
        "Finding": "finding",
        "Details": "details",
        "Priority": "priority",
    },
    "sp_BlitzIndex": {
        "Finding": "finding",
        "Details: schema.table.index(indexid)": "details_schema_table_index_indexid",
        "Priority": "priority",
    },
    "sp_BlitzCache": {
        "Query Text": "query_text",
        "Avg CPU (ms)": "avg_cpu_ms",
        "Total CPU (ms)": "total_cpu_ms",
        "Warnings": "warnings",
    },
}
