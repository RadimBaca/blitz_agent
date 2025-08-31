CREATE TABLE Procedure_type (
  p_id INTEGER PRIMARY KEY,
  display_name VARCHAR(50),
  procedure_name VARCHAR(30)
);

CREATE TABLE Database_connection (
  db_id INTEGER PRIMARY KEY,
  db_name VARCHAR(100) NOT NULL,
  db_user VARCHAR(100) NOT NULL,
  db_password VARCHAR(100) NOT NULL,
  db_host VARCHAR(100) NOT NULL,
  db_port INTEGER NOT NULL,
  -- optional server information
  version VARCHAR(512) NULL,
  instance_memory_mb INTEGER NULL
);

CREATE TABLE Procedure_call (
  pc_id INTEGER PRIMARY KEY,
  run TIMESTAMP NOT NULL,
  p_id INTEGER NOT NULL REFERENCES Procedure_type (p_id),
  db_id INTEGER NOT NULL REFERENCES Database_connection (db_id)
);

CREATE TABLE Procedure_blitz (
  pb_id INTEGER PRIMARY KEY,
  procedure_order INTEGER NOT NULL,
  pc_id INTEGER NOT NULL REFERENCES Procedure_call (pc_id),
  finding TEXT,
  details TEXT,
  priority INTEGER,
  raw_record TEXT
);

CREATE TABLE Procedure_blitzindex (
  pbi_id INTEGER PRIMARY KEY,
  procedure_order INTEGER NOT NULL,
  pc_id INTEGER NOT NULL REFERENCES Procedure_call (pc_id),
  finding TEXT,
  details_schema_table_index_indexid TEXT,
  priority INTEGER,
  more_info TEXT,
  raw_record TEXT,
  database_name TEXT,
  schema_name TEXT,
  table_name TEXT,
  index_findings_loaded BOOLEAN DEFAULT FALSE
);

CREATE TABLE Procedure_blitzcache (
  pbc_id INTEGER PRIMARY KEY,
  procedure_order INTEGER NOT NULL,
  pc_id INTEGER NOT NULL REFERENCES Procedure_call (pc_id),
  query_text TEXT,
  avg_cpu_ms REAL,
  total_cpu_ms REAL,
  executions INTEGER,
  total_reads INTEGER,
  last_execution TIMESTAMP,
  warnings TEXT,
  raw_record TEXT
);

-- Chat tables for each procedure type
CREATE TABLE Chat_blitz (
  cb_id INTEGER PRIMARY KEY,
  response TEXT NOT NULL,
  type VARCHAR,
  chat_order INTEGER,
  pb_id INTEGER NOT NULL REFERENCES Procedure_blitz (pb_id)
);

CREATE TABLE Chat_blitzindex (
  cbi_id INTEGER PRIMARY KEY,
  response TEXT NOT NULL,
  type VARCHAR,
  chat_order INTEGER,
  pbi_id INTEGER NOT NULL REFERENCES Procedure_blitzindex (pbi_id)
);

CREATE TABLE Chat_blitzcache (
  cbc_id INTEGER PRIMARY KEY,
  response TEXT NOT NULL,
  type VARCHAR,
  chat_order INTEGER,
  pbc_id INTEGER NOT NULL REFERENCES Procedure_blitzcache (pbc_id)
);

-- New table for storing index information from Q1 queries
CREATE TABLE DB_Indexes (
  di_id INTEGER PRIMARY KEY,
  pbi_id INTEGER NOT NULL REFERENCES Procedure_blitzindex (pbi_id),
  db_schema_object_indexid TEXT,
  index_definition TEXT,
  secret_columns TEXT,
  fill_factor INTEGER,
  index_usage_summary TEXT,
  index_op_stats TEXT,
  index_size_summary TEXT,
  partition_compression_detail TEXT,
  index_lock_wait_summary TEXT,
  is_referenced_by_foreign_key INTEGER,
  fks_covered_by_index INTEGER,
  last_user_seek TEXT,
  last_user_scan TEXT,
  last_user_lookup TEXT,
  last_user_update TEXT,
  create_date TEXT,
  modify_date TEXT,
  page_latch_wait_count INTEGER,
  page_latch_wait_time TEXT,
  page_io_latch_wait_count INTEGER,
  page_io_latch_wait_time TEXT,
  create_tsql TEXT,
  drop_tsql TEXT
);

-- New table for storing findings from Q2 queries (Missing index findings)
CREATE TABLE DB_Findings (
  df_id INTEGER PRIMARY KEY,
  pbi_id INTEGER NOT NULL REFERENCES Procedure_blitzindex (pbi_id),
  finding TEXT,
  url TEXT,
  estimated_benefit TEXT,
  missing_index_request TEXT,
  estimated_impact TEXT,
  create_tsql TEXT,
  sample_query_plan TEXT
);

-- Table for storing recommendations based on analysis
CREATE TABLE Recommendation (
  id_recom INTEGER PRIMARY KEY,
  description TEXT NOT NULL,
  sql_command TEXT,
  pb_id INTEGER REFERENCES Procedure_blitz (pb_id),
  pbi_id INTEGER REFERENCES Procedure_blitzindex (pbi_id),
  pbc_id INTEGER REFERENCES Procedure_blitzcache (pbc_id),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT check_single_reference CHECK (
    (pb_id IS NOT NULL AND pbi_id IS NULL AND pbc_id IS NULL) OR
    (pb_id IS NULL AND pbi_id IS NOT NULL AND pbc_id IS NULL) OR
    (pb_id IS NULL AND pbi_id IS NULL AND pbc_id IS NOT NULL)
  )
);


INSERT INTO Procedure_type VALUES (1, 'Blitz', 'sp_Blitz'),
                                 (2, 'Blitz Index', 'sp_BlitzIndex'),
                                 (3, 'Blitz Cache', 'sp_BlitzCache');
