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
  db_port INTEGER NOT NULL
);

CREATE TABLE Procedure_call (
  pc_id INTEGER PRIMARY KEY,
  run TIMESTAMP NOT NULL,
  p_id INTEGER NOT NULL REFERENCES Procedure_type (p_id),
  db_id INTEGER NOT NULL REFERENCES Database_connection (db_id)
);

CREATE TABLE Procedure_result (
  pr_id INTEGER PRIMARY KEY,
  result TEXT NOT NULL,
  procedure_order INTEGER NOT NULL,
  pc_id INTEGER NOT NULL REFERENCES Procedure_call (pc_id)
);

CREATE TABLE Chat (
  ch_id INTEGER PRIMARY KEY,
  response TEXT NOT NULL,
  type VARCHAR,
  chat_order INTEGER,
  pr_id INTEGER NOT NULL REFERENCES Procedure_result (pr_id)
);

-- New separate tables for each Blitz procedure
CREATE TABLE Procedure_blitz (
  pb_id INTEGER PRIMARY KEY,
  procedure_order INTEGER NOT NULL,
  pc_id INTEGER NOT NULL REFERENCES Procedure_call (pc_id),
  finding TEXT,
  details TEXT,
  priority INTEGER
);

CREATE TABLE Procedure_blitzindex (
  pbi_id INTEGER PRIMARY KEY,
  procedure_order INTEGER NOT NULL,
  pc_id INTEGER NOT NULL REFERENCES Procedure_call (pc_id),
  finding TEXT,
  details_schema_table_index_indexid TEXT,
  priority INTEGER
);

CREATE TABLE Procedure_blitzcache (
  pbc_id INTEGER PRIMARY KEY,
  procedure_order INTEGER NOT NULL,
  pc_id INTEGER NOT NULL REFERENCES Procedure_call (pc_id),
  query_text TEXT,
  avg_cpu_ms REAL,
  warnings TEXT
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

INSERT INTO Procedure_type VALUES (1, 'Blitz', 'sp_Blitz'),
                                 (2, 'Blitz Index', 'sp_BlitzIndex'),
                                 (3, 'Blitz Cache', 'sp_BlitzCache');