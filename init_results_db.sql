CREATE TABLE Procedure_type (
  p_id INTEGER PRIMARY KEY,
  display_name VARCHAR(50),
  procedure_name VARCHAR(30)
);

CREATE TABLE Procedure_call (
  pc_id INTEGER PRIMARY KEY,
  run TIMESTAMP NOT NULL,
  p_id INTEGER NOT NULL REFERENCES Procedure_type (p_id)
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

INSERT INTO Procedure_type VALUES (1, 'Blitz', 'sp_blitz'),
                                 (2, 'Blitz Index', 'sp_BlitzIndex'),
                                 (3, 'Blitz Cache', 'sp_BlitzCache');