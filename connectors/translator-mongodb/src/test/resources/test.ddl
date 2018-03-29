CREATE FOREIGN TABLE G1 (
    e1 integer NOT NULL,
    e2 integer NOT NULL,
    e3 integer,
    PRIMARY KEY (e1)
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE G2 (
    e1 integer NOT NULL PRIMARY KEY,
    e2 integer NOT NULL,
    e3 integer,
    FOREIGN KEY (e1) REFERENCES G1 (e1)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'G1');

CREATE FOREIGN TABLE G3 (
    e1 integer NOT NULL PRIMARY KEY,
    e2 integer NOT NULL,
    e3 integer,
    FOREIGN KEY (e2) REFERENCES G1 (e1)
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:MERGE" 'G1');

CREATE FOREIGN TABLE G1E (
    e1 integer NOT NULL,
    e2 integer NOT NULL,
    e3 integer,
    e4 integer,
    PRIMARY KEY (e1),
    FOREIGN KEY (e4) REFERENCES G4 (e1)
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE G4 (
    e1 integer NOT NULL PRIMARY KEY,
    e2 integer NOT NULL,
    e3 integer  
) OPTIONS(UPDATABLE 'TRUE', "teiid_mongo:EMBEDDABLE" 'true');

CREATE FOREIGN TABLE TIME_TEST (
    e1 integer NOT NULL,
    e2 timestamp,
    PRIMARY KEY (e1)
) OPTIONS(UPDATABLE 'TRUE');
