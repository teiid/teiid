
-- these tables are used by custom junit test for testing transactions

create Table g1 (
	  e1 NUMERIC(5), 
	  e2 VARCHAR(50),
	  PRIMARY KEY (e1)
);
create Table g2 (
	e1 NUMERIC(5) REFERENCES g1 (e1), 
	e2 VARCHAR(50)
);
   