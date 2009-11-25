
-- these tables are used by custom junit test for testing transactions
create Table g1 (e1 NUMERIC(5) PRIMARY KEY,   e2 varchar(50))ENGINE=INNODB;
create Table g2 (e1 NUMERIC(5), e2 varchar(50), FOREIGN KEY (e1) REFERENCES g1(e1))ENGINE=INNODB;


--create Table g1 (e1 NUMERIC(5) PRIMARY KEY,   e2 varchar(50));
--create Table g2 (e1 NUMERIC(5) REFERENCES g1 (e1), e2 varchar(50));



   