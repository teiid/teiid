CREATE FOREIGN TABLE A (
	a_id integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	a_value string,
	PRIMARY KEY(a_id)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'data.A', "teiid_odata:Type" 'ENTITY_COLLECTION', "teiid_rel:fqn" 'entity+container=data/entity+set=A');

CREATE FOREIGN TABLE C (
	c_id integer NOT NULL OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	a_id integer OPTIONS (NATIVE_TYPE 'Edm.Int32'),
	PRIMARY KEY(c_id),
	CONSTRAINT A_FK0 FOREIGN KEY(a_id) REFERENCES teiid.A (a_id)
) OPTIONS (UPDATABLE TRUE, "teiid_odata:NameInSchema" 'data.C', "teiid_odata:Type" 'ENTITY_COLLECTION', "teiid_rel:fqn" 'entity+container=data/entity+set=C');