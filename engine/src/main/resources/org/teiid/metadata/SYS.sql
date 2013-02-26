CREATE FOREIGN TABLE Columns (
	VDBName string(255) NOT NULL,
	SchemaName string(255),
	TableName string(255) NOT NULL,
	Name string(255) NOT NULL,
	Position integer NOT NULL,
	NameInSource string(255),
	DataType string(100) NOT NULL,
	Scale integer NOT NULL,
	Length integer NOT NULL,
	IsLengthFixed boolean NOT NULL,
	SupportsSelect boolean NOT NULL,
	SupportsUpdates boolean NOT NULL,
	IsCaseSensitive boolean NOT NULL,
	IsSigned boolean NOT NULL,
	IsCurrency boolean NOT NULL,
	IsAutoIncremented boolean NOT NULL,
	NullType string(20) NOT NULL,
	MinRange string(50),
	MaxRange string(50),
	DistinctCount integer,
	NullCount integer,
	SearchType string(20) NOT NULL,
	Format string(255),
	DefaultValue string(255),
	JavaClass string(500) NOT NULL,
	"Precision" integer NOT NULL,
	CharOctetLength integer,
	Radix integer NOT NULL,
	UID string(50) NOT NULL,
	Description string(255),
	OID integer,
	PRIMARY KEY (VDBName, SchemaName, TableName, Name),
	FOREIGN KEY (VDBName, SchemaName, TableName) REFERENCES Tables (VDBName, SchemaName, Name),
	FOREIGN KEY (DataType) REFERENCES DataTypes(Name),
	UNIQUE (UID)
);

CREATE FOREIGN TABLE DataTypes (
	Name string(100) NOT NULL,
	IsStandard boolean,
	IsPhysical boolean,
	TypeName string(100) NOT NULL,
	JavaClass string(500) NOT NULL,
	Scale integer,
	TypeLength integer NOT NULL,
	NullType string(20) NOT NULL,
	IsSigned boolean NOT NULL,
	IsAutoIncremented boolean NOT NULL,
	IsCaseSensitive boolean NOT NULL,
	"Precision" integer NOT NULL,
	Radix integer,
	SearchType string(20) NOT NULL,
	UID string(50) NOT NULL,
	RuntimeType string(64),
	BaseType string(64),
	Description string(255),
	OID integer,
	PRIMARY KEY (Name),
	UNIQUE (UID)	
);

CREATE FOREIGN TABLE KeyColumns (
	VDBName string(255) NOT NULL,
	SchemaName string(255),
	TableName string(2048) NOT NULL,
	Name string(255) NOT NULL,
	KeyName string(255),
	KeyType string(20) NOT NULL,
	RefKeyUID string(50),
	UID string(50) NOT NULL,
	Position integer,
	OID integer,
	PRIMARY KEY (VDBName, SchemaName, TableName, Name),
	FOREIGN KEY (VDBName, SchemaName, TableName) REFERENCES Tables (VDBName, SchemaName, Name),
	UNIQUE (UID)	
);

CREATE FOREIGN TABLE Keys (
	VDBName string(255) NOT NULL,
	SchemaName string(255),
	TableName string(2048) NOT NULL,
	Name string(255) NOT NULL,
	Description string(255),
	NameInSource string(255),
	Type string(20) NOT NULL,
	IsIndexed boolean NOT NULL,
	RefKeyUID string(50),
	UID string(50) NOT NULL,
	OID integer,
	PRIMARY KEY (VDBName, SchemaName, TableName, Name),
	FOREIGN KEY (VDBName, SchemaName, TableName) REFERENCES Tables (VDBName, SchemaName, Name),
	UNIQUE (UID)
);

CREATE FOREIGN TABLE ProcedureParams (
	VDBName string(255) NOT NULL,
	SchemaName string(255),
	ProcedureName string(255) NOT NULL,
	Name string(255) NOT NULL,
	DataType string(25) NOT NULL,
	Position integer NOT NULL,
	Type string(100) NOT NULL,
	Optional boolean NOT NULL,
	"Precision" integer NOT NULL,
	TypeLength integer NOT NULL,
	Scale integer NOT NULL,
	Radix integer NOT NULL,
	NullType string(10) NOT NULL,
	UID string(50),
	Description string(255),
	OID integer,
	PRIMARY KEY (VDBName, SchemaName, ProcedureName, Name),
	FOREIGN KEY (VDBName, SchemaName, ProcedureName) REFERENCES Procedures (VDBName, SchemaName, Name),
	FOREIGN KEY (DataType) REFERENCES DataTypes(Name),
	UNIQUE (UID)	
);

CREATE FOREIGN TABLE Procedures (
	VDBName string(255) NOT NULL,
	SchemaName string(255),
	Name string(255) NOT NULL,
	NameInSource string(255),
	ReturnsResults boolean NOT NULL,
	UID string(50) NOT NULL,
	Description string(255),
	OID integer,
	PRIMARY KEY (VDBName, SchemaName, Name),
	FOREIGN KEY (VDBName, SchemaName) REFERENCES Schemas (VDBName, Name),
	UNIQUE (UID)	
);

CREATE FOREIGN TABLE Properties (
	Name string(255) NOT NULL,
	"Value" string(255) NOT NULL,
	UID string(50) NOT NULL,
	OID integer,
	ClobValue clob(2097152),
	UNIQUE(UID, Name)
);

CREATE FOREIGN TABLE ReferenceKeyColumns (
	PKTABLE_CAT string(255),
	PKTABLE_SCHEM string(255),
	PKTABLE_NAME string(255),
	PKCOLUMN_NAME string(255),
	FKTABLE_CAT string(255),
	FKTABLE_SCHEM string(255),
	FKTABLE_NAME string(255),
	FKCOLUMN_NAME string(255),
	KEY_SEQ short,
	UPDATE_RULE integer,
	DELETE_RULE integer,
	FK_NAME string(255),
	PK_NAME string(255),
	DEFERRABILITY integer
);

CREATE FOREIGN TABLE Schemas (
	VDBName string(255),
	Name string(255),
	IsPhysical boolean NOT NULL,
	UID string(50) NOT NULL,
	Description string(255),
	PrimaryMetamodelURI string(255) NOT NULL,
	OID integer,
	PRIMARY KEY (VDBName, Name),
	UNIQUE (UID)	
);

CREATE FOREIGN TABLE Tables (
	VDBName string(255),
	SchemaName string(255),
	Name string(255) NOT NULL,
	Type string(20) NOT NULL,
	NameInSource string(255),
	IsPhysical boolean NOT NULL,
	SupportsUpdates boolean NOT NULL,
	UID string(50) NOT NULL,
	Cardinality integer NOT NULL,
	Description string(255),
	IsSystem boolean,
	IsMaterialized boolean NOT NULL,
	OID integer,
	PRIMARY KEY (VDBName, SchemaName, Name),
	FOREIGN KEY (VDBName, SchemaName) REFERENCES Schemas (VDBName, Name),
	UNIQUE (UID)	
);

CREATE FOREIGN TABLE VirtualDatabases (
	Name string(255) NOT NULL,
	Version string(50) NOT NULL,
	PRIMARY KEY (Name, Version)
);

CREATE FOREIGN PROCEDURE getXMLSchemas(IN document string NOT NULL) RETURNS TABLE (schema xml)
OPTIONS (UPDATECOUNT 0)
