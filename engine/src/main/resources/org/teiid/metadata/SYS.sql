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
	TableUID string (50) NOT NULL,
	TypeName string(100),
	TypeCode integer,
	ColumnSize integer,
	PRIMARY KEY (VDBName, SchemaName, TableName, Name),
	FOREIGN KEY (VDBName, SchemaName, TableName) REFERENCES Sys.Tables (VDBName, SchemaName, Name),
	FOREIGN KEY (TableUID) REFERENCES Sys.Tables (UID),
	UNIQUE (UID)
);

CREATE FOREIGN TABLE DataTypes (
	Name string(100) NOT NULL,
	IsStandard boolean,
	Type string(64),
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
	TypeCode integer,
	Literal_Prefix string(64),
	Literal_Suffix string(64),
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
	TableUID string (50) NOT NULL,
	PRIMARY KEY (VDBName, SchemaName, TableName, Name),
	FOREIGN KEY (VDBName, SchemaName, TableName) REFERENCES Sys.Tables (VDBName, SchemaName, Name),
	FOREIGN KEY (TableUID) REFERENCES Sys.Tables (UID),
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
	TableUID string(50) NOT NULL,
	SchemaUID string(50) NOT NULL,
	RefTableUID string(50) NOT NULL,
	RefSchemaUID string(50) NOT NULL,
	ColPositions short[] NOT NULL,
	ColNames string[] NOT NULL,
	PRIMARY KEY (VDBName, SchemaName, TableName, Name),
	FOREIGN KEY (VDBName, SchemaName, TableName) REFERENCES Sys.Tables (VDBName, SchemaName, Name),
	UNIQUE (UID)
);

CREATE FOREIGN TABLE ProcedureParams (
	VDBName string(255) NOT NULL,
	SchemaName string(255),
	ProcedureName string(255) NOT NULL,
	ProcedureUID string(50) NOT NULL,
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
	TypeName string(100),
    TypeCode integer,
    ColumnSize integer,
    DefaultValue string(255),
	PRIMARY KEY (VDBName, SchemaName, ProcedureName, Name),
	FOREIGN KEY (VDBName, SchemaName, ProcedureName) REFERENCES Procedures (VDBName, SchemaName, Name),
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
	SchemaUID string (50) NOT NULL,
	PRIMARY KEY (VDBName, SchemaName, Name),
	FOREIGN KEY (VDBName, SchemaName) REFERENCES Schemas (VDBName, Name),
	FOREIGN KEY (SchemaUID) REFERENCES Schemas (UID),
	UNIQUE (UID)	
);

CREATE FOREIGN TABLE FunctionParams (
    VDBName string(255) NOT NULL,
    SchemaName string(255),
    FunctionName string(255) NOT NULL,
    FunctionUID string(50) NOT NULL,
    Name string(255) NOT NULL,
    DataType string(25) NOT NULL,
    Position integer NOT NULL,
    Type string(100) NOT NULL,
    "Precision" integer NOT NULL,
    TypeLength integer NOT NULL,
    Scale integer NOT NULL,
    Radix integer NOT NULL,
    NullType string(10) NOT NULL,
    UID string(50),
    Description string(4000),
    TypeName string(100),
    TypeCode integer,
    ColumnSize integer,
    UNIQUE (UID),
    PRIMARY KEY (VDBName, SchemaName, FunctionName, Name),
    FOREIGN KEY (VDBName, SchemaName, FunctionName, FunctionUID) REFERENCES Functions (VDBName, SchemaName, Name, UID)
);

CREATE FOREIGN TABLE Functions (
    VDBName string(255) NOT NULL,
    SchemaName string(255),
    Name string(255) NOT NULL,
    NameInSource string(255),
    UID string(50) NOT NULL,
    Description string(4000),
    IsVarArgs boolean,
    SchemaUID string (50) NOT NULL,
    PRIMARY KEY (VDBName, SchemaName, Name, UID),
    FOREIGN KEY (VDBName, SchemaName) REFERENCES Schemas (VDBName, Name),
    UNIQUE (UID)    
);

CREATE FOREIGN TABLE Properties (
	Name string(4000) NOT NULL,
	"Value" string(4000) NOT NULL,
	UID string(50) NOT NULL,
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
	DEFERRABILITY integer,
	FK_UID string(50)
);

CREATE FOREIGN TABLE Schemas (
	VDBName string(255),
	Name string(255),
	IsPhysical boolean NOT NULL,
	UID string(50) NOT NULL,
	Description string(255),
	PrimaryMetamodelURI string(255) NOT NULL,
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
	SchemaUID string (50) NOT NULL,
	PRIMARY KEY (VDBName, SchemaName, Name),
	FOREIGN KEY (VDBName, SchemaName) REFERENCES Schemas (VDBName, Name),
	FOREIGN KEY (SchemaUID) REFERENCES Schemas (UID),
	UNIQUE (UID)	
);

CREATE FOREIGN TABLE VirtualDatabases (
	Name string(255) NOT NULL,
	Version string(50) NOT NULL,
	Description string(4000),
	LoadingTimestamp timestamp,
	ActiveTimestamp timestamp,
	PRIMARY KEY (Name, Version)
);

CREATE VIEW spatial_ref_sys (
    srid integer primary key,
    auth_name string(256),
    auth_srid integer,
    srtext string(2048),
    proj4text string(2048))
    OPTIONS (MATERIALIZED true)
AS select t.* from objecttable('teiid_context' COLUMNS x clob 'teiid_row.spatialRefSys') o
, texttable(o.x columns srid integer, auth_name string, auth_srid integer, srtext string, proj4text string skip 1) t;

CREATE VIEW GEOMETRY_COLUMNS ( 
    F_TABLE_CATALOG VARCHAR(256) NOT NULL, 
    F_TABLE_SCHEMA VARCHAR(256) NOT NULL, 
    F_TABLE_NAME VARCHAR(256) NOT NULL, 
    F_GEOMETRY_COLUMN VARCHAR(256) NOT NULL,
    COORD_DIMENSION INTEGER NOT NULL, 
    SRID INTEGER NOT NULL, 
    TYPE VARCHAR(30) NOT NULL)
as select c.VDBName, c.SchemaName, c.TableName, c.Name, 
  nvl(cast((select "value" from sys.properties where uid = c.UID and name='{http://www.teiid.org/translator/spatial/2015}coord_dimension') as integer), 2), 
  nvl(cast((select "value" from sys.properties where uid = c.UID and name='{http://www.teiid.org/translator/spatial/2015}srid') as integer), 0),
  nvl((select "value" from sys.properties where uid = c.UID and name='{http://www.teiid.org/translator/spatial/2015}type'), 'GEOMETRY') 
  from sys.columns as c where DataType = 'geometry';

CREATE VIEW GEOGRAPHY_COLUMNS ( 
    F_TABLE_CATALOG VARCHAR(256) NOT NULL, 
    F_TABLE_SCHEMA VARCHAR(256) NOT NULL, 
    F_TABLE_NAME VARCHAR(256) NOT NULL, 
    F_GEOMETRY_COLUMN VARCHAR(256) NOT NULL,
    COORD_DIMENSION INTEGER NOT NULL, 
    SRID INTEGER NOT NULL, 
    TYPE VARCHAR(30) NOT NULL)
as select c.VDBName, c.SchemaName, c.TableName, c.Name, 
  nvl(cast((select "value" from sys.properties where uid = c.UID and name='{http://www.teiid.org/translator/spatial/2015}coord_dimension') as integer), 2), 
  nvl(cast((select "value" from sys.properties where uid = c.UID and name='{http://www.teiid.org/translator/spatial/2015}srid') as integer), 4326),
  nvl((select "value" from sys.properties where uid = c.UID and name='{http://www.teiid.org/translator/spatial/2015}type'), 'GEOGRAPY') 
  from sys.columns as c where DataType = 'geography';
  
CREATE FOREIGN PROCEDURE ARRAYITERATE (val object[]) RETURNS TABLE (col object);
