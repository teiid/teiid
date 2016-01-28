CREATE FOREIGN TABLE payment (
    payment_id integer NOT NULL PRIMARY KEY OPTIONS(NAMEINSOURCE 'rowid'),
    rental_id integer OPTIONS("teiid_accumulo:CF" 'payment', "teiid_accumulo:CQ" 'rental_rowid'),
    amount decimal(9,2) OPTIONS("teiid_accumulo:CF" 'payment', "teiid_accumulo:CQ" 'amount'),
    FOREIGN KEY (rental_id) REFERENCES rental (rental_id)
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE rental (
    rental_id integer PRIMARY KEY OPTIONS(NAMEINSOURCE 'rowid'),
    amount decimal(9,2) NOT NULL OPTIONS("teiid_accumulo:CF" 'amount', "teiid_accumulo:VALUE-IN" '{CQ}'),
    customer_id integer OPTIONS("teiid_accumulo:CF" 'rental', "teiid_accumulo:CQ" 'customer_rowid'),
    FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE customer (
    customer_id integer PRIMARY KEY OPTIONS(NAMEINSOURCE 'rowid'),
    firstName varchar(25) OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'firstNameAttribute'),
    lastName varchar(25) OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'lastNameAttribute')
) OPTIONS(UPDATABLE 'TRUE');

CREATE FOREIGN TABLE smalla(
    ROWID integer PRIMARY KEY OPTIONS(NAMEINSOURCE 'rowid'),
    STRINGKEY string OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'stringkey'),
    INTNUM integer OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'intnum'),
    STRINGNUM string OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'stringnum'),
    FLOATNUM float OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'floatnum'),
    LONGNUM long OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'longnum'),
    DOUBLENUM double OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'doublenum'),
    BYTENUM byte OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'bytenum'),
    DATEVALUE date OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'datevalue'),
    TIMEVALUE time OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'timevalue'),
    TIMESTAMPVALUE timestamp OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'timestampvalue'),
    BOOLEANVALUE boolean OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'booleanvalue'),
    CHARVALUE string OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'charvalue'),
    SHORTVALUE smallint OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'shortvalue'),
    BIGINTEGERVALUE biginteger OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'bigintvalue'),
    BIGDECIMALVALUE bigdecimal OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'bigdvalue'),
    OBJECTVALUE varbinary OPTIONS("teiid_accumulo:CF" 'customer', "teiid_accumulo:CQ" 'objvalue')
) OPTIONS(UPDATABLE 'TRUE');