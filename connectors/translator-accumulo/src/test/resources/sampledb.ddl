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