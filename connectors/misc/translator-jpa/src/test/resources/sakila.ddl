CREATE FOREIGN TABLE country (
  country_id integer  NOT NULL AUTO_INCREMENT,
  country varchar(50) NOT NULL,
  last_update timestamp NOT NULL,
  PRIMARY KEY  (country_id)
);

CREATE FOREIGN TABLE city (
  city_id integer  NOT NULL AUTO_INCREMENT,
  city varchar(50) NOT NULL,
  country_id integer  NOT NULL OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.country',
      "teiid_jpa:relation_property" 'country',
      "teiid_jpa:relation_key" 'country_id'),
  last_update timestamp NOT NULL,
  PRIMARY KEY  (city_id),
  FOREIGN KEY (country_id) REFERENCES country (country_id) OPTIONS(NAMEINSOURCE 'country')
);

CREATE FOREIGN TABLE address (
  address_id integer NOT NULL AUTO_INCREMENT,
  address varchar(50) NOT NULL,
  address2 varchar(50),
  district varchar(20) NOT NULL,
  city_id integer  NOT NULL OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.city',
      "teiid_jpa:relation_property" 'city',
      "teiid_jpa:relation_key" 'city_id'
      ),
  postal_code varchar(10),
  phone varchar(20) NOT NULL,
  last_update timestamp NOT NULL,
  PRIMARY KEY  (address_id),
  FOREIGN KEY (city_id) REFERENCES city (city_id) OPTIONS(NAMEINSOURCE 'city')
);

CREATE FOREIGN TABLE staff (
  staff_id byte NOT NULL AUTO_INCREMENT,
  first_name varchar(45) NOT NULL,
  last_name varchar(45) NOT NULL,
  address_id integer NOT NULL OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.address',
      "teiid_jpa:relation_property" 'address',
      "teiid_jpa:relation_key" 'address_id'),
  email varchar(50),
  store_id byte NOT NULL OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.store',
      "teiid_jpa:relation_property" 'store',
      "teiid_jpa:relation_key" 'store_id'),
  active boolean NOT NULL,
  username varchar(16) NOT NULL,
  password varchar(40),
  last_update timestamp,
  PRIMARY KEY  (staff_id),
  FOREIGN KEY (store_id) REFERENCES store (store_id) OPTIONS(NAMEINSOURCE 'store'),
  FOREIGN KEY (address_id) REFERENCES address (address_id) OPTIONS(NAMEINSOURCE 'address')
);

CREATE FOREIGN TABLE store (
  store_id byte  NOT NULL AUTO_INCREMENT,
  manager_staff_id byte NOT NULL OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.staff',
      "teiid_jpa:relation_property" 'manager_staff',
      "teiid_jpa:relation_key" 'manager_staff_id'),
  address_id integer NOT NULL OPTIONS ("teiid_jpa:assosiated_with_table" 'sakila.store'),
  last_update timestamp NOT NULL,
  PRIMARY KEY  (store_id),
  FOREIGN KEY (manager_staff_id) REFERENCES staff (staff_id) OPTIONS(NAMEINSOURCE 'staff'),
  FOREIGN KEY (address_id) REFERENCES address (address_id) OPTIONS(NAMEINSOURCE 'address')
);

CREATE FOREIGN TABLE customer (
  customer_id integer NOT NULL AUTO_INCREMENT,
  store_id byte NOT NULL OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.store',
      "teiid_jpa:relation_property" 'store',
      "teiid_jpa:relation_key" 'store_id'),
  first_name varchar(45) NOT NULL,
  last_name varchar(45) NOT NULL,
  email varchar(50),
  address_id integer  NOT NULL OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.address',
      "teiid_jpa:relation_property" 'address',
      "teiid_jpa:relation_key" 'address_id'),
  active boolean NOT NULL,
  create_date timestamp NOT NULL,
  last_update timestamp,
  PRIMARY KEY  (customer_id),
  FOREIGN KEY (address_id) REFERENCES address (address_id) OPTIONS(NAMEINSOURCE 'address'),
  FOREIGN KEY (store_id) REFERENCES store (store_id) OPTIONS(NAMEINSOURCE 'store')
);

CREATE FOREIGN TABLE thing_type (
  id integer NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL,
  PRIMARY KEY (id)
);

CREATE FOREIGN TABLE thing (
  id integer NOT NULL AUTO_INCREMENT,
  parent_id integer NOT NULL OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.thing',
      "teiid_jpa:relation_property" 'parent',
      "teiid_jpa:relation_key" 'id'),
  thing_type_id integer OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.thing_type',
      "teiid_jpa:relation_property" 'thing_type',
      "teiid_jpa:relation_key" 'id'),
  thing_subtype_id integer OPTIONS (
      "teiid_jpa:assosiated_with_table" 'sakila.thing_type',
      "teiid_jpa:relation_property" 'thing_subtype',
      "teiid_jpa:relation_key" 'id'),
  PRIMARY KEY  (id),
  FOREIGN KEY (parent_id) REFERENCES thing (id) OPTIONS(NAMEINSOURCE 'parent'),
  FOREIGN KEY (thing_type_id) REFERENCES thing_type (id) OPTIONS(NAMEINSOURCE 'thing_type'),
  FOREIGN KEY (thing_subtype_id) REFERENCES thing_type (id) OPTIONS(NAMEINSOURCE 'thing_subtype')
);

CREATE FOREIGN TABLE thing_with_embedded (
  id integer NOT NULL AUTO_INCREMENT,
  prop1 integer OPTIONS(NAMEINSOURCE 'embedded.prop1'),
  prop2 integer OPTIONS(NAMEINSOURCE 'embedded.prop2'),
  PRIMARY KEY (id)
);