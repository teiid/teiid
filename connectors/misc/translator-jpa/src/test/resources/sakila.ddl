CREATE FOREIGN TABLE country (
  country_id integer  NOT NULL AUTO_INCREMENT,
  country varchar(50) NOT NULL,
  last_update timestamp NOT NULL,
  PRIMARY KEY  (country_id)
);

CREATE FOREIGN TABLE city (
  city_id integer  NOT NULL AUTO_INCREMENT,
  city varchar(50) NOT NULL,
  country_id integer  NOT NULL OPTIONS ("teiid_jpa:assosiated_with_table" 'sakila.country'),
  last_update timestamp NOT NULL,
  PRIMARY KEY  (city_id),
  FOREIGN KEY (country_id) REFERENCES country (country_id) OPTIONS(NAMEINSOURCE 'country')
);

CREATE FOREIGN TABLE address (
  address_id integer NOT NULL AUTO_INCREMENT,
  address varchar(50) NOT NULL,
  address2 varchar(50),
  district varchar(20) NOT NULL,
  city_id integer  NOT NULL OPTIONS ("teiid_jpa:assosiated_with_table" 'sakila.city'),
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
  address_id integer NOT NULL OPTIONS ("teiid_jpa:assosiated_with_table" 'sakila.address'),
  email varchar(50),
  store_id byte NOT NULL OPTIONS ("teiid_jpa:assosiated_with_table" 'sakila.store'),
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
  manager_staff_id byte NOT NULL OPTIONS ("teiid_jpa:assosiated_with_table" 'sakila.staff'),
  address_id integer NOT NULL OPTIONS ("teiid_jpa:assosiated_with_table" 'sakila.store'),
  last_update timestamp NOT NULL,
  PRIMARY KEY  (store_id),
  FOREIGN KEY (manager_staff_id) REFERENCES staff (staff_id) OPTIONS(NAMEINSOURCE 'staff'),
  FOREIGN KEY (address_id) REFERENCES address (address_id) OPTIONS(NAMEINSOURCE 'address')
);

CREATE FOREIGN TABLE customer (
  customer_id integer NOT NULL AUTO_INCREMENT,
  store_id byte NOT NULL OPTIONS ("teiid_jpa:assosiated_with_table" 'sakila.store'),
  first_name varchar(45) NOT NULL,
  last_name varchar(45) NOT NULL,
  email varchar(50),
  address_id integer  NOT NULL OPTIONS ("teiid_jpa:assosiated_with_table" 'sakila.address'),
  active boolean NOT NULL,
  create_date timestamp NOT NULL,
  last_update timestamp,
  PRIMARY KEY  (customer_id),
  FOREIGN KEY (address_id) REFERENCES address (address_id) OPTIONS(NAMEINSOURCE 'address'),
  FOREIGN KEY (store_id) REFERENCES store (store_id) OPTIONS(NAMEINSOURCE 'store')
);