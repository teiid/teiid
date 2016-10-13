create database empty version '2';

create foreign translator y;use database empty version '2';

create foreign data wrapper y2 type y OPTIONS (key 'value'); 

create server z type 'custom' version 'one' foreign data wrapper y2 options(key 'value');

create schema PM1 server z;

--import foreign schema anyschema FROM SERVER z into PM1;  

create foreign table mytable ("my-column" string) OPTIONS(UPDATABLE true);

create role admin with jaas role superuser;

grant select,update ON TABLE "PM1.mytable" TO admin; 

