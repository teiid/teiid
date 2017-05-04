create database empty version '2';

use database empty version '2';

create foreign translator y;use database empty version '2';

create server z type 'custom' version 'one' foreign data wrapper y options(key 'value');

create schema PM1 server z;

create role admin with jaas role superuser;

import foreign schema anyschema FROM SERVER z into PM1;

import foreign schema anyschema FROM REPOSITORY myrepo into PM1 OPTIONS(myrepokey 'value');

set schema PM1;

create foreign table mytable ("my-column" string) OPTIONS(UPDATABLE true);

grant select,update ON TABLE "PM1.mytable" TO admin; 

