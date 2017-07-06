CREATE DATABASE multisource VERSION '1';

USE DATABASE multisource VERSION '1';

CREATE FOREIGN DATA WRAPPER "file";

CREATE SERVER "text-connector" FOREIGN DATA WRAPPER "file" OPTIONS ("jndi-name" 'java:/test-file');

CREATE SCHEMA MarketData SERVER "text-connector" OPTIONS (multisource true, "multisource.addColumn" true);

IMPORT FOREIGN SCHEMA "%" FROM SERVER "text-connector" INTO MarketData;
