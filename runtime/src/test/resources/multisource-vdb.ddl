CREATE DATABASE multisource VERSION '1';

USE DATABASE multisource VERSION '1';

CREATE SERVER "text-connector" FOREIGN DATA WRAPPER "file" OPTIONS ("resource-name" 'java:/test-file');

CREATE SCHEMA MarketData SERVER "text-connector" OPTIONS (multisource true, "multisource.addColumn" true);

IMPORT FROM SERVER "text-connector" INTO MarketData;
