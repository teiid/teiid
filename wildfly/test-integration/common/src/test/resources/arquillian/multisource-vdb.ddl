CREATE DATABASE multisource VERSION '1';

USE DATABASE multisource VERSION '1';

CREATE FOREIGN DATA WRAPPER "file1" TYPE "file" OPTIONS (exceptionIfFileNotFound false);

CREATE SERVER "text-connector" FOREIGN DATA WRAPPER "file1" OPTIONS ("resource-name" 'java:/test-file');

CREATE SCHEMA MarketData SERVER "text-connector" OPTIONS (multisource true);

IMPORT FOREIGN SCHEMA "%" FROM SERVER "text-connector" INTO MarketData;
