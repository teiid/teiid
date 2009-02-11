-- DB2 script. Truncate MatviewVDB cache table.
DELETE FROM A_physical_staging_table_in_src;

-- Oracle script. Truncate MatviewVDB cache table.
TRUNCATE TABLE A_physical_staging_table_in_src REUSE STORAGE;

-- SqlServer script. Truncate MatviewVDB cache table.
TRUNCATE TABLE A_physical_staging_table_in_src;
