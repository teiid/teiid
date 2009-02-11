-- DB2 script. Swap MatviewVDB cache tables.
RENAME TABLE A_physical_table_in_src TO A_physical_table_in_src_TEMP;
RENAME TABLE A_physical_staging_table_in_src TO A_physical_table_in_src;
RENAME TABLE A_physical_table_in_src_TEMP TO A_physical_staging_table_in_src;

-- Oracle script. Swap MatviewVDB cache tables.
ALTER TABLE A_physical_table_in_src RENAME TO A_physical_table_in_src_TEMP;
ALTER TABLE A_physical_staging_table_in_src RENAME TO A_physical_table_in_src;
ALTER TABLE A_physical_table_in_src_TEMP RENAME TO A_physical_staging_table_in_src;

-- SqlServer script. Swap MatviewVDB cache tables.
EXEC sp_rename 'A_physical_table_in_src', 'A_physical_table_in_src_TEMP';
EXEC sp_rename 'A_physical_staging_table_in_src', 'A_physical_table_in_src';
EXEC sp_rename 'A_physical_table_in_src_TEMP', 'A_physical_staging_table_in_src';
