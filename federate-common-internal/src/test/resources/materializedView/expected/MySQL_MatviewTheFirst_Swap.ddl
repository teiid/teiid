-- MySQL script. Swap View_1 cache tables.
RENAME TABLE A_physical_table_in_src TO A_physical_table_in_src_TEMP;
RENAME TABLE A_physical_staging_table_in_src TO A_physical_table_in_src;
RENAME TABLE A_physical_table_in_src_TEMP TO A_physical_staging_table_in_src;

-- MySQL script. Swap View_2 cache tables.
RENAME TABLE A_physical_table_in_src TO A_physical_table_in_src_TEMP;
RENAME TABLE A_physical_staging_table_in_src TO A_physical_table_in_src;
RENAME TABLE A_physical_table_in_src_TEMP TO A_physical_staging_table_in_src;

-- MySQL script. Swap View_3 cache tables.
RENAME TABLE A_physical_table_in_src TO A_physical_table_in_src_TEMP;
RENAME TABLE A_physical_staging_table_in_src TO A_physical_table_in_src;
RENAME TABLE A_physical_table_in_src_TEMP TO A_physical_staging_table_in_src;
