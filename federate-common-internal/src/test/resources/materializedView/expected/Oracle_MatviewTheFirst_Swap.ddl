-- Oracle script. Swap View_1 cache tables.
ALTER TABLE A_physical_table_in_src RENAME TO A_physical_table_in_src_TEMP;
ALTER TABLE A_physical_staging_table_in_src RENAME TO A_physical_table_in_src;
ALTER TABLE A_physical_table_in_src_TEMP RENAME TO A_physical_staging_table_in_src;

-- Oracle script. Swap View_2 cache tables.
ALTER TABLE A_physical_table_in_src RENAME TO A_physical_table_in_src_TEMP;
ALTER TABLE A_physical_staging_table_in_src RENAME TO A_physical_table_in_src;
ALTER TABLE A_physical_table_in_src_TEMP RENAME TO A_physical_staging_table_in_src;

-- Oracle script. Swap View_3 cache tables.
ALTER TABLE A_physical_table_in_src RENAME TO A_physical_table_in_src_TEMP;
ALTER TABLE A_physical_staging_table_in_src RENAME TO A_physical_table_in_src;
ALTER TABLE A_physical_table_in_src_TEMP RENAME TO A_physical_staging_table_in_src;
