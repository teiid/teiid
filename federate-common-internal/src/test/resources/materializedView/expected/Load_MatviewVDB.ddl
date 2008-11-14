-- MetaMatrix script. Populate MatviewVDB cache table.
SELECT col_1, col_2, col_3, col_4, col_5 INTO A_physical_staging_table FROM A_virtual_table OPTION NOCACHE;
