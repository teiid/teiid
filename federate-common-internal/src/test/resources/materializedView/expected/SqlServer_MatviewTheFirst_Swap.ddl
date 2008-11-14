-- SqlServer script. Swap View_1 cache tables.
EXEC sp_rename 'A_physical_table_in_src', 'A_physical_table_in_src_TEMP';
EXEC sp_rename 'A_physical_staging_table_in_src', 'A_physical_table_in_src';
EXEC sp_rename 'A_physical_table_in_src_TEMP', 'A_physical_staging_table_in_src';

-- SqlServer script. Swap View_2 cache tables.
EXEC sp_rename 'A_physical_table_in_src', 'A_physical_table_in_src_TEMP';
EXEC sp_rename 'A_physical_staging_table_in_src', 'A_physical_table_in_src';
EXEC sp_rename 'A_physical_table_in_src_TEMP', 'A_physical_staging_table_in_src';

-- SqlServer script. Swap View_3 cache tables.
EXEC sp_rename 'A_physical_table_in_src', 'A_physical_table_in_src_TEMP';
EXEC sp_rename 'A_physical_staging_table_in_src', 'A_physical_table_in_src';
EXEC sp_rename 'A_physical_table_in_src_TEMP', 'A_physical_staging_table_in_src';
