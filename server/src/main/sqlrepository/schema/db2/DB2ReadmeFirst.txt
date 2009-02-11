Because the MetaMatrix repository has tables where a single record could exceed 4096 (the default pagesize for DB2) we need to have a tablespace that has a pagesize of at least 32k.

In DB2 there must also be a corresponding bufferpool with the same pagesize.
So, in order to run our mm_create.sql script for DB2 the following steps need to be done BEFORE running this script.

1) Create/have already a bufferpool with a pagesize of 32k or larger.
2) Create/have already a tablespace with a pagesize of 32k or larger (matching a bufferpool)


Also user needs Create Package Authority
