To run scripts on NT:

-->>Open a command window:<<--
Start->Programs->DB2 for Windows NT->Command Window


-->>Connect to database:<<--
db2 connect to dbname user <user> using <password>


-->>Create repository:<<--
run schema script: db2 -td% -vf mm_create.sql -z mm_create.out


-->>Drop tables:<<--
Run metadata script: db2 -td% -vf mm_drop.sql -z mm_drop.out

To run scripts on NT:
Assumes IBM DB2 client tools are installed on PC.

-->>Open a DB2 command window:<<--
Start->Programs->IBM DB2->Command Window


-->>Should navigate to the directory that contains SQL scripts or use fully qualified path names in commands below.<<--


-->>Connect to database:<<--
db2 connect to <dbname> user <user> using <password>
Note:  <dbname> is database alias already defined in Client Configuration Assistant.


-->>To create a repository:<<--
Run schema script by entering the following line at command prompt:
db2 -td% -vf mm_create.sql -z mm_create.out
Note:  'mm_create.out' is a log file and will be written to current directory.


-->>To drop tables:<<--
Run metadata script by entering following line at command prompt:
db2 -td% -vf mm_drop.sql -z mm_drop.out
Note:  'mm_drop.out' is a log file and will be written to current directory.


