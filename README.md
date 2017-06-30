# What it does and how it works ?

If you want to migrate data from one redshift instance to another instance 
may be in same or different account . 
You can provide list of tables you want to move . Script will create schema is does not exists and will create table as well using the view created in "How to use ?" section given below. 

ToDo : Will add later to migrate just by providing schema names list.  
Just read *How to use ?* given below . 

You will need AWS S3 account access details along with root user credentials for source and destination redshift instance . As it will unload data on S3 first from source and then copy it to desination. 

Make sure in your vpc settings that the script run on a server through which it can connect with both the Redshift instances. 


# Setup :
```
make install
```

---

# How to use ?
### 1) Run SQL commands given below before jumping to anything else . It will generate a view that will help us get the create statement for any table as there is no "show create table in redshift". 

```
create schema if not admin ;
COMMIT; 
```
and then create view using sql command given below : 

```
CREATE OR REPLACE VIEW admin.v_generate_tbl_ddl
AS
SELECT
 schemaname
 ,tablename
 ,seq
 ,ddl
FROM
 (
 SELECT
  schemaname
  ,tablename
  ,seq
  ,ddl
 FROM
  (

  SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,0 AS seq
   ,'--DROP TABLE "' + n.nspname + '"."' + c.relname + '";' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'

  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,2 AS seq
   ,'CREATE TABLE IF NOT EXISTS "' + n.nspname + '"."' + c.relname + '"' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'

  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 5 AS seq, '(' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'

  UNION SELECT
   schemaname
   ,tablename
   ,seq
   ,'\t' + col_delim + col_name + ' ' + col_datatype + ' ' + col_nullable + ' ' + col_default + ' ' + col_encoding AS ddl
  FROM
   (
   SELECT
    n.nspname AS schemaname
    ,c.relname AS tablename
    ,100000000 + a.attnum AS seq
    ,CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim
    ,'"' + a.attname + '"' AS col_name
    ,CASE WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')
     WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')
     ELSE UPPER(format_type(a.atttypid, a.atttypmod))
     END AS col_datatype
    ,CASE WHEN format_encoding((a.attencodingtype)::integer) = 'none'
     THEN ''
     ELSE 'ENCODE ' + format_encoding((a.attencodingtype)::integer)
     END AS col_encoding
    ,CASE WHEN a.atthasdef IS TRUE THEN 'DEFAULT ' + adef.adsrc ELSE '' END AS col_default
    ,CASE WHEN a.attnotnull IS TRUE THEN 'NOT NULL' ELSE '' END AS col_nullable
   FROM pg_namespace AS n
   INNER JOIN pg_class AS c ON n.oid = c.relnamespace
   INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
   LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
   WHERE c.relkind = 'r'
     AND a.attnum > 0
   ORDER BY a.attnum
   )

  UNION (SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,200000000 + CAST(con.oid AS INT) AS seq
   ,'\t,' + pg_get_constraintdef(con.oid) AS ddl
  FROM pg_constraint AS con
  INNER JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid
  INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' AND pg_get_constraintdef(con.oid) NOT LIKE 'FOREIGN KEY%'
  ORDER BY seq)

  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 299999999 AS seq, ')' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'

  UNION SELECT
  n.nspname AS schemaname
   ,c.relname AS tablename
   ,300000000 AS seq
   ,'BACKUP NO' as ddl
FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN (SELECT
    SPLIT_PART(key,'_',5) id
    FROM pg_conf
    WHERE key LIKE 'pg_class_backup_%'
    AND SPLIT_PART(key,'_',4) = (SELECT
      oid
      FROM pg_database
      WHERE datname = current_database())) t ON t.id=c.oid
  WHERE c.relkind = 'r'

  UNION SELECT
  n.nspname AS schemaname
   ,c.relname AS tablename
   ,1 AS seq
   ,'--WARNING: This DDL inherited the BACKUP NO property from the source table' as ddl
FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN (SELECT
    SPLIT_PART(key,'_',5) id
    FROM pg_conf
    WHERE key LIKE 'pg_class_backup_%'
    AND SPLIT_PART(key,'_',4) = (SELECT
      oid
      FROM pg_database
      WHERE datname = current_database())) t ON t.id=c.oid
  WHERE c.relkind = 'r'

  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,300000001 AS seq
   ,CASE WHEN c.reldiststyle = 0 THEN 'DISTSTYLE EVEN'
    WHEN c.reldiststyle = 1 THEN 'DISTSTYLE KEY'
    WHEN c.reldiststyle = 8 THEN 'DISTSTYLE ALL'
    ELSE '<<Error - UNKNOWN DISTSTYLE>>'
    END AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'

  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,400000000 + a.attnum AS seq
   ,'DISTKEY ("' + a.attname + '")' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND a.attisdistkey IS TRUE
    AND a.attnum > 0

  UNION select schemaname, tablename, seq,
       case when min_sort <0 then 'INTERLEAVED SORTKEY (' else 'SORTKEY (' end as ddl
from (SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,499999999 AS seq
   ,min(attsortkeyord) min_sort FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
  AND abs(a.attsortkeyord) > 0
  AND a.attnum > 0
  group by 1,2,3 )
  UNION (SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,500000000 + abs(a.attsortkeyord) AS seq
   ,CASE WHEN abs(a.attsortkeyord) = 1
    THEN '\t"' + a.attname + '"'
    ELSE '\t, "' + a.attname + '"'
    END AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  ORDER BY abs(a.attsortkeyord))
  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,599999999 AS seq
   ,'\t)' AS ddl
  FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN  pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0

  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 600000000 AS seq, ';' AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' )
  UNION (
    SELECT 'zzzzzzzz' AS schemaname,
       'zzzzzzzz' AS tablename,
       700000000 + CAST(con.oid AS INT) AS seq,
       'ALTER TABLE ' + n.nspname + '.' + c.relname + ' ADD ' + pg_get_constraintdef(con.oid)::VARCHAR(1024) + ';' AS ddl
    FROM pg_constraint AS con
      INNER JOIN pg_class AS c
              ON c.relnamespace = con.connamespace
             AND c.oid = con.conrelid
      INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
    AND   pg_get_constraintdef (con.oid) LIKE 'FOREIGN KEY%'
    ORDER BY seq
  )
 ORDER BY schemaname, tablename, seq
 )
;
```

```
COMMIT; 
```

---

### 2) in migrate.py script fill these details before using it : 
 BACKUP_TABLES
 
 SOURCE DB Details

 DESTINATION DB details
 
 S3 ACCESS DETAILS

### 3) Run script with command
```
python migrate.py
```

