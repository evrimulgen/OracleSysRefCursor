-- Execute in SQL/Plus
---------------------------

SET WRAP OFF LINESIZE 200 TRUNC OFF TAB OFF DEFINE OFF SERVEROUTPUT ON SIZE 100000

DROP TABLE legend purge;
CREATE TABLE legend(id NUMBER PRIMARY KEY, christianName VARCHAR2(10), surname VARCHAR2(10), dob DATE);

INSERT INTO legend(id, christianName, surname, dob)
 VALUES(5, 'Freddie', 'Mercury', DATE '1946-09-05'); 
INSERT INTO legend(id, christianName, surname, dob)
 VALUES(6, 'Brian', 'May', DATE '1947-07-19'); 
INSERT INTO legend(id, christianName, surname, dob)
 VALUES(7, 'Roger', 'Taylor', DATE '1949-07-26'); 
INSERT INTO legend(id, christianName, surname, dob)
 VALUES(8, 'John', 'Deacon', DATE '1951-08-19');
INSERT INTO legend(id, christianName, surname, dob)
 VALUES(20, 'Roger', 'Waters', DATE '1943-09-06'); 
INSERT INTO legend(id, christianName, surname, dob)
 VALUES(21, 'David', 'Gilmour', DATE '1946-03-06'); 
INSERT INTO legend(id, christianName, surname, dob)
 VALUES(22, 'Nick', 'Mason', DATE '1944-01-27'); 
INSERT INTO legend(id, christianName, surname, dob)
 VALUES(23, 'Richard', 'Wright', DATE '1943-07-28');
 
DROP TABLE band purge;
CREATE TABLE band(id NUMBER PRIMARY KEY, bandName VARCHAR2(10));

INSERT INTO band(id, bandName)
 VALUES(1, 'Pink Floyd');
INSERT INTO band(id, bandName)
 VALUES(2, 'Queen');


DROP TABLE bandLegend;
CREATE TABLE bandLegend(bandID NUMBER, legendID NUMBER);
INSERT INTO bandLegend(bandID, legendID)
 VALUES(1, 20);
INSERT INTO bandLegend(bandID, legendID)
 VALUES(1, 21);
INSERT INTO bandLegend(bandID, legendID)
 VALUES(1, 22);
INSERT INTO bandLegend(bandID, legendID)
 VALUES(1, 23);
INSERT INTO bandLegend(bandID, legendID)
 VALUES(2, 5);
 INSERT INTO bandLegend(bandID, legendID)
 VALUES(2, 6);
INSERT INTO bandLegend(bandID, legendID)
 VALUES(2, 7);
INSERT INTO bandLegend(bandID, legendID)
 VALUES(2, 8);
 

SELECT l.christianName, l.surname, l.dob, b.bandName
 FROM legend l,
      band b,
      bandLegend bd
  WHERE bd.legendID = l.id
    AND bd.bandID = b.id;



CREATE OR REPLACE PACKAGE utilRefCursor AS

    /* 
     * Upsert in the current ref cursor
     */
    PROCEDURE populate(src IN sys_refcursor);

    /*
     * Return the merged result set
     */
    FUNCTION resultSet RETURN sys_refcursor;

    /*
     * Teardown
     */
    PROCEDURE tearDown;

 END utilRefCursor;
/




CREATE OR REPLACE PACKAGE BODY utilRefCursor AS

     tempTableName CONSTANT VARCHAR2(10) := 'TEMPTABLE';
     idColumnName CONSTANT CHAR(2) := 'ID';
     
     idColumnNumber NUMBER := NULL;
     
     columnCount NUMBER;
     describeTable DBMS_SQL.desc_tab;

     cursorNumber NUMBER;
     
    /*
     * From the provided REF CURSOR, get the ID column number, returning NULL if it does not exist.
     */     
    PROCEDURE setState(src IN sys_refcursor) IS
     r1 sys_refcursor := src;
     BEGIN
      r1 := src;
      cursorNumber := DBMS_SQL.TO_CURSOR_NUMBER(r1);
      DBMS_SQL.DESCRIBE_COLUMNS(c => cursorNumber, col_cnt => columnCount, desc_t => describeTable);
  
      idColumnNumber := NULL;
      FOR colNo IN 1..columnCount
        LOOP
          idColumnNumber := CASE
                              WHEN describeTable(colNo).col_name = 'ID' THEN colNo
                              ELSE idColumnNumber
                            END;
        END LOOP;
     END;

    /*
     * Add columns from ref cursor by name to temporary table
     */
    PROCEDURE addColsToTempTable(src IN sys_refcursor) IS
     BEGIN
  
      IF idColumnNumber IS NOT NULL THEN
  
            FOR colNo IN 1..columnCount
              LOOP
                FOR col IN (
                              SELECT describeTable(colNo).col_name columnName,
                                     describeTable(colNo).col_type columnType,
                                     describeTable(colNo).col_max_len columnMaxLength,
                                     NVL2(utc.column_name, 'Y', 'N') columnPresentInTT
                                FROM user_tab_cols utc
                                  RIGHT OUTER JOIN DUAL
                                    ON (utc.table_name = tempTableName AND utc.column_name = describeTable(colNo).col_name)
                                    WHERE describeTable(colNo).col_type IN (1, 2, 12, 96) -- not bothering with CLOB, XMLTYPE, TIMESTAMP, NUMERic precision and scale etc.
                           ) LOOP
                               IF col.columnPresentInTT = 'N' THEN
                                   EXECUTE IMMEDIATE 'ALTER TABLE ' || tempTableName
                                                                    || '   ADD ' || col.columnName
                                                                    || ' '
                                                                    || CASE
                                                                         WHEN col.columnType = 1 THEN 'VARCHAR2(' || col.columnMaxLength || ')'
                                                                         WHEN col.columnType = 2 THEN 'NUMBER'
                                                                         WHEN col.columnType = 12 THEN 'DATE'
                                                                         WHEN col.columnType = 96 THEN 'CHAR(' || col.columnMaxLength || ')'
                                                                        END;
                                                         
                               END IF;
                            END LOOP;
              END LOOP;
      END IF;  
     END addColsToTempTable;



    /*
     * Setup the system to perform the utility functions
     */
    PROCEDURE setup(src IN sys_refcursor) IS
     BEGIN

      -- 1. Create the table to hold the REF CURSOR content, if it doesn't already exist
      FOR r IN (
                 SELECT 1
                  FROM user_tables ut
                    RIGHT OUTER JOIN DUAL
                      ON (ut.table_name = tempTableName)
                        WHERE ut.table_name IS NULL
               ) LOOP
                   EXECUTE IMMEDIATE 'CREATE TABLE ' || tempTableName || '(id NUMBER PRIMARY KEY)';
                END LOOP;

      -- 2. Get package internal state for subsequent operations
      setState(src => src);
     
      -- 3. Add the columns from the REF CURSOR to the GTT iif they don't already exist
      addColsToTempTable(src => src);
     END setup;





    /* 
     * Upsert the ref cursor content
     */
    PROCEDURE populate(src IN sys_refcursor) IS
    
     dataTypeVarchar2 VARCHAR(32767);
     dataTypeNumber NUMBER;
     dataTypeDate DATE;
     
     idColumnValue NUMBER; -- Assuming that the ID column value data type is a NUMBER that will do for demo code, a PoC
    
     BEGIN
      setup(src => src);
      
      FOR colNo IN 1..columnCount
         LOOP
           IF describeTable(colNo).col_type = 1 THEN DBMS_SQL.DEFINE_COLUMN(cursorNumber, colNo, dataTypeVarchar2, 32767);
             ELSIF describeTable(colNo).col_type = 2 THEN DBMS_SQL.DEFINE_COLUMN(cursorNumber, colNo, dataTypeNumber);
               ELSIF describeTable(colNo).col_type = 12 THEN DBMS_SQL.DEFINE_COLUMN(cursorNumber, colNo, dataTypeDate);
                 ELSIF describeTable(colNo).col_type = 96 THEN DBMS_SQL.DEFINE_COLUMN(cursorNumber, colNo, dataTypeVarchar2, 32767);
           END IF;
         END LOOP;
         
      -- And now step through the REF CURSOR row at a time, retrieve the values, and populate the GTT
      WHILE DBMS_SQL.FETCH_ROWS(cursorNumber) > 0
        LOOP
          DBMS_SQL.COLUMN_VALUE(cursorNumber, idColumnNumber, idColumnValue);

          FOR colNo IN 1..columnCount
            LOOP
               IF describeTable(colNo).col_name != idColumnName THEN
               
                     IF describeTable(colNo).col_type = 1 THEN DBMS_SQL.COLUMN_VALUE(cursorNumber, colNo, dataTypeVarchar2); 
                       ELSIF describeTable(colNo).col_type = 2 THEN DBMS_SQL.COLUMN_VALUE(cursorNumber, colNo, dataTypeNumber); 
                         ELSIF describeTable(colNo).col_type = 12 THEN DBMS_SQL.COLUMN_VALUE(cursorNumber, colNo, dataTypeDate); 
                           ELSIF describeTable(colNo).col_type = 96 THEN DBMS_SQL.COLUMN_VALUE(cursorNumber, colNo, dataTypeVarchar2); 
                     END IF;

                     EXECUTE IMMEDIATE 'MERGE INTO ' || tempTableName || ' tt'
                                                     || ' USING(SELECT :colID1 id FROM DUAL) r'
                                                     || ' ON (r.id = tt.id)'
                                                     || '  WHEN MATCHED THEN UPDATE SET '     || describeTable(colNo).col_name || '= :collVal1'
                                                     || '  WHEN NOT MATCHED THEN INSERT(id, ' || describeTable(colNo).col_name || ') VALUES(:colID2, :colVal2)'
                                USING idColumnValue,
                                      CASE
                                         WHEN describeTable(colNo).col_type = 2 THEN TO_CHAR(dataTypeNumber)
                                         WHEN describeTable(colNo).col_type = 12 THEN TO_CHAR(dataTypeDate)
                                         ELSE dataTypeVarchar2
                                      END,
                                      idColumnValue,
                                      CASE
                                         WHEN describeTable(colNo).col_type = 2 THEN TO_CHAR(dataTypeNumber)
                                         WHEN describeTable(colNo).col_type = 12 THEN TO_CHAR(dataTypeDate)
                                         ELSE dataTypeVarchar2
                                      END;
               END IF;
            END LOOP;
        END LOOP;

     END populate;


    /*
     * Return the merged result set
     */
    FUNCTION resultSet RETURN sys_refcursor IS
     rc sys_refcursor;
     BEGIN
      OPEN rc FOR 'SELECT * FROM ' || tempTableName;
      RETURN rc;        
     END resultSet;


    /*
     * Teardown
     */
    PROCEDURE tearDown IS
     BEGIN
      FOR r IN (
                 SELECT 1
                  FROM user_tables ut
                    WHERE ut.table_name = tempTableName
               ) LOOP
                   EXECUTE IMMEDIATE 'DROP TABLE ' || tempTableName;
                END LOOP;
      idColumnNumber := NULL;
     END tearDown;

END utilRefCursor;
/



VAR rc REFCURSOR

DECLARE
     TYPE tsrc IS REF CURSOR;
     rc1 tsrc;
     rc2 tsrc;
BEGIN
   utilRefCursor.tearDown;
   
   -- Step 1, populate the legends. In this query, rc1 is a REF CURSOR created in this block, but it could equally
   --    be a REF CURSOR returned from a stored procedure or other PSM
   OPEN rc1 FOR
     SELECT id, christianName, surname, dob
       FROM legend;
    utilRefCursor.populate(rc1);
    
  -- Step 2, merge in some more content, and from a second REF CURSOR. As above, the REF CURSOR could 
  --   come from some other inexcessible process, not necessarily created on the fly by querying a 
  --   small handful of tables.
  OPEN rc2 FOR
   SELECT l.id, b.bandName
     FROM legend l,
          band b,
          bandLegend bl
      WHERE bl.legendID = l.id
      AND bl.bandID = b.id;
  utilRefCursor.populate(rc2);
  
  -- Step 3, populate the SQL/Plus cursor variable, that could equally be returned to
  --         some invoking process
  :rc := utilRefCursor.resultSet;
END;
/
















