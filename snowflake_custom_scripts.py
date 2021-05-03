#!/usr/bin/env python
import snowflake.connector
from snowflake.connector import ProgrammingError
import time

conn = snowflake.connector.connect(
    user='username',#CREATED USERNAME IN SNOWFLAKE WITH ROLE ASSIGNED
    password='password',#PASSWORD FOR USERNAME
    account='snowflakeurl'#FIRST BIT OF SNOWFLAKE LOGIN URL
    )
cur = conn.cursor()
cur.execute("USE ROLE ACCOUNTADMIN")#set role
cur.execute("USE WAREHOUSE COMPUTE_WH") #set computer wh
cur.execute("USE DATABASE JDE_TEST")# set  database

cur.execute("USE SCHEMA JDE_SCHEMA ") #set schema

def stagecopy(stage,csvname): 
   
    cur.execute('select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40 from @' +stage+ '/' + csvname +'.csv (file_format => \'mycsvformathead\') limit 1')
    try:        
        query_id = cur.sfqid
        while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
            time.sleep(1)
    except ProgrammingError as err:
        print('Programming Error: {0}'.format(err))
    results = cur.fetchall()
    
    createtblsql = "CREATE TABLE " + csvname +  " AS SELECT "
    headcount = []
    for i in range (0,99):
        if results[0][i] != None :
            
            createtblsql = createtblsql + "$"+ str(i+1)+", "        
            headcount.append(results[0][i])
        else:
            createtblsql = createtblsql[:len(createtblsql)-2] + " FROM @" +stage+ "/" + csvname +".csv"
            break
    cur.execute(createtblsql)

    #Rename columns 
    for i in range(len(headcount)):
        cur.execute("alter table "+csvname+ " rename column $"+str(i+1)+" to " + headcount[i])
    return
        
"""        
#legacy dup check code
def ExcepTest_Dup(plan, mapping,table,pks):
    #Check if staging table contains duplicates
    #enter pks in format "PK1,PK2,PK3" etc, comma between Primary keys
    
    #create stage name
    stage = table+"_S"

    #create aggregated primary keys
    pk = pks.replace(", ",",").replace(",","||'~'||")

    #copy over temp table
    cur.execute("create or replace table " +stage+ " clone " +table)

    cur.execute("select " +pks+ " from " +stage+ " group by " +pks +" having count(*) > 1")
    try:        
        query_id = cur.sfqid
        while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
            time.sleep(1)
    except ProgrammingError as err:
        print('Programming Error: {0}'.format(err))
    results = cur.fetchall()

    #exits if no dups
    if results == None:
        return stage
    else:
        #insert dups into exception table
        cur.execute("insert into ETL_Exceptions (SELECT 'TMP_PLAN', 'TMP_MAPPING', sysdate(), sysdate(), %(pk)s, 'DUPLICATE' from "+stage+ " group by " +pks+ " having count(*) > 1)")
        #remove dups from temp stable
        cur.execute("DELETE FROM " +stage+ "  where %(pk)s in  (select %(pk)s from "+stage+" group by "+pks+" having count(*) > 1)") 
    return stage    
"""

def ExcepTest(plan, mapping,stage,target, pks):
    #Check if staging table contains duplicates
    #Plan: load plan name
    #Mapping: Mapping Name
    #Stage: Transformed table ready to be checked and inserted into target
    #Target: Target table to be loaded into
    #PKs: Primary Keys  in staging table enter pks in format "PK1,PK2,PK3" etc, comma between Primary keys

    #create aggregated primary keys
    pk = pks.replace(", ",",").replace(",","||'~'||")
    target = target.upper()

    #Query to grab the properties of the target table. Column Name, max length, data type, nullable state. Information grabbed from information schema in snowflake 
    cur.execute("""select 
       col.column_name,
       col.character_maximum_length as max_length,
       col.data_type,
       col.IS_NULLABLE
       
        from information_schema.columns col
        join information_schema.tables tab on tab.table_schema = col.table_schema
                                   and tab.table_name = col.table_name
                                   and tab.table_type = 'BASE TABLE'
    where col.TABLE_NAME in ( %(target)s )
     
      and col.table_schema != 'INFORMATION_SCHEMA'
    order by col.table_schema,
         col.table_name,
         col.ordinal_position""", {'target': target,})
    try:        
        query_id = cur.sfqid
        while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
            time.sleep(1)
    except ProgrammingError as err:
        print('Programming Error: {0}'.format(err))
    #Stores query to results
    results = cur.fetchall()

    for i in results:
        #Length check, i[2] Checks for data type
        if i[2] in ("TEXT","VARCHAR","STRING","CHAR","BINARY"):
            #Inserts PKs of  any rows that exceed the max length of a field in the target table into ETL Exception table
            cur.execute("insert into ETL_Exceptions (SELECT 'TMP_PLAN', 'TMP_MAPPING', sysdate(), sysdate(), %(pk)s , 'LENGTH OVERAGE' from "+stage+" where len(" + i[0]+") > "+ str(i[1])+")",{'pk': pk, })
            cur.execute("DELETE FROM " +stage+ "  where %(pk)s in  (select %(pk)s from "+stage+" where len(" + i[0]+") > "+ str(i[1])+")",{'pk': pk, })
        #Num Check
        if i[2] in ("NUMBER","NUMERIC","DECIMAL","FLOAT","DOUBLE","REAL", "INT"):
            #Inserts PKs of rows with any characters detected in the staging table that is going to a numeric field in the target into ETL Exception table
            cur.execute("insert into ETL_Exceptions (SELECT 'TMP_PLAN', 'TMP_MAPPING', sysdate(), sysdate(), %(pk)s, 'CHAR DETECTED IN NUMERIC FIELD' from "+stage+ " where try_to_number(" + i[0]+") IS NULL AND "+i[0]+" IS NOT NULL)",{'pk': pk, })
            cur.execute("DELETE FROM " +stage+ "  where %(pk)s in  (select %(pk)s from "+stage+" where try_to_number(" + i[0]+") IS NULL AND "+i[0]+" IS NOT NULL)",{'pk': pk, })
        #Null Check
        if i[3] == "NO":
            #Inserts PKs of any rows that contain NULLs where it isnt allowed in the target table into  ETL Exception table
            cur.execute("insert into ETL_Exceptions (SELECT 'TMP_PLAN', 'TMP_MAPPING', sysdate(), sysdate(), %(pk)s, 'NULL IN NON-NULLABLE FIELD' from "+stage+ " where " + i[0]+" IS NULL)",{'pk': pk, })
            cur.execute("DELETE FROM " +stage+ "  where %(pk)s in  (select %(pk)s from "+stage+" where " + i[0]+" IS NULL)",{'pk': pk, })

    #Duplicate records check
    cur.execute("select %(pks)s from " +stage+ " group by " +pks +" having count(*) > 1",{'pks': pks, })
    try:        
        query_id = cur.sfqid
        while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
            time.sleep(1)
    except ProgrammingError as err:
        print('Programming Error: {0}'.format(err))
    results = cur.fetchall()

    #exits if no dups
    if results == None:
        return stage
    else:
        #insert dups into exception table
        cur.execute("insert into ETL_Exceptions (SELECT 'TMP_PLAN', 'TMP_MAPPING', sysdate(), sysdate(), %(pk)s, 'DUPLICATE' from "+stage+ " group by %(pks)s having count(*) > 1)",{'pk': pk,'pks': pks })
        #remove dups from temp stable
        cur.execute("DELETE FROM " +stage+ "  where %(pk)s in  (select %(pk)s from "+stage+" group by %(pks)s having count(*) > 1)",{'pk': pk, 'pks': pks }) 
    return stage   
