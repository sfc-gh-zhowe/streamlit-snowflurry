#!/usr/bin/env python
WAREHOUSE_SIZES = ('XSMALL', 'SMALL', 'MEDIUM', 'LARGE', 'XLARGE', 'XXLARGE', 'XXXLARGE', 'X4LARGE', 'X5LARGE', 'X6LARGE')
MCW_SIZE_RANGE = (1, 10)
from logging import PlaceHolder
from xml.etree.ElementPath import prepare_descendant
import streamlit as st
import datetime
import time
import json
import os
import snowflake.connector
from snowflake.connector import ProgrammingError
from math import ceil

st.set_page_config(layout="wide")

resultQuery = """
WITH qh AS (SELECT CLUSTER_NUMBER, TOTAL_ELAPSED_TIME / 1000 AS ELAPSED_SECONDS, ROWS_PRODUCED AS QRY_ROWS, BYTES_SCANNED / POW(2,20) MB_SCANNED
 , COMPILATION_TIME / 1000 AS COMPILATION_SECONDS, EXECUTION_TIME / 1000 AS EXECUTION_SECONDS, TRANSACTION_BLOCKED_TIME / 1000 AS BLOCKED_SECONDS, (QUEUED_PROVISIONING_TIME+QUEUED_REPAIR_TIME+QUEUED_OVERLOAD_TIME) / 1000 AS QUEUED_SECONDS
 FROM table(information_schema.query_history_by_session(RESULT_LIMIT => 10000)) 
 WHERE QUERY_TYPE = 'SELECT' AND QUERY_TAG = '{queryTag}')
SELECT '-ALL-' AS CLUSTER_NUMBER, COUNT(*) AS QRY_COUNT
, MIN(QRY_ROWS) AS MIN_ROWS, MAX(QRY_ROWS) AS MAX_ROWS, SUM(QRY_ROWS) AS SUM_ROWS, ROUND(AVG(QRY_ROWS), 0) AS AVG_ROWS
, MIN(MB_SCANNED) AS MIN_MB_SCANNED, MAX(MB_SCANNED) AS MAX_MB_SCANNED, SUM(MB_SCANNED) AS SUM_MB_SCANNED, ROUND(AVG(MB_SCANNED), 1) AS AVG_MB_SCANNED
, MIN(COMPILATION_SECONDS) AS MIN_COMPILATION, MAX(COMPILATION_SECONDS) AS MAX_COMPILATION, SUM(COMPILATION_SECONDS) AS SUM_COMPILATION, ROUND(AVG(COMPILATION_SECONDS), 3) AS AVG_COMPILATION
, MIN(BLOCKED_SECONDS) AS MIN_BLOCKED, MAX(BLOCKED_SECONDS) AS MAX_BLOCKED, SUM(BLOCKED_SECONDS) AS SUM_BLOCKED, ROUND(AVG(BLOCKED_SECONDS), 3) AS AVG_BLOCKED
, MIN(QUEUED_SECONDS) AS MIN_QUEUED, MAX(QUEUED_SECONDS) AS MAX_QUEUED, SUM(QUEUED_SECONDS) AS SUM_QUEUED, ROUND(AVG(QUEUED_SECONDS), 3) AS AVG_QUEUED
, MIN(EXECUTION_SECONDS) AS MIN_EXECUTION, MAX(EXECUTION_SECONDS) AS MAX_EXECUTION, SUM(EXECUTION_SECONDS) AS SUM_EXECUTION, ROUND(AVG(EXECUTION_SECONDS), 3) AS AVG_EXECUTION
, MIN(ELAPSED_SECONDS) AS MIN_ELAPSED, MAX(ELAPSED_SECONDS) AS MAX_ELAPSED, SUM(ELAPSED_SECONDS) AS SUM_ELAPSED, ROUND(AVG(ELAPSED_SECONDS), 3) AS AVG_ELAPSED
FROM qh
UNION ALL
SELECT CLUSTER_NUMBER::string, COUNT(*) AS QRY_COUNT
, MIN(QRY_ROWS) AS MIN_ROWS, MAX(QRY_ROWS) AS MAX_ROWS, SUM(QRY_ROWS) AS SUM_ROWS, ROUND(AVG(QRY_ROWS), 0) AS AVG_ROWS
, MIN(MB_SCANNED) AS MIN_MB_SCANNED, MAX(MB_SCANNED) AS MAX_MB_SCANNED, SUM(MB_SCANNED) AS SUM_MB_SCANNED, ROUND(AVG(MB_SCANNED), 1) AS AVG_MB_SCANNED
, MIN(COMPILATION_SECONDS) AS MIN_COMPILATION, MAX(COMPILATION_SECONDS) AS MAX_COMPILATION, SUM(COMPILATION_SECONDS) AS SUM_COMPILATION, ROUND(AVG(COMPILATION_SECONDS), 3) AS AVG_COMPILATION
, MIN(BLOCKED_SECONDS) AS MIN_BLOCKED, MAX(BLOCKED_SECONDS) AS MAX_BLOCKED, SUM(BLOCKED_SECONDS) AS SUM_BLOCKED, ROUND(AVG(BLOCKED_SECONDS), 3) AS AVG_BLOCKED
, MIN(QUEUED_SECONDS) AS MIN_QUEUED, MAX(QUEUED_SECONDS) AS MAX_QUEUED, SUM(QUEUED_SECONDS) AS SUM_QUEUED, ROUND(AVG(QUEUED_SECONDS), 3) AS AVG_QUEUED
, MIN(EXECUTION_SECONDS) AS MIN_EXECUTION, MAX(EXECUTION_SECONDS) AS MAX_EXECUTION, SUM(EXECUTION_SECONDS) AS SUM_EXECUTION, ROUND(AVG(EXECUTION_SECONDS), 3) AS AVG_EXECUTION
, MIN(ELAPSED_SECONDS) AS MIN_ELAPSED, MAX(ELAPSED_SECONDS) AS MAX_ELAPSED, SUM(ELAPSED_SECONDS) AS SUM_ELAPSED, ROUND(AVG(ELAPSED_SECONDS), 3) AS AVG_ELAPSED
FROM qh GROUP BY CLUSTER_NUMBER ORDER BY CLUSTER_NUMBER
"""

def ConfigLogging():
 import logging
 logging.basicConfig(filename='/tmp/snowflake_python_connector.log', level=logging.INFO)

CONFIG_FILE = os.environ.get('SNOWFLURRY_CONFIG_FILE')
def ImportConfig():
 INITIALIZED_KEY = 'INITIALIZED'
 if INITIALIZED_KEY not in st.session_state:
  st.session_state[INITIALIZED_KEY] = True
  with open(CONFIG_FILE) as jsonFile:    
   data = json.load(jsonFile)
   for key in data:
    st.session_state[key]=data[key] #element by elemnet copying
   st.session_state['Mcw'] = (int(st.session_state['McwMin']), int(st.session_state['McwMax']))
   st.session_state['Iterations'] = int(st.session_state['Iterations'])
   st.session_state['ResultSetCache'] = bool(st.session_state['ResultSetCache'])

def ExportConfig():
 args = st.session_state
 with open(CONFIG_FILE) as jsonFile:
  data = {
  'Account': args['Account'],
  'User': args['User'],
  'Password': args['Password'],
  'Database': args['Database'],
  'Warehouse': args['Warehouse'],
  'Schema':  args['Schema'],
  'Role': args['Role'],

  'WarehouseSize': args['WarehouseSize'],
  'McwMin': min(args['Mcw']),
  'McwMax': max(args['Mcv']),
  'ResultSetCache': args['ResultSetCache'],
  'QueryTag': args['QueryTag'],
  'Iterations': args['Iterations'],
  'SqlFile': args['SqlFile']
  }
  json.dump(data, jsonFile)

def ReadSqlFile(fileName):
 with open('/scripts/'+fileName, 'r') as sqlFile:
    data = sqlFile.read()
 return [i.strip() for i in data.split(';') if i.strip()]

def ClearDataFrame():
 if ('df' in st.session_state):
  del st.session_state['df']

def TestConnection():
 try:
  ClearDataFrame()
  con = ConnectSnowflake()
 except Exception as err:
  st.write(f'Exception: {err}')
 else:
  st.write('Connection Succeeded')

def ExecuteMain():
 ClearDataFrame()
 args = st.session_state
 warehouse = args['Warehouse']
 try:
  sqlList = ReadSqlFile(args['SqlFile'])
  iterations = args['Iterations']
  mult = ceil(iterations/len(sqlList))
  sqlList = (sqlList * mult)[0:iterations]

  con = ConnectSnowflake()
  queryTag = ConfigureWarehouse(con, warehouse)
  startTime = time.time()
  ExecuteQueries(con, sqlList)
  endTime = time.time()
  ResetWarehouse(con, warehouse)
  CollectResults(con, queryTag, endTime-startTime)
  SuspendWarehouse(con, warehouse)
  con.close()
 except Exception as err:
  st.session_state['ResultContainer'].write(err)
  ResetWarehouse(con, warehouse)
  SuspendWarehouse(con, warehouse)

def ConnectSnowflake():
 args = st.session_state
 return snowflake.connector.connect(user=args['User'], password=args['Password'], account=args['Account'], role=args['Role'], warehouse=args['Warehouse'], database=args['Database'], schema=args['Schema'], session_parameters={'USE_CACHED_RESULT': args['ResultSetCache']})
   
def ConfigureWarehouse(con, warehouse):
 try:
  args = st.session_state
  cur = con.cursor()
  cur.execute('ALTER SESSION UNSET QUERY_TAG')
  SuspendWarehouse(con, warehouse)
  cur.execute(f'ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = {args["WarehouseSize"]} MIN_CLUSTER_COUNT = {min(args["Mcw"])} MAX_CLUSTER_COUNT = {max(args["Mcw"])}')
  cur.execute(f'ALTER WAREHOUSE {warehouse} RESUME')
  timeString = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
  queryTag = f'{timeString}_{args["Iterations"]}_{args["WarehouseSize"]}_{min(args["Mcw"])}_{max(args["Mcw"])}_{"RC_" if args["ResultSetCache"] else ""}{args["QueryTag"]}'
  cur.execute(f'ALTER SESSION SET QUERY_TAG = "{queryTag}"')
  return queryTag
 except Exception as err:
  st.session_state['ResultContainer'].write(err)
  raise

def ResetWarehouse(con, warehouse):
  cur = con.cursor()
  cur.execute('ALTER SESSION UNSET QUERY_TAG')
  cur.execute(f'ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = XSMALL MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1')

def SuspendWarehouse(con, warehouse):
 try:
  cur = con.cursor()
  cur.execute(f'ALTER WAREHOUSE {warehouse} SUSPEND')
 except ProgrammingError as err:
  if "Invalid state." not in str(err):
    raise

def ExecuteQueries(con, sqlList):
 queryIdList = []
 cur = con.cursor()

 progressBar = st.progress(0)
 for sql in sqlList:
  cur.execute_async(sql)
  queryIdList.append(cur.sfqid)
 
 runningList = queryIdList.copy()
 while len(runningList) > 0:
  time.sleep(1)
  runningList = [id for id in queryIdList if con.is_still_running(con.get_query_status(id))]
  percentComplete = (len(queryIdList)-len(runningList))/len(queryIdList)
  progressBar.progress(percentComplete)

def CollectResults(con, queryTag, elapsedTime):
 try:
  cur = con.cursor()
  cur.execute(resultQuery.format(queryTag=queryTag))
  df = cur.fetch_pandas_all()
  tagIndex = queryTag.index('_', queryTag.index('_')+1)
  queryTag = queryTag[:tagIndex] + '\r\n' + queryTag[tagIndex + 1:]
  tag = f"{queryTag}\r\nElapsed: {elapsedTime:9.3f}"
  idx = df.index[df['CLUSTER_NUMBER'] == '-ALL-'].tolist()
  df.at[idx[0], 'CLUSTER_NUMBER'] = tag
  df = df.astype({
    'MIN_COMPILATION': 'float', 'MAX_COMPILATION': 'float', 'SUM_COMPILATION': 'float', 'AVG_COMPILATION': 'float',
    'MIN_BLOCKED': 'float', 'MAX_BLOCKED': 'float', 'SUM_BLOCKED': 'float', 'AVG_BLOCKED': 'float',
    'MIN_QUEUED': 'float', 'MAX_QUEUED': 'float', 'SUM_QUEUED': 'float', 'AVG_QUEUED': 'float',
    'MIN_EXECUTION': 'float', 'MAX_EXECUTION': 'float', 'SUM_EXECUTION': 'float', 'AVG_EXECUTION': 'float',
    'MIN_ELAPSED': 'float', 'MAX_ELAPSED': 'float', 'SUM_ELAPSED': 'float', 'AVG_ELAPSED': 'float',
    })
  st.session_state['df'] = df
 finally:
  cur.close()

def DownloadResults():
 df = st.session_state['df']
 df.to_clipboard()

def main():
 ConfigLogging()
 ImportConfig()
 
 st.header('Welcome to Snowflurry!')
 col1, col2 = st.columns(2)
 with col1.expander("Log-in", True):
  st.text_input('Account', key='Account', help="Snowflake Account Identifier", placeholder='snowflake_account')
  st.text_input('User', key='User', help='Username', placeholder='User')
  st.text_input('Password', key='Password', type='password', help='Password', placeholder='Password')
  st.text_input('Database', key='Database', help='Default Database Name.  A database that the User has USAGE on.  Needed to access information_schema functions', placeholder="Database'")
  st.text_input('Schema', key='Schema', help='Default Schema', placeholder='public')
  st.text_input('Role', key='Role', help='Default Role.  Must have access to run all queries to be tested, as well as resize/start/suspend the Warehouse', placeholder='Role')
  if st.button('Test Connection'):
   TestConnection()

 with col2.expander("Warehouse", True):
  st.text_input('Warehouse Name', key='Warehouse', help='The Warehouse that will be used for testing.  Should not be used by anyone else, as we will be sizing/starting/stopping it!', placeholder='SNOWFLURRY_WH')
  st.selectbox('Warehouse Size', WAREHOUSE_SIZES, help='Warehouse size for a given test')
  st.slider('MCW Size', min(MCW_SIZE_RANGE), max(MCW_SIZE_RANGE), key='Mcw', help='Set Min and Max Multicluster Warehouse sizes')
  st.checkbox('ResultSet Cache', key='ResultSetCache', help='If checked, Snowflake ResultSet Caching will be enabled')
 with col2.expander("Query", True):
  st.text_input('Query Tag', key='QueryTag', help='QueryTag base attached to all test queries', placeholder='Snowflurry')
  st.number_input('Iterations', min_value=0, key='Iterations', help='Total iterations to run, to simulate concurrency')
  st.text_input('Sql File', key='SqlFile', help='File containing SQL statements to be eecuted', placeholder='snowflurry.sql')
  executeMain = st.button('Execute')
    
 if executeMain:
  st.snow()
  with st.spinner():
   ExecuteMain()

 if ('df' in st.session_state):
  st.write(st.session_state['df'])
  st.button('Download', on_click=DownloadResults)

main()
