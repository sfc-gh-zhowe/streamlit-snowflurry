#!/usr/bin/env python
import os
import datetime
import time
import json
import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector import ProgrammingError
from math import ceil

DEBUG = True

WAREHOUSE_SIZES = ('XSMALL', 'SMALL', 'MEDIUM', 'LARGE', 'XLARGE', 'XXLARGE', 'XXXLARGE', 'X4LARGE', 'X5LARGE', 'X6LARGE')
MCW_SIZE_RANGE = (1, 10)

CONFIG_FILE = os.environ.get('SNOWFLURRY_CONFIG_FILE') if 'SNOWFLURRY_CONFIG_FILE' in os.environ else './secrets/snowflurry.json'
IN_DOCKER = 'IN_DOCKER' in os.environ
EXCEL_PATH = '/excel/' if IN_DOCKER else './excel/'

SUMMARY_QUERY_SQL = """
WITH qh AS (SELECT CLUSTER_NUMBER, TOTAL_ELAPSED_TIME / 1000 AS ELAPSED_SECONDS, ROWS_PRODUCED AS QRY_ROWS, BYTES_SCANNED / POW(2,20) MB_SCANNED
 , COMPILATION_TIME / 1000 AS COMPILATION_SECONDS, EXECUTION_TIME / 1000 AS EXECUTION_SECONDS, TRANSACTION_BLOCKED_TIME / 1000 AS BLOCKED_SECONDS
 , (QUEUED_PROVISIONING_TIME+QUEUED_REPAIR_TIME+QUEUED_OVERLOAD_TIME) / 1000 AS QUEUED_SECONDS
 , PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY EXECUTION_SECONDS) OVER (PARTITION BY CLUSTER_NUMBER) AS P95_EXECUTION
 , PERCENTILE_CONT(0.95) WITHIN GROUP(ORDER BY ELAPSED_SECONDS) OVER (PARTITION BY CLUSTER_NUMBER) AS P95_ELAPSED
 FROM table(information_schema.query_history_by_user(USER_NAME => '{userName}', RESULT_LIMIT => 10000)) 
 WHERE QUERY_TYPE = 'SELECT' AND QUERY_TAG = '{queryTag}')
SELECT '-ALL-' AS CLUSTER_NUMBER, COUNT(*) AS QRY_COUNT
, MIN(QRY_ROWS) AS MIN_ROWS, MAX(QRY_ROWS) AS MAX_ROWS, SUM(QRY_ROWS) AS SUM_ROWS, ROUND(AVG(QRY_ROWS), 0) AS AVG_ROWS
, MIN(MB_SCANNED) AS MIN_MB_SCANNED, MAX(MB_SCANNED) AS MAX_MB_SCANNED, SUM(MB_SCANNED) AS SUM_MB_SCANNED, ROUND(AVG(MB_SCANNED), 1) AS AVG_MB_SCANNED
, MIN(COMPILATION_SECONDS) AS MIN_COMPILATION, MAX(COMPILATION_SECONDS) AS MAX_COMPILATION, SUM(COMPILATION_SECONDS) AS SUM_COMPILATION, ROUND(AVG(COMPILATION_SECONDS), 3) AS AVG_COMPILATION
, MIN(BLOCKED_SECONDS) AS MIN_BLOCKED, MAX(BLOCKED_SECONDS) AS MAX_BLOCKED, SUM(BLOCKED_SECONDS) AS SUM_BLOCKED, ROUND(AVG(BLOCKED_SECONDS), 3) AS AVG_BLOCKED
, MIN(QUEUED_SECONDS) AS MIN_QUEUED, MAX(QUEUED_SECONDS) AS MAX_QUEUED, SUM(QUEUED_SECONDS) AS SUM_QUEUED, ROUND(AVG(QUEUED_SECONDS), 3) AS AVG_QUEUED
, MIN(EXECUTION_SECONDS) AS MIN_EXECUTION, MAX(EXECUTION_SECONDS) AS MAX_EXECUTION, SUM(EXECUTION_SECONDS) AS SUM_EXECUTION, ROUND(AVG(EXECUTION_SECONDS), 3) AS AVG_EXECUTION, ROUND(AVG(P95_EXECUTION), 3) AS P95_EXECUTION
, MIN(ELAPSED_SECONDS) AS MIN_ELAPSED, MAX(ELAPSED_SECONDS) AS MAX_ELAPSED, SUM(ELAPSED_SECONDS) AS SUM_ELAPSED, ROUND(AVG(ELAPSED_SECONDS), 3) AS AVG_ELAPSED, ROUND(AVG(P95_ELAPSED), 3) AS P95_ELAPSED
FROM qh
UNION ALL
SELECT CLUSTER_NUMBER::string, COUNT(*) AS QRY_COUNT
, MIN(QRY_ROWS) AS MIN_ROWS, MAX(QRY_ROWS) AS MAX_ROWS, SUM(QRY_ROWS) AS SUM_ROWS, ROUND(AVG(QRY_ROWS), 0) AS AVG_ROWS
, MIN(MB_SCANNED) AS MIN_MB_SCANNED, MAX(MB_SCANNED) AS MAX_MB_SCANNED, SUM(MB_SCANNED) AS SUM_MB_SCANNED, ROUND(AVG(MB_SCANNED), 1) AS AVG_MB_SCANNED
, MIN(COMPILATION_SECONDS) AS MIN_COMPILATION, MAX(COMPILATION_SECONDS) AS MAX_COMPILATION, SUM(COMPILATION_SECONDS) AS SUM_COMPILATION, ROUND(AVG(COMPILATION_SECONDS), 3) AS AVG_COMPILATION
, MIN(BLOCKED_SECONDS) AS MIN_BLOCKED, MAX(BLOCKED_SECONDS) AS MAX_BLOCKED, SUM(BLOCKED_SECONDS) AS SUM_BLOCKED, ROUND(AVG(BLOCKED_SECONDS), 3) AS AVG_BLOCKED
, MIN(QUEUED_SECONDS) AS MIN_QUEUED, MAX(QUEUED_SECONDS) AS MAX_QUEUED, SUM(QUEUED_SECONDS) AS SUM_QUEUED, ROUND(AVG(QUEUED_SECONDS), 3) AS AVG_QUEUED
, MIN(EXECUTION_SECONDS) AS MIN_EXECUTION, MAX(EXECUTION_SECONDS) AS MAX_EXECUTION, SUM(EXECUTION_SECONDS) AS SUM_EXECUTION, ROUND(AVG(EXECUTION_SECONDS), 3) AS AVG_EXECUTION, ROUND(AVG(P95_EXECUTION), 3) AS P95_EXECUTION
, MIN(ELAPSED_SECONDS) AS MIN_ELAPSED, MAX(ELAPSED_SECONDS) AS MAX_ELAPSED, SUM(ELAPSED_SECONDS) AS SUM_ELAPSED, ROUND(AVG(ELAPSED_SECONDS), 3) AS AVG_ELAPSED, ROUND(AVG(P95_ELAPSED), 3) AS P95_ELAPSED
FROM qh GROUP BY CLUSTER_NUMBER ORDER BY CLUSTER_NUMBER
"""
DETAIL_QUERY_SQL = """
SELECT *
FROM table(information_schema.query_history_by_user(USER_NAME => '{userName}', RESULT_LIMIT => 10000)) 
WHERE QUERY_TYPE = 'SELECT' AND QUERY_TAG = '{queryTag}'
"""

st.set_page_config(layout="wide")

def ConfigLogging():
 import logging
 logging.basicConfig(filename='/tmp/snowflake_python_connector.log', level=logging.INFO)

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
   st.session_state['DetailedResults'] = False
   del st.session_state['McwMin']
   del st.session_state['McwMax']

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
 with open('./scripts/'+fileName, 'r', encoding='UTF8') as sqlFile:
    data = sqlFile.read()

 import re
 sqls = re.split(r';$', data, flags = re.MULTILINE)
 sqls = [i.strip() for i in sqls if i.strip()]
 print('sqls: ' + str(len(sqls)))
 return sqls

def ClearDataFrame():
 if ('exception' in st.session_state):
  del st.session_state['exception']
 if ('dfSummary' in st.session_state):
  del st.session_state['dfSummary']
 if ('dfDetail' in st.session_state):
  del st.session_state['dfDetail']

def TestConnection():
 try:
  ClearDataFrame()
  con = ConnectSnowflake()
 except Exception as err:
  st.session_state['connection'] = f'Exception: {err}'
 else:
  st.session_state['connection'] = 'Connection Succeeded'

def ExecuteMain(con):
 if (DEBUG):
  print('ExecuteMain')
 ClearDataFrame()
 args = st.session_state
 warehouse = args['Warehouse']
 try:
  sqlList = ReadSqlFile(args['SqlFile'])
  multiplier = len(sqlList) if args['IterateBy'] == 'File' else 1
  iterations = args['Iterations'] * multiplier
  mult = ceil(iterations/len(sqlList))
  sqlList = (sqlList * mult)[0:iterations]

  if (DEBUG):
   print('sqlList set to ' + str(len(sqlList)) + ' queries')
  queryTag = ConfigureWarehouse(con, warehouse)
  startTime = time.time()
  ExecuteQueries(con, sqlList)
  endTime = time.time()
  ResetWarehouse(con, warehouse)
  CollectResults(con, queryTag, endTime-startTime, args['DetailedResults'])
  SuspendWarehouse(con, warehouse)
 except Exception as err:
  OutputException(err)
  if (con):
   ResetWarehouse(con, warehouse)
   SuspendWarehouse(con, warehouse)

def ConnectSnowflake():
 args = st.session_state
 if (DEBUG):
  print(args)
 con =  snowflake.connector.connect(user=args['User'], password=args['Password'], account=args['Account'], role=args['Role'], 
                                    warehouse=args['Warehouse'], database=args['Database'], schema=args['Schema'], 
                                    authenticator=args['Authenticator'], session_parameters={'USE_CACHED_RESULT': args['ResultSetCache']})
 return con
   
def ConfigureWarehouse(con, warehouse):
 try:
  args = st.session_state
  cur = con.cursor()
  cur.execute('ALTER SESSION UNSET QUERY_TAG')
  SuspendWarehouse(con, warehouse)
  if (DEBUG):
   print(args)
  alterStatement = f'ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = {args["WarehouseSize"]} MIN_CLUSTER_COUNT = {min(args["Mcw"])} MAX_CLUSTER_COUNT = {max(args["Mcw"])} MAX_CONCURRENCY_LEVEL = {args["MaxConcurrencyLevel"]}'
  if (DEBUG):
   print(alterStatement)
  cur.execute(alterStatement)
  timeString = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  queryTag = f'{timeString}_{args["Iterations"]}_{args["WarehouseSize"]}_{min(args["Mcw"])}_{max(args["Mcw"])}_{args["MaxConcurrencyLevel"]}_{"RC_" if args["ResultSetCache"] else ""}{args["QueryTag"]}'
  cur.execute(f'ALTER SESSION SET QUERY_TAG = "{queryTag}"')

  if (args['SecondaryRoles']):
   cur.execute('USE SECONDARY ROLES ALL')
  cur.execute(f'ALTER WAREHOUSE {warehouse} RESUME')
  return queryTag
 except Exception as err:
  OutputException(err)
  raise

def ResetWarehouse(con, warehouse):
  cur = con.cursor()
  cur.execute('ALTER SESSION UNSET QUERY_TAG')
  cur.execute(f'ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = XSMALL MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 MAX_CONCURRENCY_LEVEL = 8')

def SuspendWarehouse(con, warehouse):
 try:
  cur = con.cursor()
  cur.execute(f'ALTER WAREHOUSE {warehouse} SUSPEND')
 except ProgrammingError as err:
  if "Invalid state." not in str(err):
    raise

def ExecuteQueries(con, sqlList):
 if (DEBUG):
  print('ExecuteQueries')
 queryIdList = []
 cur = con.cursor()

 progressBar = st.progress(0)
 if (DEBUG):
  print('sqlList: ' + str(sqlList))
 for sql in sqlList:
  if (DEBUG):
    print(sql)
  cur.execute_async(sql)
  queryIdList.append(cur.sfqid)
 
 runningList = queryIdList.copy()
 while len(runningList) > 0:
#  time.sleep(1)
  print('Running: ' + str(len(runningList)))
  print(runningList)
  runningList = [id for id in queryIdList if con.is_still_running(con.get_query_status(id))]
  percentComplete = (len(queryIdList)-len(runningList))/len(queryIdList)
  progressBar.progress(percentComplete)

def CollectResults(con, queryTag, elapsedTime, detailedResults=False):
 try:
  cur = con.cursor()
  args = st.session_state
  cur.execute(SUMMARY_QUERY_SQL.format(userName=args['User'], queryTag=queryTag))
  dfSummary = cur.fetch_pandas_all()
  tagIndex = queryTag.index('_', queryTag.index('_')+1)
  tag = f"{queryTag[:tagIndex]} \r\n{queryTag[tagIndex + 1:]}\r\nElapsed: {elapsedTime:9.3f}"
  print(tag)
  idx = dfSummary.index[dfSummary['CLUSTER_NUMBER'] == '-ALL-'].tolist()
  dfSummary.at[idx[0], 'CLUSTER_NUMBER'] = tag
  dfSummary = dfSummary.astype({
    'MIN_COMPILATION': 'float', 'MAX_COMPILATION': 'float', 'SUM_COMPILATION': 'float', 'AVG_COMPILATION': 'float',
    'MIN_BLOCKED': 'float', 'MAX_BLOCKED': 'float', 'SUM_BLOCKED': 'float', 'AVG_BLOCKED': 'float',
    'MIN_QUEUED': 'float', 'MAX_QUEUED': 'float', 'SUM_QUEUED': 'float', 'AVG_QUEUED': 'float',
    'MIN_EXECUTION': 'float', 'MAX_EXECUTION': 'float', 'SUM_EXECUTION': 'float', 'AVG_EXECUTION': 'float', 'P95_EXECUTION': 'float',
    'MIN_ELAPSED': 'float', 'MAX_ELAPSED': 'float', 'SUM_ELAPSED': 'float', 'AVG_ELAPSED': 'float', 'P95_ELAPSED': 'float',
    })
  st.session_state['dfSummary'] = dfSummary
  st.session_state['fileName'] = queryTag + '.xlsx'

  if (detailedResults):
   cur.execute(DETAIL_QUERY_SQL.format(userName=args['User'], queryTag=queryTag))
   dfDetail = cur.fetch_pandas_all()
   st.session_state['dfDetail'] = dfDetail

 except Exception as err:
  OutputException(err)
 finally:
  cur.close()

def OutputException(exception):
 print("EXCEPTION OCCURED")
 print(exception)
 st.session_state['exception'] = exception

def ClipboardResults(**kwargs):
 df = st.session_state[kwargs['dataframe']]
 df.to_clipboard()

def ExcelResults():
 dfSummary = st.session_state['dfSummary']
 hasDetail = 'dfDetail' in st.session_state
 dfDetail = st.session_state['dfDetail'] if hasDetail else None

 with pd.ExcelWriter(EXCEL_PATH + st.session_state['fileName']) as writer:
  dfSummary.to_excel(writer, sheet_name='Summary')
  if (hasDetail):
   date_columns = dfDetail.select_dtypes(include=['datetime64[ns, UTC]']).columns
   for date_column in date_columns:
    dfDetail[date_column] = dfDetail[date_column].dt.tz_localize(None)
   dfDetail.to_excel(writer, sheet_name='Detail')

def EnableInputs():
 st.session_state['inputsDisabled'] = False
 st.experimental_rerun()

def DisableInputs():
 st.session_state['inputsDisabled'] = True

def ValidateInputs(validateForExecute):
 if (not st.session_state['Database']):
  st.session_state['exception'] = 'A DatabaseName is required to access INFORMATION_SCHEMA'
  return False
  #check that role can alter the warehouse
 if (validateForExecute):
  pass
  #check that the warehouse is not running any jobs

 return True

def DisplayDataFrames():
 hasSummary = DisplayDataFrame('dfSummary')
 hasDetail = DisplayDataFrame('dfDetail')
 if (hasSummary or hasDetail):
  st.button('Save As Excel File', on_click=ExcelResults)

def DisplayDataFrame(dfName):
 hasDataFrame = dfName in st.session_state 
 if (hasDataFrame):
  st.write(st.session_state[dfName])

  if not IN_DOCKER:
   st.button('Copy to Clipboard', on_click=ClipboardResults, key='btn_'+dfName, kwargs={'dataframe': dfName})
  
 return hasDataFrame

def main():
 if 'inputsDisabled' not in st.session_state:
  EnableInputs()

 ConfigLogging()
 ImportConfig()
 
 inputsDisabled = st.session_state['inputsDisabled']
 if IN_DOCKER:
  st.header('Welcome to SnowFlurry in Docker!')
 else:
  st.header('Welcome to SnowFlurry!')

 col1, col2 = st.columns(2)
 with col1.expander("Log-in", True):
  st.selectbox('Authenticator', ('Snowflake', 'ExternalBrowser'), key='Authenticator', help='Use Snowflake User/Password or ExternalBrowser (SSO)', disabled=inputsDisabled)
  st.text_input('Account', key='Account', help="Snowflake Account Identifier", placeholder='snowflake_account', disabled=inputsDisabled)
  st.text_input('User', key='User', help='Username', placeholder='User', disabled=inputsDisabled)
  st.text_input('Password', key='Password', type='password', help='Password', placeholder='Password', disabled=inputsDisabled)
  st.text_input('Database', key='Database', help='Default Database Name.  A database that the User has USAGE on.  Needed to access information_schema functions', placeholder="Database", disabled=inputsDisabled)
  st.text_input('Schema', key='Schema', help='Default Schema', placeholder='public', disabled=inputsDisabled)
  st.text_input('Role', key='Role', help='Default Role.  Must have access to run all queries to be tested, as well as resize/start/suspend the Warehouse', placeholder='Role', disabled=inputsDisabled)
  st.checkbox('Use Scondary Roles', key='SecondaryRoles', help='If checked, Will USE SECONDARY ROLES ALL, otherwise uses default for user', disabled=inputsDisabled)
  testConnection = st.button('Test Connection', disabled=inputsDisabled, on_click=DisableInputs)
  if ('connection' in st.session_state):
   st.write(st.session_state['connection'])
   del st.session_state['connection']
 col1.checkbox('Detailed Results', key='DetailedResults', help='If checked, Query History Details will be returned', disabled=inputsDisabled)

 with col2.expander("Warehouse", True):
  st.text_input('Warehouse Name', key='Warehouse', help='The Warehouse that will be used for testing.  Should not be used by anyone else, as we will be sizing/starting/stopping it!', placeholder='SNOWFLURRY_WH', disabled=inputsDisabled)
  st.selectbox('Warehouse Size', WAREHOUSE_SIZES, key='WarehouseSize', help='Warehouse size for a given test', disabled=inputsDisabled)
  st.slider('MCW Size', min(MCW_SIZE_RANGE), max(MCW_SIZE_RANGE), key='Mcw', help='Set Min and Max Multicluster Warehouse sizes', disabled=inputsDisabled)
  st.number_input('Max Concurrency Level', key="MaxConcurrencyLevel", help="Max Concurrency Level Default for Warehouse", disabled=inputsDisabled, min_value=1, max_value=12)
  st.checkbox('ResultSet Cache', key='ResultSetCache', help='If checked, Snowflake ResultSet Caching will be enabled', disabled=inputsDisabled)
 with col2.expander("Query", True):
  st.text_input('Query Tag', key='QueryTag', help='QueryTag base attached to all test queries', placeholder='Snowflurry', disabled=inputsDisabled)
  st.selectbox('Iterate by', ('File', 'Query'), key='IterateBy', help='Run the file as a batch, or individual SQLs', disabled=inputsDisabled)
  st.number_input('Iterations', min_value=0, key='Iterations', help='Total iterations to run, to simulate concurrency', disabled=inputsDisabled)
  st.text_input('Sql File', key='SqlFile', help='File containing SQL statements to be executed, from the ./scripts PATH', placeholder='snowflurry.sql', disabled=inputsDisabled)
  executeMain = st.button('Execute', disabled=inputsDisabled, on_click=DisableInputs)

 if testConnection:
  if ValidateInputs(False):
   TestConnection()
  EnableInputs()

 if executeMain:
  if ValidateInputs(True):
    try:
     con = ConnectSnowflake()
     if (not con):
      print('NO CONNECTION')
     st.snow()
     with st.spinner():
      ExecuteMain(con)
      con.close()
    except Exception as err:
     OutputException(err)

  EnableInputs()

 if ('exception' in st.session_state):
  st.error(st.session_state['exception'])

 DisplayDataFrames()

main()
