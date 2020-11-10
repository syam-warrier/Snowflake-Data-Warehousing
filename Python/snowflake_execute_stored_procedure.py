#!/usr/bin/env python
import sys
import snowflake.connector
import argparse as ap
import json
import logging

log = logging.getLogger('logger')
log.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
log.addHandler(ch)

class Snowflake:

    def __init__(self):
        # Reading snowflake credentials from password-protected config file
        log.info('Reading credentials')
        with open('Snowflake/config.json') as f:
            data = json.load(f)
            self.username             =    data['username']
            self.password             =    data['password']
            self.account              =    data['account']
            self.database             =    data['database']
            
            # Connecting to snowflake using the extracted snowflake credentials
            log.info('Connecting to snowflake account with following credentials:')
            log.info('Username : ' + self.username)
            log.info('Password : ' + self.password)
            log.info('Account : ' + self.account)
            log.info('Database : ' + self.database)
            self.con                  =    snowflake.connector.connect(user = self.username, password = self.password, account = self.account, database = self.database)
            log.info('Snowflake connection established successfully.')
    
    def executeQuery(self, query):
        try:
            # Execute the given query using Snowflake connection
            self.cursor               =   self.con.cursor()
            log.info('Executing query : ' + query)
            self.cursor.execute(query)            
            return self.cursor.fetchall()
        except Exception as e:
            raise Exception(str(type(e)) + " " + str(e))

if __name__ == '__main__':

    # Parse command line arguments
    parser  =   ap.ArgumentParser();
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-a', '--app-name', help='Name of Application (Business)')
    group.add_argument('-b', '--batch-id', help='Open batch ID')
    parser.add_argument('stored_procedure_name',        help='Database and Name of stored procedure to be executed')
    parser.add_argument('stored_procedure_parameters',  nargs='*')
    args = parser.parse_args();
    
    # Create an Snowflake object for querying
    sf                                =    Snowflake()

    # Check if Batch ID is provided; if not, then get it from metadata batch table using Application Name
    if args.batch_id is None:
        log.info('Fetching open batch information...')
        get_open_batch_command        =   'SELECT A.BATCH_ID FROM DWH_METADATA.META_BATCH_DETAILS A WHERE A.APPLICATION_NAME = \'' + args.app_name + '\' AND UPPER(A.BATCH_STATUS) = \'OPEN\' AND NOT EXISTS (SELECT 1 FROM DWH_METADATA.META_BATCH_DETAILS B WHERE B.APPLICATION_NAME = \'' + args.app_name + '\' AND UPPER(B.BATCH_STATUS) IN (\'SUCCESS\',\'FAILURE\') AND A.BATCH_ID = B.BATCH_ID)'
        open_batch_id                 =   sf.executeQuery(get_open_batch_command)
        if len(open_batch_id) != 0:
            open_batch_id             =    str(open_batch_id[0][0])
        else:
            log.info('No batch information provided and no batch is open for application ' + args.app_name + '.')
            open_batch_id             =    ""
    else:
        open_batch_id                 =    args.batch_id
    log.info("Batch ID : " + open_batch_id)
    
    # If batch is open, tag the subsequent commands under that Batch ID
    if open_batch_id != "":
        log.info('Setting session parameter QUERY_TAG to batch id (' + open_batch_id + ').')
        set_query_tag_command         =   'ALTER SESSION SET QUERY_TAG = \'' + open_batch_id + '\''
        sf.executeQuery(set_query_tag_command)
        log.info('Session parameter QUERY_TAG set to batch id (' + open_batch_id + ').')
    
    # Check if Application Name is provided; if not, then get it from metadata batch table using Batch ID
    if args.app_name is None:
        log.info('Fetching application information...')
        get_app_name_command          =   'SELECT APPLICATION_NAME FROM DWH_METADATA.META_BATCH_DETAILS A WHERE A.BATCH_ID = ' + args.batch_id + ' AND UPPER(A.BATCH_STATUS) = \'OPEN\' AND NOT EXISTS (SELECT 1 FROM DWH_METADATA.META_BATCH_DETAILS B WHERE B.BATCH_ID = ' + args.batch_id + ' AND UPPER(B.BATCH_STATUS) IN (\'SUCCESS\',\'FAILURE\') AND A.APPLICATION_NAME = B.APPLICATION_NAME)'
        app_name                      =   sf.executeQuery(get_app_name_command)
        if len(app_name) != 0:
            app_name                  =    str(app_name[0][0])
        else:
            raise Exception('No application name provided and either batch is not found in metadata table or batch ' + open_batch_id + ' is not open.')
    else:
        app_name                      =    args.app_name
    log.info("Application Name : " + app_name)
    
    # Based on application name provided, find the appropriate warehouse from metadata table
    log.info('Fetching warehouse information for ' + app_name + '...')
    get_warehouse_name_command        =   'SELECT WAREHOUSE_NAME FROM DWH_METADATA.DWH_APPLICATIONS WHERE APPLICATION_NAME = \'' + app_name + '\''
    warehouse_name                    =   sf.executeQuery(get_warehouse_name_command)
    if len(warehouse_name) != 0:
        warehouse_name                =    str(warehouse_name[0][0])
    else:
        warehouse_name                =    ""
    log.info("Warehouse Name : " + warehouse_name)
    
    # Use the warehouse derived for executing this current session
    log.info('Using warehouse ' + warehouse_name + ' for current session.')
    set_warehouse_command             =   'USE WAREHOUSE ' + warehouse_name
    sf.executeQuery(set_warehouse_command)
    
    # Prepare the stored procedure parameters and command and run it
    stored_proc_parameters            =    ','.join(map(lambda x : '\'' + str(x) + '\'', filter(lambda x : x != "", [open_batch_id] + args.stored_procedure_parameters)))
    exec_stored_proc_command          =   'CALL ' + args.stored_procedure_name + '(' + stored_proc_parameters + ')'
    log.info("Calling stored procedure : " + exec_stored_proc_command)
    stored_proc_result                =   sf.executeQuery(exec_stored_proc_command)[0][0]
    
    sys.exit(stored_proc_result)