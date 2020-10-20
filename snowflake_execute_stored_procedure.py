#!/usr/bin/env python
import sys
import snowflake.connector
import argparse as ap
import json

def executeQuery(query):
    with open('Snowflake/config.json') as f:
        data = json.load(f)

        with snowflake.connector.connect(user = data['username'], password = data['password'], account = data['account']) as con:
            curr        =   con.cursor()
            curr.execute(query)
            return curr..fetchall()


if __name__ == '__main__':
    parser  =   ap.ArgumentParser();
    parser.add_argument('business_name',                help='Name of Business')
    parser.add_argument('stored_procedure_name',        help='Database and Name of stored procedure to be executed')
    parser.add_argument('stored_procedure_parameters',  nargs='*')

    args = parser.parse_args();

    get_open_batch_command      =   'SELECT BATCH_ID FROM DWH_META.META_BATCH_DETAILS WHERE APPLICATION_NAME = "' + args.business_name + '" AND BATCH_STATUS = "OPEN"'
    open_batch_id               =   executeQuery(get_open_batch_command)[0][0]

    get_warehouse_name_command  =   'SELECT WAREHOUSE_NAME FROM DWH_META.BUSINESS WHERE BUSINESS_NAME = "' + args.business_name + '"
    warehouse_name              =   executeQuery(get_warehouse_name_command)[0][0]

    set_warehouse_command       =   'USE WAREHOUSE ' + warehouse_name
    executeQuery(set_warehouse_command)

    exec_stored_proc_command    =   'CALL ' + args.stored_procedure_name + '(\'' + open_batch_id + '\',' + '\', \''.join(args.stored_procedure_parameters) + '\')'
    stored_proc_result          =   executeQuery(exec_stored_proc_command)[0][0]

    sys.exit(stored_proc_result)
