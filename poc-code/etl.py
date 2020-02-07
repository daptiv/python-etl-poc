#!/usr/bin/env python3

from datetime import datetime
import os
import sys
import time

from sf_connector import SF_CONNECTOR

from snowflake.connector import DictCursor

# Pygrametl's __init__ file provides a set of helper functions and more
# importantly the class ConnectionWrapper for wrapping PEP 249 connections
import pygrametl

# Pygrametl provides simple reading of data through datasources.
from pygrametl.datasources import SQLSource, CSVSource

# Interacting with the dimensions and the fact table is done through a set
# of classes. A suitable object must be created for each.
from pygrametl.tables import Dimension, FactTable, CachedDimension, SnowflakedDimension,\
    SlowlyChangingDimension, BulkFactTable


def process():

    print("Running etl.process().....")

    connector = SF_CONNECTOR().get_connector()
    
    try:

        #TRANSFORM_TIMESTAMP = str(datetime.utcnow())

        connection = pygrametl.ConnectionWrapper(connector)
        #connection.setasdefault()
        #connection.execute('set search_path to pygrametlexa')

        # An instance of Dimension is created for each dimension in the data
        # warehouse. For each table, the name of the table, the primary key of
        # the table, and a list of non key attributes in the table, are added.
        # In addition, for the location dimension we specify which attributes
        # should be used for a lookup of the primary key, as only the city is
        # present in the sales database and is enough to perform a lookup of
        # a unique primary key. As mentioned in the beginning of the guide, using
        # named parameters is strongly encouraged.
        workspace_dimension = SlowlyChangingDimension(
            name='DIM_WORKSPACE',
            key='WORKSPACE_DIM_ID',
            attributes=['STG_WORKSPACE_ROWID','WORKSPACE_ID','TITLE','SUMMARY','PLANNED_START','PLANNED_FINISH','PARENT_WORKSPACE_ID','HEALTH','ORIGINATED_FROM_PROPOSAL',
                        'APPROVAL_STATUS','SUMMARY_TASK_ID','PRIORITY','PROJECT_NUMBER','STATE','NOTES','WORKSPACE_PHASE_ID','REQUEST_TYPE_ID','IS_CAPACITY_PLANNED',
                        'OBJECT_STATUS','CREATED_ON','UPDATED_BY','UPDATED_ON','ACTIVE_START_DATE','ACTIVE_FINISH_DATE','VERSION','ENTERPRISE_ID'],
            lookupatts=['WORKSPACE_ID'],
            versionatt='VERSION',
            fromatt='ACTIVE_START_DATE',
            toatt='ACTIVE_FINISH_DATE'
        )

        """ user_dimension = Dimension(
            name='DIM_WORKSPACE',
            key='WORKSPACE_DIM_ID',
            attributes=['STG_WORKSPACE_ROWID','WORKSPACE_ID','TITLE','SUMMARY','PLANNED_START','PLANNED_FINISH','PARENT_WORKSPACE_ID','HEALTH','ORIGINATED_FROM_PROPOSAL',
                        'APPROVAL_STATUS','SUMMARY_TASK_ID','PRIORITY','PROJECT_NUMBER','STATE','NOTES','WORKSPACE_PHASE_ID','REQUEST_TYPE_ID','IS_CAPACITY_PLANNED',
                        'OBJECT_STATUS','CREATED_ON','UPDATED_BY','UPDATED_ON','ACTIVE_START_DATE','ACTIVE_FINISH_DATE','VERSION','ENTERPRISE_ID'],
            lookupatts=['WORKSPACE_ID']
        ) """

       

        connector = SF_CONNECTOR().get_connector()
        cursor = connector.cursor(DictCursor)


        sql = f"""
           /* CTE for workspaces with new rows coming in */
            WITH changed_workspaces (workspaceid, rowid) AS (
                SELECT workspaceid, rowid
                FROM (
                    SELECT workspaceid, objectstatus, rowop, rowid, MAX(rowid) OVER (PARTITION BY workspaceid) latestrowid
                    FROM "PPM_TEST"."PRIVATE"."STG_WORKSPACE"
                    WHERE enterpriseid = 'E93B6B81-4208-4E73-8F45-C6375238B363'
                ) sw
                WHERE (NOT EXISTS (
                        SELECT workspace_id
                        FROM dim_workspace dw
                        WHERE active_finish_date IS NULL
                            AND sw.workspaceid = dw.workspace_id
                            AND dw.enterprise_id = 'E93B6B81-4208-4E73-8F45-C6375238B363'
                    ) OR UPPER(sw.rowop) = 'U')
                    AND BITAND(sw.objectstatus, 8) = 0
                    AND (sw.rowop IS NULL OR UPPER(sw.rowop) IN ('I', 'U'))
                    AND rowid = latestrowid
                )

            SELECT
                sw.rowid as STG_WORKSPACE_ROWID, 
                sw.workspaceid as WORKSPACE_ID,  
                title, 
                summary, 
                startdate as PLANNED_START, 
                enddate as PLANNED_FINISH, 
                parentworkspaceid as PARENT_WORKSPACE_ID, 
                externalid as PARENT_WORKSPACE_IDEXTERNAL_ID, 
                health, 
                DECODE(sw.workspaceproposalid, NULL, FALSE, TRUE) as ORIGINATED_FROM_PROPOSAL, 
                approvalstatus as APPROVAL_STATUS, 
                summarytaskid as SUMMARY_TASK_ID, 
                priority, 
                itemnumber as PROJECT_NUMBER,
                state, 
                notes, 
                workspacephaseid as WORKSPACE_PHASE_ID, 
                workspacerequesttypeid as REQUEST_TYPE_ID,
                iscapacityplanned as IS_CAPACITY_PLANNED, 
                objectstatus as OBJECT_STATUS, 
                createdon as CREATED_ON, 
                updatedby as UPDATED_BY,
                updatedon as UPDATED_ON, 
                enterpriseid as ENTERPRISE_ID
            FROM 
                "PPM_TEST"."PRIVATE"."STG_WORKSPACE" sw
                INNER JOIN changed_workspaces cw
                    ON sw.workspaceid = cw.workspaceid
                    AND sw.rowid = cw.rowid
            WHERE 
                sw.enterpriseid = 'E93B6B81-4208-4E73-8F45-C6375238B363'
        """

        cursor.execute(sql)

        """ for rec in results:
            print('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s' % (rec[0], rec[1], rec[2],rec[3],rec[4],rec[5],rec[6],rec[7],rec[8],rec[9],rec[10],rec[11],
            rec[12],rec[13],rec[14],rec[15],rec[16],rec[17],rec[18],rec[19],rec[20],rec[21],rec[22],))

            # The timestamp is split into its three parts
            #split_timestamp(rec) """

        for rec in cursor:
            workspace_dimension.scdensure(rec)

       
        connection.commit()


    except Exception as err:
        print('There was an error')
        print(err)
    finally:
        cursor.close()
        connector.close()
        connection.close()


# A normal Python function is used to split the timestamp into its parts
def split_timestamp(row):
    """Splits a timestamp containing a date into its three parts"""

    # First the timestamp is extracted from the row dictionary
    timestamp = row['CREATED_ON']

    # Then the string is split on the / in the time stamp
    timestamp_split = timestamp.split('/')

    # Finally each part is reassigned to the row dictionary. It can then be
    # accessed by the caller as the row is a reference to the dict object
    row['year'] = timestamp_split[0]
    row['month'] = timestamp_split[1]
    row['day'] = timestamp_split[2]

process()