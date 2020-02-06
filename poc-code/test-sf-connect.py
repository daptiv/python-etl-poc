#!/usr/bin/env python3
from sf_connector import SF_CONNECTOR

def test():

    print("Running test_sf_connector.test().....")

    connector = SF_CONNECTOR().get_connector()
    
    try:

        connector = SF_CONNECTOR().get_connector()
        cursor = connector.cursor()
    
        cursor.execute("SELECT current_version()")
        one_row = cursor.fetchone()
        print(one_row[0])

    finally:
        cursor.close()
        connector.close()

test()