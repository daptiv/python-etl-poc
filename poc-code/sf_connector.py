#!/usr/bin/env python3

import snowflake.connector
import os
import sys

class SF_CONNECTOR:

    def get_connector(self):

        try:

            ctx = snowflake.connector.connect(
                account='changepoint.us-east-1',
                user='US_DEV_SFSVC',
                password='Marines100!',
                database='HACKYPYTHONETL',
                warehouse = 'PPM_WH',
                schema = 'PUBLIC',
                timezone = 'UTC',
                autocommit = False
            )

            return ctx

        except Exception:
            raise
