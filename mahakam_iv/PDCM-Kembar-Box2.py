# -*- coding: utf-8 -*-
#!/usr/bin/env python
# https://stackoverflow.com/questions/3258066/pyinotify-handling-in-modify-triggers

import os
from sys import stdout
from dotenv import load_dotenv
load_dotenv('../.env')  # take environment variables from .env.
# from apscheduler.schedulers.background import BackgroundScheduler
# import dropbox
# from influxdb import DataFrameClient
# from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
import numpy as np
import time
import xdrlib
import http.client
import glob
from pathlib import Path
from datetime import timedelta
import datetime as dt
import math
import pyinotify
import pandas as pd

import logging
import json

class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    blue = "\x1b[34m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    # format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    format = "%(asctime)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: blue + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S %Z")
        return formatter.format(record)

# create logger with 'spam_application'
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.ERROR)

# create console handler with a higher log level
ch = logging.StreamHandler(stdout)
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())

logger.addHandler(ch)

pd.options.display.float_format = '{:.2f}'.format

URL=os.getenv("URL_MAHAKAM_IV")
TOKEN=os.getenv('TOKEN_MAHAKAM_IV')
ORG=os.getenv('ORG_MAHAKAM_IV')
BUCKET=os.getenv('BUCKET_MAHAKAM_IV')

TZ_ORIGIN='Asia/Makassar'

localFolder = [
    '/home/shms/ftp/2007-kembar-additional/vwsg/',
    '/home/shms/ftp/2007-kembar-additional/lvdt/',
    # '/home/shms/ftp/2007-kembar-additional/diag/',
]

def my_date_parser_2(dt_naive):
    dt_aware = pd.to_datetime(dt_naive, errors='coerce', utc=False).tz_localize(TZ_ORIGIN)
    return dt_aware


class MyEventHandler(pyinotify.ProcessEvent):
    # def process_IN_CLOSE_NOWRITE(self, event):
    # print ("File closed:", event.pathname)

    # def process_IN_OPEN(self, event):
    # print ("File opened::", event.pathname)

    # def process_IN_ATTRIB(self, event):
    # print ("File closed::", event.pathname)

    def process_IN_CLOSE_WRITE(self, event):

        short_file_name = event.name
        full_file_name = event.pathname
        
        logger.info("Parsing:: %s", short_file_name)   

        if 'diag' in full_file_name and 'Strain' in full_file_name:
            sensorType = 'diag_vwsg'
        elif "vwsg" in full_file_name:
            sensorType = 'vwsg'
        elif "lvdt" in full_file_name:
            sensorType = 'lvdt'
        else:
            logger.warning('Sensor type not in the list!')
            return

        df = pd.read_csv(
            full_file_name,
            skiprows=range(2, 4),
            index_col='TIMESTAMP',
            parse_dates=True,
            header=1,
            usecols=lambda x: x != 'RECORD',
            na_values='NAN',
            infer_datetime_format=True,
            # error_bad_lines=False,
            # warn_bad_lines=True,
            on_bad_lines='skip',
            date_parser = my_date_parser_2,
        )
        
        # =========================================================
        # filter out row with bad datetime
        # Note: this can only be achieved if using my_date_parser_2
        # =========================================================
        df = df[~df.index.isnull()]
        
        # =========================================================================================================
        # catch bad string that cannot be converted into float or numeric
        # source: https://saturncloud.io/blog/how-to-handle-the-pandas-valueerror-could-not-convert-string-to-float/
        # source: https://stackoverflow.com/a/36814203/10079180
        # =========================================================================================================
        df = df.apply(pd.to_numeric, errors='coerce')
        
        # once bad rows are handled/filtered out, conver all sensor data to float
        df = df.astype(float)
        
        logger.debug(df)
        
        # add TAGS to dataframe
        logger.debug(df)

        # print(df)

        if (1 and len(df)):
            logger.info("Uploading %u points [%s] to InfluxDB...", len(df), short_file_name)

            start = time.time()

            client = InfluxDBClient(
                url=URL,
                token=TOKEN,
                org=ORG
            )

            write_api = client.write_api(write_options=SYNCHRONOUS)

            try:
                write_api.write(
                    bucket=BUCKET,
                    record=df,
                    data_frame_measurement_name=sensorType
                )
            except Exception as e:
                logger.error("Exception occurred, send to influx failed", exc_info=True)
                if client is not None:
                    write_api.__del__()
                    client.__del__()
            else:
                end = time.time()
                diff = end - start
                # diff.total_seconds() * 1000
                logger.info('Took: %.2f seconds' % (diff))

                """
                Close client
                """
                if client is not None:
                    write_api.close()
                    client.close()

        else:
            logger.warning("No data is available")


def main():
    # Watch manager (stores watches, you can add multiple dirs)
    wm = pyinotify.WatchManager()
    # User's music is in /tmp/music, watch recursively
    wm.add_watch(localFolder, pyinotify.ALL_EVENTS, rec=True)

    # Previously defined event handler class
    eh = MyEventHandler()

    # Register the event handler with the notifier and listen for events
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()


try:
    logger.info('%s started', os.path.basename(__file__))
    main()
except KeyboardInterrupt:
    logger.info('shutting down')
except Exception as e:
    logger.error("Exception occurred", exc_info=True)
