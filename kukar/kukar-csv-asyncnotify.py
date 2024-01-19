# -*- coding: utf-8 -*-
#!/usr/bin/env python
# https://stackoverflow.com/questions/3258066/pyinotify-handling-in-modify-triggers

import os
from dotenv import load_dotenv
load_dotenv('../.env')  # take environment variables from .env.
# from apscheduler.schedulers.background import BackgroundScheduler
# import dropbox
# from influxdb import DataFrameClient
# from influxdb import InfluxDBClient

# from minotaur import Inotify, Mask
from asyncinotify import Inotify, Event, Mask
import ciso8601

import asyncio
from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS

from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

import numpy as np
import time
import timeit
import xdrlib
import http.client
import glob
from pathlib import Path
from datetime import timedelta
import pytz
import datetime as dt
import math
import pyinotify
import pandas as pd
import paho.mqtt.client as mqtt
import logging

class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    blue = "\x1b[34m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: blue + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

# create logger with 'spam_application'
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(logging.ERROR)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())

logger.addHandler(ch)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# fh.setFormatter(formatter)
pd.options.display.float_format = '{:.2f}'.format
# pd.set_option("display.max_rows", None, "display.max_columns", None)

# fileList = Path("/home/shms/ftp/2004_wika_lukulo/DMM").glob("DMM_Data*.txt")
# for path in fileList:
# print(path)

# fileList = glob.glob('/home/shms/ftp/2004_wika_lukulo/DMM").glob("DMM_Data*.txt')

# sensorType = 'vwsg'

URL=os.getenv("URL_TEST")
TOKEN=os.getenv('TOKEN_TEST')
ORG=os.getenv('ORG_TEST')
BUCKET=os.getenv('BUCKET_TEST')

TZ_ORIGIN='Asia/Makassar'

folders_to_watch = [
    # '/home/shms/ftp/2023-Kukar-Box1/LVDT/1s',
    '/home/shms/ftp/2023-Kukar-Box1',
    '/home/shms/ftp/2023-Kukar-Box2',
]

# fileToCreate = localFolder + sensorType + '/' + sensorType + '.csv'
def my_date_parser(t):
    ## Adding a timezone                    
    tz_origin_obj = pytz.timezone(TZ_ORIGIN)   
    aware = tz_origin_obj.localize(ciso8601.parse_datetime(t))
    return aware

def process_csijson(event):
    global job
    job += 1
    
    short_file_name = event.name
    full_file_name = event.pathname
    
    # file_list = []
    # for x in folders_to_watch:
    #     file_list = file_list + glob.glob(x + '*.txt')

    logger.info("[%u] File closed:: %s", job, short_file_name)        

    f = open(full_file_name, encoding="utf-8")
    first_line = f.readline()
    
    #remove quotes & \n, then split to tuple
    x = eval(first_line)
    
    MEASUREMENT = x[1]

    TAGS = {
        # 'station_name': x[1],
        'model': x[2],
        'serial_no': x[3],
        # 'os_version': x[4],
        # 'prog_name': x[5],
        'table_name': x[7],
    }
    
    df = pd.read_csv(full_file_name,
                    skiprows=[2, 3],
                    index_col='TIMESTAMP',
                    parse_dates=True,
                    header=1,
                    usecols=lambda x: x != 'RECORD',
                    na_values='NAN',
                    infer_datetime_format=True,
                    date_parser = my_date_parser,
                    )
    
    # ensure all sensor values are dloat data type
    df = df.astype(float)
    # print(df.dtypes)
    
    logger.debug(df)
    
    # add TAGS to dataframe
    temp_df = pd.DataFrame(TAGS, index=df.index)
    df = pd.concat([df, temp_df], axis=1)
    

    if (1 and len(df)):
        logger.info("[%u] Uploading [%s] to InfluxDB", job, short_file_name)

        start = time.time()

        client = InfluxDBClient(
            url=URL,
            token=TOKEN,
            org=ORG,
            # enable_gzip=True,
            # verify_ssl=False
        )

        write_api = client.write_api(write_options=SYNCHRONOUS)
        
        try:
            write_api.write(
                bucket=BUCKET,
                record=df,
                data_frame_measurement_name=MEASUREMENT,
                data_frame_tag_columns=TAGS
            )
        except Exception as e:
            logger.error("Exception occurred, send to influx failed", exc_info=True)
            if client is not None:
                client.close()
                
        """
        Close client
        """
        if client is not None:
            client.close()

        end = time.time()
        diff = end - start
        # diff.total_seconds() * 1000
        logger.info('[%u] Took: %.2f seconds' ,job ,diff)

       
    else:
        logger.debug("No data is available")
        """
        Close client
        """
        # client.close()

async def process_csijson_async(event):
    global job
    job += 1
    
    short_file_name = event.name
    full_file_name = event.pathname
    
    # file_list = []
    # for x in folders_to_watch:
    #     file_list = file_list + glob.glob(x + '*.txt')

    logger.info("[%u] File closed:: %s", job, short_file_name)        

    f = open(full_file_name, encoding="utf-8")
    first_line = f.readline()
    
    #remove quotes & \n, then split to tuple
    x = eval(first_line)
    
    MEASUREMENT = x[1]

    TAGS = {
        # 'station_name': x[1],
        'model': x[2],
        'serial_no': x[3],
        # 'os_version': x[4],
        # 'prog_name': x[5],
        'table_name': x[7],
    }
    
    df = pd.read_csv(full_file_name,
                    skiprows=[2, 3],
                    index_col='TIMESTAMP',
                    parse_dates=True,
                    header=1,
                    usecols=lambda x: x != 'RECORD',
                    na_values='NAN',
                    infer_datetime_format=True,
                    date_parser = my_date_parser,
                    )
    
    # ensure all sensor values are dloat data type
    df = df.astype(float)
    # print(df.dtypes)
    
    logger.debug(df)
    
    # add TAGS to dataframe
    temp_df = pd.DataFrame(TAGS, index=df.index)
    df = pd.concat([df, temp_df], axis=1)
    

    if (1 and len(df)):
        logger.info("[%u] Uploading [%s] to InfluxDB", job, short_file_name)

        start = time.time()

        try:
            async with InfluxDBClientAsync(
                url=URL,
                token=TOKEN,
                org=ORG,
                # enable_gzip=True,
                # verify_ssl=False
            ) as client:
                await client.write_api().write(
                    bucket=BUCKET,
                    record=df,
                    data_frame_measurement_name=MEASUREMENT,
                    data_frame_tag_columns=TAGS
                )
        except Exception as e:
            logger.error("Exception occurred, send to influx failed", exc_info=True)
            if client is not None:
                await client.close()
            

        end = time.time()
        diff = end - start
        # diff.total_seconds() * 1000
        logger.info('[%u] Took: %.2f seconds' ,job ,diff)

        """
        Close client
        """
        if client is not None:
            await client.close()

job = 0

class MyEventHandler(pyinotify.ProcessEvent):
    """Class representing a person"""
    # def process_IN_CLOSE_NOWRITE(self, event):
    # print ("File closed:", event.pathname)

    # def process_IN_OPEN(self, event):
    # print ("File opened::", event.pathname)

    # def process_IN_ATTRIB(self, event):
    # print ("File closed::", event.pathname)
    
    
    
    def process_IN_CLOSE_WRITE(self, event):
        """Function printing python version."""
        
        asyncio.run(process_csijson(event))

        
from pathlib import Path
from typing import Generator, AsyncGenerator
import posixpath

def get_directories_recursive(path: Path) -> Generator[Path, None, None]:
    '''Recursively list all directories under path, including path itself, if
    it's a directory.

    The path itself is always yielded before its children are iterated, so you
    can pre-process a path (by watching it with inotify) before you get the
    directory listing.

    Passing a non-directory won't raise an error or anything, it'll just yield
    nothing.
    '''

    if path.is_dir():
        yield path
        for child in path.iterdir():
            yield from get_directories_recursive(child)

# async def watch_recursive(path: Path, mask: Mask) -> AsyncGenerator[Event, None]:
async def watch_recursive(paths, mask: Mask) -> AsyncGenerator[Event, None]:
    with Inotify() as inotify:
        for path in paths:
            for directory in get_directories_recursive(Path(path)):
                print(f'INIT: watching {directory}')
                inotify.add_watch(directory, mask | Mask.MOVED_FROM | Mask.MOVED_TO | Mask.CREATE | Mask.DELETE_SELF | Mask.IGNORED)

            # Things that can throw this off:
            #
            # * Moving a watched directory out of the watch tree (will still
            #   generate events even when outside of directory tree)
            #
            # * Doing two changes on a directory or something before the program
            #   has a time to handle it (this will also throw off a lot of inotify
            #   code, though)
            #
            # * Moving a watched directory within a watched directory will get the
            #   wrong path.  This needs to use the cookie system to link events
            #   together and complete the move properly, which can still make some
            #   events get the wrong path if you get file events during the move or
            #   something silly like that, since MOVED_FROM and MOVED_TO aren't
            #   guaranteed to be contiguous.  That exercise is left up to the
            #   reader.
            #
            # * Trying to watch a path that doesn't exist won't automatically
            #   create it or anything of the sort.
            #
            # * Deleting and recreating or moving the watched directory won't do
            #   anything special, but it probably should.
        async for event in inotify:

            # If there is at least some overlap, assume the user wants this event.
            if event.mask & mask:
                # print(f'YIELD EVENT: {event.path}')
                yield event
            else:
                # Note that these events are needed for cleanup purposes.
                # We'll always get IGNORED events so the watch can be removed
                # from the inotify.  We don't need to do anything with the
                # events, but they do need to be generated for cleanup.
                # We don't need to pass IGNORED events up, because the end-user
                # doesn't have the inotify instance anyway, and IGNORED is just
                # used for management purposes.
                logger.info(f'UNYIELDED EVENT: {event}')

async def main():
    async for event in watch_recursive(
        [
            '/home/shms/ftp/2023-Kukar-Box1',
            '/home/shms/ftp/2023-Kukar-Box2'
            ],
        Mask.CLOSE_WRITE):
        # print(f'MAIN: got {event} for path {repr(event.path)}')
        logger.info(f'MAIN: got event for path {event.path.resolve()}')
        # print(repr(event.path))
            
        obj = lambda: None
        obj.name = event.name
        obj.pathname = event.path.resolve()
        # d = {'name': evt.name, 'pathname': Path.joinpath(path, evt.name)}
        # for k, v in d.items():
        #     setattr(obj, k, v)
        # print(event.path.resolve())
        
        # process_csijson(obj)
        await process_csijson_async(obj)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print('shutting down')
    
# if __name__ == '__main__':
#     logger.info('kukar-csv.py started')
#     # main()
#     main()
#     logger.info('notifier-loop started')

