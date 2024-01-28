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

# from minotaur import Inotify, Mask
from asyncinotify import Inotify, Event, Mask
import ciso8601

import asyncio
from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS

from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

import numpy as np
import time
from zoneinfo import ZoneInfo
import timeit
import xdrlib
import http.client
import glob
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pytz
import datetime as dt
import math
import pyinotify
import pandas as pd
import paho.mqtt.client as mqtt
import logging
import json
import random

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
logger.setLevel(logging.INFO)

# create console handler with a higher log level
ch = logging.StreamHandler(stdout)
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

DICT_FILES_TO_WATCH=os.path.join(
    os.path.dirname(__file__),
    'dict-' + os.path.splitext(os.path.basename(__file__))[0] + '.json'
    )

folders_to_watch = [
    '/home/shms/ftp/2023-Kukar-Box1',
    '/home/shms/ftp/2023-Kukar-Box2',
    # '/home/shms/ftp/test',
]

myjob = 0

# fileToCreate = localFolder + sensorType + '/' + sensorType + '.csv'
def my_date_parser(t):
    ## Adding a timezone                    
    tz_origin_obj = pytz.timezone(TZ_ORIGIN)   
    aware = tz_origin_obj.localize(ciso8601.parse_datetime(t))
    return aware

def my_date_parser_2(dt_naive):
    dt_aware = pd.to_datetime(dt_naive, errors='coerce', utc=False).tz_localize(TZ_ORIGIN)
    return dt_aware

def update_my_list(csv_shor_file_name, dataframe, dict_buffer, dict_file_full_path):    
    if csv_shor_file_name not in dict_buffer:
        dict_buffer[csv_shor_file_name] = dict()        
        insert_date = datetime.now(tz=ZoneInfo('UTC'))  
        dict_buffer[csv_shor_file_name]['insert_date'] = insert_date
        dict_buffer[csv_shor_file_name]['init'] = len(dataframe)
        dict_buffer[csv_shor_file_name]['last'] = len(dataframe)
        dict_buffer[csv_shor_file_name]['age'] = 0
    else:
        insert_date = datetime.now(tz=ZoneInfo('UTC'))
        dict_buffer[csv_shor_file_name]['insert_date'] = insert_date
        age = datetime.now(tz=ZoneInfo('UTC')) - dict_buffer[csv_shor_file_name]['insert_date']
        dict_buffer[csv_shor_file_name]['age'] = round(age.total_seconds(),1)
        dict_buffer[csv_shor_file_name]['last'] += len(dataframe)

    for v,z in list(dict_buffer.items()):
        logger.debug('age:%5.0f\tinit:%u\tlast:%u\t%s' % (dict_buffer[v]['age'],dict_buffer[v]['init'],dict_buffer[v]['last'],v))
        diff = datetime.now(tz=ZoneInfo('UTC')) - dict_buffer[v]['insert_date']
        dict_buffer[v]['age'] = round(diff.total_seconds(),1)
        if (dict_buffer[v]['age']) > 1800:
            del dict_buffer[v]
            logger.warning('key [%s] removed from dict_buffer', v)
            
    # Convert and write JSON object to file
    try:
        with open(dict_file_full_path, "w") as outfile:
            json.dump(dict_buffer, outfile, indent=4, sort_keys=True, default=str)
            logger.debug(json.dumps(dict_buffer, indent=4, sort_keys=True, default=str))
            outfile.close()
    except Exception as e:
        logger.error("%s, failed updating my_list file", e)
        
        
# =================
# ASYNC
# =================



async def process_csijson_async(_job, event): 
    my_list = dict()
    try:
        with open(DICT_FILES_TO_WATCH, 'r') as j:
            my_list = json.loads(j.read())
            for v,z in list(my_list.items()):
                my_list[v]['insert_date'] = ciso8601.parse_datetime(my_list[v]['insert_date'])
            j.close()

    # except FileExistsError:
    except FileNotFoundError as e:
        logger.warning("File %s not found", os.path.basename(DICT_FILES_TO_WATCH))
    except Exception as e:
        logger.error("Exception error while converting my_list to dict", exc_info=True)
    
    short_file_name = event.name.stem
    full_file_name = event.pathname.absolute().as_posix()

    # sleep = random.randint(0, 9)
    # print("[%u] sleep %u" % (_job, sleep))
    # logger.info("[%u] Parsing:: %s", _job, short_file_name)    
    # await asyncio.sleep(sleep)     
    # logger.info("[%u] Completed:: %s", _job, short_file_name)     
    # return 'wakeup ' + str(_job)

    logger.info("[%u] Parsing:: %s", _job, short_file_name) 

    f = open(full_file_name, encoding="utf-8")
    first_line = f.readline()
    f.close()
    
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
    
    
    if full_file_name not in my_list:
        start_row = 4
    else:
        start_row = my_list[full_file_name]['last'] + 4
        
    try:
        df = pd.read_csv(
            full_file_name,
            skiprows=range(2, start_row),
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
    except Exception as e:
        # logger.error("%s %s", e, full_file_name, exc_info=True)
        logger.error("%s, file: [%s]", e, full_file_name)
    else:
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
        temp_df = pd.DataFrame(TAGS, index=df.index)
        df = pd.concat([df, temp_df], axis=1)
        logger.debug(df)
        
        if (1 and len(df)):
            pass
        else:
            if len(df)==0:
                logger.warning("[%u] dataframe is empty [%s]", _job, full_file_name)
            return
        
        logger.info("[%u] Uploading %u points [%s]", _job, len(df), short_file_name)

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
                await client.close()
                # await asyncio.gather(db_write, db_close, return_exceptions=True)
                # update_my_list(full_file_name, df, my_list, DICT_FILES_TO_WATCH)    
                
                # end = time.time()
                # diff = end - start
                # logger.info('[%u] Took: %.2f seconds' ,_job ,diff)
                # return
        except Exception as e:
            if client is not None:
                await client.close()
            logger.error("Failed send to DB, %s", e, exc_info=False)
        else:
            
            update_my_list(full_file_name, df, my_list, DICT_FILES_TO_WATCH)    
            
            end = time.time()
            diff = end - start
            logger.info('[%u] Took: %.2f seconds' ,_job ,diff)
            return

        
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
                logger.debug(f'INIT: watching {directory}')
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
                logger.warning(f'UNYIELDED EVENT: {event}')
                
async def some_method(cond):
    print("Starting...")
    
    await cond.acquire()
    await cond.wait()
    cond.release()
    
    print("Finshed...")

async def main():    
    async for event in watch_recursive(
            folders_to_watch,
            Mask.CLOSE_WRITE
        ):
        
        global myjob
        myjob += 1
        
        # print(f'MAIN: got {event} for path {repr(event.path)}')
        logger.info(f'[{myjob}] {event.mask} event for file {event.name}')
        # print(repr(event.path))
            
        obj = lambda: None
        obj.name = event.name
        obj.pathname = event.path.resolve()        
        
        asyncio.create_task(process_csijson_async(myjob, obj))
        # task = asyncio.create_task(process_csijson_async(myjob, obj))
        # await task

try:
    print('%s started' % os.path.basename(__file__))
    asyncio.run(main())
except KeyboardInterrupt:
    print('shutting down')
except Exception as e:
    logger.error("Exception occurred", exc_info=True)

