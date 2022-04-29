# -*- coding: utf-8 -*-
#!/usr/bin/env python
# https://stackoverflow.com/questions/3258066/pyinotify-handling-in-modify-triggers

import pandas as pd
import pyinotify
import math
import datetime as dt
from datetime import timedelta
from pathlib import Path
import glob
import http.client
import xdrlib
import time
import numpy as np
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient, Point, Dialect
import os
from dotenv import load_dotenv
load_dotenv('../.env')  # take environment variables from .env.
# from apscheduler.schedulers.background import BackgroundScheduler
# import dropbox
# from influxdb import DataFrameClient
# from influxdb import InfluxDBClient
pd.options.display.float_format = '{:.2f}'.format
# pd.set_option("display.max_rows", None, "display.max_columns", None)

# fileList = Path("/home/shms/ftp/2004_wika_lukulo/DMM").glob("DMM_Data*.txt")
# for path in fileList:
# print(path)

# fileList = glob.glob('/home/shms/ftp/2004_wika_lukulo/DMM").glob("DMM_Data*.txt')

# sensorType = 'vwsg'

URL = os.getenv("URL_WIKA_LUKULO")
TOKEN = os.getenv('TOKEN_WIKA_LUKULO')
ORG = os.getenv('ORG_WIKA_LUKULO')
BUCKET = os.getenv('BUCKET_WIKA_LUKULO')

localFolder = [
    '/home/shms/ftp/2004_wika_lukulo/DMM/',
    # '/home/shms/ftp/2004_wika_lukulo/DMM/DMM_Data_2021-09-19.txt',
]

# fileToCreate = localFolder + sensorType + '/' + sensorType + '.csv'

# DATABASE = 'pdcm-martadipura'


class MyEventHandler(pyinotify.ProcessEvent):
    # def process_IN_CLOSE_NOWRITE(self, event):
    # print ("File closed:", event.pathname)

    # def process_IN_OPEN(self, event):
    # print ("File opened::", event.pathname)

    # def process_IN_CREATE(self, event):
    #     print ("File closed::", event.pathname)

    def process_IN_CLOSE_WRITE(self, event):
        fileList = []
        filePath = localFolder
        for x in filePath:
            # print(x)
            fileList = fileList + glob.glob(x + '*.txt')
        # fileList = glob.glob('/home/shms/ftp/2004_wika_lukulo/DMM/test.txt')
        # print(fileList)

        print("File closed::", event.pathname)
        # if event.pathname == '/home/shms/ftp/2004_wika_lukulo/DMM/test.txt':

        KOLOM_DATE_TIME = 'TIMESTAMP'
        if event.pathname in fileList == False:
            print('Path is not in the list')
        else:

            fileName = event.pathname

            if 'diag' in fileName and 'Strain' in fileName:
                sensorType = 'diag_vwsg'
            elif "VWSG" in fileName:
                sensorType = 'vwsg'
                fixtags = {"sensorType": "vwsg", "box": "1"}
            elif "DMM" in fileName:
                sensorType = 'dmm'
                fixtags = {"sensorType": "DMM", "box": "1"}
            elif "Miros" in fileName:
                sensorType = 'miros'
                fixtags = {"sensorType": "miros", "box": "1"}
            else:
                print('Sensor type does not match.')
                return

            cols = list(pd.read_csv(fileName,
                                    index_col=False,
                                    # index_col='TIMESTAMP',
                                    #  drop=False,
                                    # parse_dates=True,
                                    header=1,
                                    # dtype = {'DMM_Calc': np.float64},
                                    # names=['Employee', 'Hired','Salary', 'Sick Days'],
                                    skiprows=[2, 3],
                                    nrows=10
                                    ))

            # print(cols)

            df = pd.read_csv(fileName,
                             #  index_col=False,
                             index_col='TIMESTAMP',
                             #   drop=False,
                             #  parse_dates=True,
                             header=1,
                             # dtype = {'DMM_Calc': np.float64},
                             # names=['Employee', 'Hired','Salary', 'Sick Days'],
                             usecols=[i for i in cols if i != 'RECORD'],
                             skiprows=[2, 3],
                             na_values='NAN',
                             #  nrows=2,
                             )

            # remove TS and Nan rows
            # df = df.drop(['TS', math.nan])
            # df = df.drop([0, 1])

            print(df)

            field_keys = list(df.columns)
            # print(field_keys)

            # cols = [5,6,7]
            # df.drop(df.columns[cols],axis=1,inplace=True)

            # df = df.drop([5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20], axis=1)
            # print(df)
            # df['TIMESTAMP'].astype('int64')//1e9

            # convert date string to datetime obj
            # df[KOLOM_DATE_TIME] = pd.to_datetime(df[KOLOM_DATE_TIME]) - timedelta(hours=0)

            df.index = pd.to_datetime(df.index) - timedelta(hours=8)

            # df.index = pd.to_datetime(df.index)

            # df.set_index('TIMESTAMP', inplace=True, drop=False)
            # df.set_index('TIMESTAMP')

            # print(df)

            # unique_list = df_zero.index.unique()

            # print('unique_list', unique_list)

            # idx_zero = df_zero.index[df_zero['InstrumentID']]
            # print('idx_zero', idx_zero)

            # id = df_zero['InstrumentID']
            # ts = df_zero[KOLOM_DATE_TIME]

            # s1.loc['b']

            # Convert multiple columns
            df = df.astype({'Reading_Interval':'int32','DMM_Reading':'float','DMM_Calc':'float'})

            if (1 and len(df)):
                print("Uploading [", event.pathname, "] to InfluxDB...")

                # df_temp = df_filtered.copy()

                # df_filtered[KOLOM_DATE_TIME] = pd.to_datetime(df_temp[KOLOM_DATE_TIME])
                # df_temp[KOLOM_DATE_TIME] = df_temp[KOLOM_DATE_TIME].astype('int64')
                # df = df.set_index(KOLOM_DATE_TIME)

                # mytag = df['InstrumentID'].values[0]

                # df_temp = df_temp.drop('InstrumentID')
                # df_filtered.drop(df_filtered.columns[0],axis=1,inplace=True)

                # print(mytag)

                start = time.time()

                # PandaUploadToInflux(df, 'MPBX', field_keys, 'localhost', 8086)

                # DATABASE = 'pdcm-martadipura'
                # client = DataFrameClient(host='localhost', port=8086)

                # client = InfluxDBClient(host='mydomain.com', port=8086, username='myuser', password='mypass' ssl=True, verify_ssl=True)
                # client.drop_database(DATABASE)
                # client.create_database(DATABASE)
                # client.get_list_database()
                # client.switch_database(DATABASE)

                client = InfluxDBClient(
                    url=URL,
                    token=TOKEN,
                    org=ORG
                )

                write_api = client.write_api(write_options=SYNCHRONOUS)

                # write_points(dataframe, measurement, tags=None, tag_columns=None, field_columns=None, time_precision=None, database=None, retention_policy=None, batch_size=None, protocol=u'line', numeric_precision=None)
                # measurement – name of measurement
                # tags – dictionary of tags, with string key-values
                # tag_columns – [Optional, default None] List of data tag names
                # field_columns – [Options, default None] List of data field names
                # time_precision – [Optional, default None] Either ‘s’, ‘ms’, ‘u’ or ‘n’.
                # batch_size (int) – [Optional] Value to write the points in batches instead of all at one time. Useful for when doing data dumps from one database to another or when doing a massive write operation
                # protocol – Protocol for writing data. Either ‘line’ or ‘json’.
                # numeric_precision – Precision for floating point values. Either None, ‘full’ or some int, where int is the desired decimal precision. ‘full’ preserves full precision for int and float datatypes. Defaults to None, which preserves 14-15 significant figures for float and all significant figures for int datatypes.

                if (1):
                    # client.write_points(
                    #     df,
                    #     sensorType,
                    #     database=DATABASE,
                    #     tags=fixtags,
                    #     # tag_columns=TAG_COLUMNS,
                    #     field_columns=field_keys,
                    #     time_precision='s',
                    #     # batch_size=,
                    #     # protocol='line',
                    #     numeric_precision='full'
                    # )

                    write_api.write(
                        bucket=BUCKET,
                        record=df,
                        data_frame_measurement_name=sensorType,
                        data_frame_tag_columns=[
                            # 'DMM_CurrentMode',
                            'Reading_Interval'
                        ]
                    )

                end = time.time()
                diff = end - start
                # diff.total_seconds() * 1000
                print('Took: %.2f seconds' % (diff))

                """
                Close client
                """
                client.close()

                # oldDate = int(str(data[-1][0]))

                # file = open("./lastdate.txt", "w")
                # file.write(str(oldDate) + ' success' + ' ' + Nanosecond_To_Human(oldDate)[1] )
                # file = open("./lastdate.txt", "r")
                # oldFile = file.read()
                # oldDate = int(oldFile.split(' ')[0])
                # oldStatus = oldFile.split(' ')[1]
                # print('File: ', oldDate, oldStatus)

                # if result2 == result1:
                # result1 = int(subprocess.check_output(cmd).split(' ')[0])
                # file.write(str(data[-1][0])+ ' success')
            else:
                print("No data is available")
                """
                Close client
                """
                client.close()


def main():
    print('DMM started')
    # Watch manager (stores watches, you can add multiple dirs)
    wm = pyinotify.WatchManager()
    # User's music is in /tmp/music, watch recursively
    wm.add_watch(localFolder, pyinotify.ALL_EVENTS, rec=True, auto_add=True)

    # Previously defined event handler class
    eh = MyEventHandler()

    # Register the event handler with the notifier and listen for events
    notifier = pyinotify.Notifier(wm, eh)
    notifier.loop()
    


if __name__ == '__main__':
    # sched = BackgroundScheduler()

    # job = sched.add_job(DropboxDownload, 'interval', minutes=5)
    # job = sched.add_job(DropboxDownload, 'interval', seconds=15)

    # start scheduler
    # sched.start()

    # job.remove()
    
    # print(URL, TOKEN)
    main()
    # DropboxDownload()
    print('HEHEHEHEH')
