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
import paho.mqtt.client as mqtt

# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# fh.setFormatter(formatter)
pd.options.display.float_format = '{:.2f}'.format
# pd.set_option("display.max_rows", None, "display.max_columns", None)

# fileList = Path("/home/shms/ftp/2004_wika_lukulo/DMM").glob("DMM_Data*.txt")
# for path in fileList:
# print(path)

# fileList = glob.glob('/home/shms/ftp/2004_wika_lukulo/DMM").glob("DMM_Data*.txt')

# sensorType = 'vwsg'

URL=os.getenv("URL_SUNGAIPUTE")
TOKEN=os.getenv('TOKEN_SUNGAIPUTE')
ORG=os.getenv('ORG_SUNGAIPUTE')
BUCKET=os.getenv('BUCKET_SUNGAIPUTE')

MQTT_ADDRESS = os.getenv("MQTT_ADDRESS_EMERALD")
MQTT_USER = os.getenv("MQTT_USER_EMERALD")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD_EMERALD")
# MQTT_TOPIC = 'home/+/+'
# MQTT_REGEX = 'home/([^/]+)/([^/]+)'
MQTT_TOPIC_LWT = 'ENERGYMETER_03F245/mqttstatus'
MQTT_TOPIC_1 = 'cs/v1/pdcm/kembar-box-1/state/cr6/11759/'
MQTT_TOPIC_2 = 'cs/v1/pdcm/kembar-box-1/data/cr6/11759/Fast_DMM_Data/cj'
#MQTT_REGEX = 'ENERGYMETER_58C210/meterreading/60s'
MQTT_CLIENT_ID = 'script-pdcm-kembar-box1'

TIMEZONE=8

localFolder = [
    '/home/shms/ftp/2010-sungaipute/data_daily/',
]

# fileToCreate = localFolder + sensorType + '/' + sensorType + '.csv'


class MyEventHandler(pyinotify.ProcessEvent):
    # def process_IN_CLOSE_NOWRITE(self, event):
    # print ("File closed:", event.pathname)

    # def process_IN_OPEN(self, event):
    # print ("File opened::", event.pathname)

    # def process_IN_ATTRIB(self, event):
    # print ("File closed::", event.pathname)

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

        # if event.pathname in fileList:

        fileName = event.pathname

        if "batttemp" in fileName:
            sensorType = 'batttemp'
        elif "strandmeter" in fileName:
            sensorType = 'strandmeter'
        # elif "temperature" in fileName:
            # sensorType = 'temperature'
        # elif "BattTemp" in fileName:
            # sensorType = 'BattTemp-Box1'
        # elif 'diag' in fileName and 'Strain' in fileName:
            # sensorType = 'diag_vwsg'
        else:
            print('Sensor type not in the list!')
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
        
        dataType={
         #'Name': str,
         #'Grade': int
         'BattV': float,
         'PTemp': float,
         
         'D(1)': float,
         'D(2)': float,
         'D(3)': float,
         'D(4)': float,
         'D(5)': float,
         'D(6)': float,
         'D(7)': float,
         'D(8)': float,
         'D(9)': float,
         'D(10)': float,
         'D(11)': float,
         'D(12)': float,
         'D(13)': float,
         'D(14)': float,
         'D(15)': float,
         'D(16)': float,
         
         'D_corrected(1)': float,
         'D_corrected(2)': float,
         'D_corrected(3)': float,
         'D_corrected(4)': float,
         'D_corrected(5)': float,
         'D_corrected(6)': float,
         'D_corrected(7)': float,
         'D_corrected(8)': float,
         'D_corrected(9)': float,
         'D_corrected(10)': float,
         'D_corrected(11)': float,
         'D_corrected(12)': float,
         'D_corrected(13)': float,
         'D_corrected(14)': float,
         'D_corrected(15)': float,
         'D_corrected(16)': float,
         
         'Strain_corrected(1)': float,
         'Strain_corrected(2)': float,
         'Strain_corrected(3)': float,
         'Strain_corrected(4)': float,
         'Strain_corrected(5)': float,
         'Strain_corrected(6)': float,
         'Strain_corrected(7)': float,
         'Strain_corrected(8)': float,
         'Strain_corrected(9)': float,
         'Strain_corrected(10)': float,
         'Strain_corrected(11)': float,
         'Strain_corrected(12)': float,
         'Strain_corrected(13)': float,
         'Strain_corrected(14)': float,
         'Strain_corrected(15)': float,
         'Strain_corrected(16)': float,
         
         'Tension_corrected(1)': float,
         'Tension_corrected(2)': float,
         'Tension_corrected(3)': float,
         'Tension_corrected(4)': float,
         'Tension_corrected(5)': float,
         'Tension_corrected(6)': float,
         'Tension_corrected(7)': float,
         'Tension_corrected(8)': float,
         'Tension_corrected(9)': float,
         'Tension_corrected(10)': float,
         'Tension_corrected(11)': float,
         'Tension_corrected(12)': float,
         'Tension_corrected(13)': float,
         'Tension_corrected(14)': float,
         'Tension_corrected(15)': float,
         'Tension_corrected(16)': float,
         
         'Tension_Total(1)': float,
         'Tension_Total(2)': float,
         'Tension_Total(3)': float,
         'Tension_Total(4)': float,
         'Tension_Total(5)': float,
         'Tension_Total(6)': float,
         'Tension_Total(7)': float,
         'Tension_Total(8)': float,
         'Tension_Total(9)': float,
         'Tension_Total(10)': float,
         'Tension_Total(11)': float,
         'Tension_Total(12)': float,
         'Tension_Total(13)': float,
         'Tension_Total(14)': float,
         'Tension_Total(15)': float,
         'Tension_Total(16)': float,
         
         'Temp(1)': float,
         'Temp(2)': float,
         'Temp(3)': float,
         'Temp(4)': float,
         'Temp(5)': float,
         'Temp(6)': float,
         'Temp(7)': float,
         'Temp(8)': float,
         'Temp(9)': float,
         'Temp(10)': float,
         'Temp(11)': float,
         'Temp(12)': float,
         'Temp(13)': float,
         'Temp(14)': float,
         'Temp(15)': float,
         'Temp(16)': float,
                     
         'TempDelta(1)': float,
         'TempDelta(2)': float,
         'TempDelta(3)': float,
         'TempDelta(4)': float,
         'TempDelta(5)': float,
         'TempDelta(6)': float,
         'TempDelta(7)': float,
         'TempDelta(8)': float,
         'TempDelta(9)': float,
         'TempDelta(10)': float,
         'TempDelta(11)': float,
         'TempDelta(12)': float,
         'TempDelta(13)': float,
         'TempDelta(14)': float,
         'TempDelta(15)': float,
         'TempDelta(16)': float
        }

        df = pd.read_csv(fileName,
                         #  index_col=False,
                         index_col='TIMESTAMP',
                         #   drop=False,
                         #  parse_dates=True,
                         header=1,
                         # dtype = {'DMM_Calc': np.float64},
                         dtype=dataType,
                         # names=['Employee', 'Hired','Salary', 'Sick Days'],
                         usecols=[i for i in cols if i != 'RECORD'],
                         skiprows=[2, 3],
                         na_values='NAN',
                         #  nrows=2,
                         )

        # remove TS and Nan rows
        # df = df.drop(['TS', math.nan])
        # df = df.drop([0, 1])

        # print(df)

        field_keys = list(df.columns)
        # print(field_keys)

        # cols = [5,6,7]
        # df.drop(df.columns[cols],axis=1,inplace=True)

        # df = df.drop([5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20], axis=1)
        # print(df)
        # df['TIMESTAMP'].astype('int64')//1e9

        # convert date string to datetime obj
        # df[KOLOM_DATE_TIME] = pd.to_datetime(df[KOLOM_DATE_TIME]) - timedelta(hours=0)

        df.index = pd.to_datetime(df.index) - timedelta(hours=TIMEZONE)

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

            # DATABASE = 'SUNGAIPUTE'
            client = InfluxDBClient(
                url=URL,
                token=TOKEN,
                org=ORG
            )

            write_api = client.write_api(write_options=SYNCHRONOUS)

            # client = InfluxDBClient(host='mydomain.com', port=8086, username='myuser', password='mypass' ssl=True, verify_ssl=True)
            # client.drop_database(DATABASE)
            # client.create_database(DATABASE)
            # client.get_list_database()
            # client.switch_database(DATABASE)

            # write_points(dataframe, measurement, tags=None, tag_columns=None, field_columns=None, time_precision=None, database=None, retention_policy=None, batch_size=None, protocol=u'line', numeric_precision=None)
            # measurement – name of measurement
            # tags – dictionary of tags, with string key-values
            # tag_columns – [Optional, default None] List of data tag names
            # field_columns – [Options, default None] List of data field names
            # time_precision – [Optional, default None] Either ‘s’, ‘ms’, ‘u’ or ‘n’.
            # batch_size (int) – [Optional] Value to write the points in batches instead of all at one time. Useful for when doing data dumps from one database to another or when doing a massive write operation
            # protocol – Protocol for writing data. Either ‘line’ or ‘json’.
            # numeric_precision – Precision for floating point values. Either None, ‘full’ or some int, where int is the desired decimal precision. ‘full’ preserves full precision for int and float datatypes. Defaults to None, which preserves 14-15 significant figures for float and all significant figures for int datatypes.

            # client.write_points(
            #     df, # dataframe
            #     sensorType, # measurement
            #     database=DATABASE,
            #     # tags,
            #     # tag_columns=,
            #     field_columns=field_keys,
            #     time_precision='s',
            #     # batch_size=,
            #     # protocol='line',
            #     # numeric_precision=None
            # )

            write_api.write(
                bucket=BUCKET,
                record=df,
                data_frame_measurement_name=sensorType
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

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC_LWT)
    client.subscribe(MQTT_TOPIC_1)
    client.subscribe(MQTT_TOPIC_2)

def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic + ' ' + str(msg.payload))
    # sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    # if sensor_data is not None:
    #     _send_sensor_data_to_influxdb(msg.topic, sensor_data)
        # print('Data sent to influxdb')

def main():
    # mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    # mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    # mqtt_client.on_connect = on_connect
    # mqtt_client.on_message = on_message

    # mqtt_client.connect(MQTT_ADDRESS, 1883)
    # mqtt_client.loop_forever()
    
    # Watch manager (stores watches, you can add multiple dirs)
    wm = pyinotify.WatchManager()
    # User's music is in /tmp/music, watch recursively
    wm.add_watch(localFolder, pyinotify.ALL_EVENTS, rec=True)

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
    print('SUNGAIPUTE started')
    # print(URL, TOKEN)
    main()
    # DropboxDownload()
    print('HEHEHEHEH')
    # logging.info('HEHEHEHEH')
