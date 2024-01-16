import os
import sys
from dotenv import load_dotenv, dotenv_values
load_dotenv('../.env')  # take environment variables from .env.
config = dotenv_values('../.env')
import ciso8601
import datetime
from datetime import datetime, timezone
import pytz
import re
import json
import math
from typing import NamedTuple

import paho.mqtt.client as mqtt
# from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
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

# log = logging.getLogger("log")
# logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

URL = os.getenv("URL_KUKAR")
TOKEN = os.getenv('TOKEN_KUKAR')
ORG = os.getenv('ORG_KUKAR')
BUCKET_AUTOGEN = os.getenv('BUCKET_AUTOGEN_KUKAR')
BUCKET_1_HOUR = os.getenv('BUCKET_1_HOUR_KUKAR')

MQTT_ADDRESS = os.getenv("MQTT_ADDRESS_KUKAR")
MQTT_USER = os.getenv("MQTT_USER_KUKAR")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD_KUKAR")
# MQTT_TOPIC = 'home/+/+'
# MQTT_REGEX = 'home/([^/]+)/([^/]+)'
MQTT_TOPIC_LWT = 'ENERGYMETER_03F245/mqttstatus'
TOPIC_DMM_1s = 'cs/v1/data/cr1000x/50498/DMM_1s/cj'
MQTT_TOPIC_2 = 'cs/v1/#'
#MQTT_REGEX = 'ENERGYMETER_58C210/meterreading/60s'
MQTT_CLIENT_ID = 'kukar-py'

TZ_ORIGIN='Asia/Makassar'

client = InfluxDBClient(
    url=URL,
    token=TOKEN,
    org=ORG
)

write_api = client.write_api(write_options=SYNCHRONOUS)

class MqttStatus(NamedTuple):
    mqttstatus: str
    
class SensorData(NamedTuple):
    timestamp: str
    vals: list
    
# function to check for JSON String validity
def is_validJSON(json_string):
    # print("JSON String:", json_string)
    try:
        return json.loads(json_string)
    except ValueError:
        return None

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC_LWT)
    client.subscribe(TOPIC_DMM_1s)
    client.subscribe(MQTT_TOPIC_2)

def parseCSIJSON(topic, json_str):
    y = json_str
    # print(json.dumps(y, indent=2))

    # parse datalogger detail
    station_name = y["head"]['environment']['station_name']
    table_name = y['head']['environment']['table_name']
    model = y['head']['environment']['model']
    serial_no = y['head']['environment']['serial_no']
    os_version = y['head']['environment']['os_version']
    prog_name = y['head']['environment']['prog_name']
    
    MEASUREMENT={'measurement': station_name}    
    TAGS = y["head"]['environment']
    
    sensorKeys = []
    for k in y["head"]['fields']:
        for v,z in k.items():
            if v == 'name':
                # print(z)
                sensorKeys.append(z)
    # print(keys)
    
    # parse sensor data 
    timeKeys = []
    sensorVals = []
    FIELDS = []
    
    for k in y["data"]:
        for v,z in k.items():
            if v == 'time':
                # parse time string
                unaware = ciso8601.parse_datetime(z)                    
                # Create timezone object                    
                tzOriginObj = pytz.timezone(TZ_ORIGIN)                    
                ## Adding a timezone                    
                aware = tzOriginObj.localize(unaware)
                # insert tz aware datetime
                timeKeys.append(aware)
                
            if v == 'vals':                    
                for i in range(len(z)):
                    z[i]=float(z[i])
                sensorVals.append(z)
                testDict = dict(map(lambda i,j : (i,j) , sensorKeys,z))
                FIELDS.append(testDict)
        
    final_dict=[]    
    for i in range(len(timeKeys)):
        final_dict.append(MEASUREMENT.copy())
        final_dict[i]['tags']=TAGS
        final_dict[i]['time']=timeKeys[i]
        final_dict[i]['fields']=FIELDS[i]
    

    logger.debug(final_dict)

    # print(json.dumps(json_body, indent=4))
    return final_dict


# def _send_sensor_data_to_influxdb(dict_points, precision):
#     try:
#         write_api.write(BUCKET_1_HOUR, ORG, dict_points, WritePrecision=precision)
#         write_api.write(BUCKET_AUTOGEN, ORG, dict_points, WritePrecision=precision)
#     except Exception as e:
#         logger.error("Exception occurred", exc_info=True)

def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    # print(msg.topic + ' ' + str(msg.payload))

    topic=msg.topic
    payload=msg.payload
    data_to_send_to_db=None
    
    logger.info(topic)
    
    logger_sn = [
        1052, #kukar-box1
        50498 #kukar-box2
        ]
    
    if not any(str(i) in topic for i in logger_sn):
        logger.info('logger not in the list')
        return
    
    json_data = is_validJSON(payload.decode('utf-8'))
    
    if json_data is None:
        return
            
    if "data" in topic and topic.startswith("cs/v1/") and topic.endswith("/cj"):
        try:
            data_to_send_to_db = parseCSIJSON(topic, json_data)
            logger.info('Parsed %d data, size=%d', len(data_to_send_to_db), sys.getsizeof(data_to_send_to_db))
        except Exception as e:
            logger.error("Exception occurred, parseCSIJSON failed", exc_info=True)
            
    elif "state" in topic and topic.startswith("cs/v1/"):
        return
        
    elif re.match(MQTT_TOPIC_LWT, topic):
        return MqttStatus(str(payload))
    
    # else:
    #     return None
    if data_to_send_to_db is not None:
        # _send_sensor_data_to_influxdb(data_to_send_to_db, 's')
        try:
            write_api.write(BUCKET_1_HOUR, ORG, data_to_send_to_db, WritePrecision='s')
            write_api.write(BUCKET_AUTOGEN, ORG, data_to_send_to_db, WritePrecision='s')
        except Exception as e:
            logger.error("Exception occurred, send to influx failed", exc_info=True)

def main():


    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('Kukar MQTT started')
    main()