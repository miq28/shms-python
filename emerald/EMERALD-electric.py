import os
import sys
from sys import stdout, stderr
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from dotenv import load_dotenv
load_dotenv('../.env')  # take environment variables from .env.
import datetime
from datetime import datetime
import pytz
import re
import json
import math
from typing import NamedTuple

import paho.mqtt.client as mqtt
# from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS
import logging_config
import logging

class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    blue = "\x1b[34m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    # format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    format = "%(asctime)s-%(levelname)s-%(message)s-(%(filename)s:%(lineno)d)"
    

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
logging.root.handlers = []
out_stream_handler = logging.StreamHandler(stdout)
out_stream_handler.setLevel(logging.DEBUG)
out_stream_handler.addFilter(lambda record: record.levelno <= logging.INFO)
# out_stream_handler.setFormatter(CustomFormatter())

err_stream_handler = logging.StreamHandler(stderr)
err_stream_handler.setLevel(logging.WARNING)
# err_stream_handler.setFormatter(CustomFormatter())

logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S %Z",
    format='%(asctime)s - %(levelname)s - %(message)s - (%(filename)s:%(lineno)d)',
    handlers=[out_stream_handler, err_stream_handler]
)

# add the handler to the root logger
logging.getLogger('')

# logging.debug('debug')
# logging.info('info')
# logging.warning('warning')
# logging.error('error')
# logging.exception('exp')

logger = logging.getLogger(os.path.basename(__file__))
# logger.setLevel(logging.INFO)

URL = os.getenv("URL_EMERALD")
TOKEN = os.getenv('TOKEN_EMERALD')
ORG = os.getenv('ORG_EMERALD')
BUCKET_AUTOGEN = os.getenv('BUCKET_AUTOGEN_EMERALD')
BUCKET_1_HOUR = os.getenv('BUCKET_1_HOUR_EMERALD')

MQTT_ADDRESS = os.getenv("MQTT_ADDRESS_EMERALD")
MQTT_USER = os.getenv("MQTT_USER_EMERALD")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD_EMERALD")

MQTT_TOPIC_LWT = 'ENERGYMETER_03F245/mqttstatus'
MQTT_TOPIC_1 = 'ENERGYMETER_03F245/meterreading/1s'
MQTT_TOPIC_2 = 'ENERGYMETER_03F245/meterreading/60s'

MQTT_CLIENT_ID = 'emerald_electric'

TIMEZONE='Asia/Jakarta'


client = InfluxDBClient(
    url=URL,
    token=TOKEN,
    org=ORG
)

write_api = client.write_api(write_options=SYNCHRONOUS)

class MqttStatus(NamedTuple):
    mqttstatus: str
    
class SensorData(NamedTuple):
    voltage: float
    ampere: float
    watt: float
    pstkwh: float
    heap: float
    
# function to check for JSON String validity
def is_validJSON(json_string):
    logger.debug("JSON String: %s", json_string)
    try:
        return json.loads(json_string)
    except ValueError as v:
        logger.warning('%s, JSON is not valid [%s]', v, json_string)
        return None

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    logger.info('Connected with result code %i', rc)
    client.subscribe(MQTT_TOPIC_LWT)
    client.subscribe(MQTT_TOPIC_1)
    client.subscribe(MQTT_TOPIC_2)

def _parse_mqtt_message(topic, payload):
    if re.match(MQTT_TOPIC_LWT, topic):
        return MqttStatus(str(payload))
    elif re.match(MQTT_TOPIC_1, topic) or re.match(MQTT_TOPIC_2, topic):
        # print('Message match')
        voltage = math.nan
        ampere = math.nan
        watt = math.nan
        pstkwh = math.nan
        heap = math.nan
        
        y = is_validJSON(payload)
        
        if y is None:
            logger.warning('Payload is not valid JSON')
            return None
        
        if "voltage" in y : voltage = float(y["voltage"])
        if "ampere" in y : ampere = float(y["ampere"])
        if "watt" in y : watt = float(y["watt"])
        if "pstkwh" in y : pstkwh = float(y["pstkwh"])
        if "heap" in y : heap = float(y["heap"])
        return SensorData(voltage, ampere, watt, pstkwh, heap)
    else:
        return None

def _send_sensor_data_to_influxdb(topic, sensor_data):
    # create datetime object
    local = datetime.now() # UTC
    tz_Jakarta = pytz.timezone(TIMEZONE) 
    now = datetime.now(tz_Jakarta)
    
    # print(sensor_data)
    if re.match(MQTT_TOPIC_LWT, topic):
        json_body = [
            {
                'measurement': 'emerald_electric',
                'tags': {
                    'param': 'mqttstatus'
                },
                'fields': {
                    'mqttstatus': sensor_data.mqttstatus
                }
            }
        ]
        # write_api.write(BUCKET_AUTOGEN, ORG, json_body)
        bucket_name = BUCKET_AUTOGEN

    elif re.match(MQTT_TOPIC_1, topic) or re.match(MQTT_TOPIC_2, topic):
        # print(sensor_data)
        json_body = [
            {
                'measurement': 'emerald_electric',
                'tags': {
                    'param': 'voltage',
                    'year': now.year,
                    'month': now.month,
                    'monthStr': now.strftime("%b"),
                    'day': now.strftime("%w"),
                    'dayStr': now.strftime("%a"),
                    'week': now.strftime("%W"),
                },
                'fields': {
                    'value': float(sensor_data.voltage)
                }
            },
            {
                'measurement': 'emerald_electric',
                'tags': {
                    'param': 'current',
                    'year': now.year,
                    'month': now.month,
                    'monthStr': now.strftime("%b"),
                    'day': now.strftime("%w"),
                    'dayStr': now.strftime("%a"),
                    'week': now.strftime("%W"),
                },
                'fields': {
                    'value': float(sensor_data.ampere)
                }
            },
            {
                'measurement': 'emerald_electric',
                'tags': {
                    'param': 'power',
                    'year': now.year,
                    'month': now.month,
                    'monthStr': now.strftime("%b"),
                    'day': now.strftime("%w"),
                    'dayStr': now.strftime("%a"),
                    'week': now.strftime("%W"),
                },
                'fields': {
                    'value': float(sensor_data.watt)
                }
            },
            {
                'measurement': 'emerald_electric',
                'tags': {
                    'param': 'kWh',
                    'year': now.year,
                    'month': now.month,
                    'monthStr': now.strftime("%b"),
                    'day': now.strftime("%w"),
                    'dayStr': now.strftime("%a"),
                    'week': now.strftime("%W"),
                },
                'fields': {
                    'value': float(sensor_data.pstkwh)
                }
            },
            {
                'measurement': 'emerald_electric',
                'tags': {
                    'param': 'heap',
                    'year': now.year,
                    'month': now.month,
                    'monthStr': now.strftime("%b"),
                    'day': now.strftime("%w"),
                    'dayStr': now.strftime("%a"),
                    'week': now.strftime("%W"),
                },
                'fields': {
                    'value': float(sensor_data.heap)
                }
            }
        ]
        if re.match(MQTT_TOPIC_1, topic):
            bucket_name = BUCKET_1_HOUR
        elif re.match(MQTT_TOPIC_2, topic):
            bucket_name = BUCKET_AUTOGEN
            
    try:
        logger.debug('%s', topic)
        write_api.write(bucket_name, ORG, json_body)
    except Exception as e:
        logger.error('%e', e)

def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    logger.debug('message=[%s] payload=[%s]', msg.topic, str(msg.payload.decode('utf-8')))
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data is not None:
        _send_sensor_data_to_influxdb(msg.topic, sensor_data)

def main():
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


try:
    # print('%s started' % os.path.basename(__file__))
    logger.info('%s started', os.path.basename(__file__))
    # asyncio.run(main())
    main()
except KeyboardInterrupt:
    # print('shutting down')
    logger.info('KeyboardInterrupt, shutting down...')
except Exception as e:
    logger.error("Exception occurred", exc_info=True)
