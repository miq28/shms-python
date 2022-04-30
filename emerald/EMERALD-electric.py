import os
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

URL = os.getenv("URL_EMERALD")
TOKEN = os.getenv('TOKEN_EMERALD')
ORG = os.getenv('ORG_EMERALD')
BUCKET_AUTOGEN = os.getenv('BUCKET_AUTOGEN_EMERALD')
BUCKET_1_HOUR = os.getenv('BUCKET_1_HOUR_EMERALD')

MQTT_ADDRESS = os.getenv("MQTT_ADDRESS_EMERALD")
MQTT_USER = os.getenv("MQTT_USER_EMERALD")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD_EMERALD")
# MQTT_TOPIC = 'home/+/+'
# MQTT_REGEX = 'home/([^/]+)/([^/]+)'
MQTT_TOPIC_LWT = 'ENERGYMETER_58C210/mqttstatus'
MQTT_TOPIC_1 = 'ENERGYMETER_58C210/meterreading/1s'
MQTT_TOPIC_2 = 'ENERGYMETER_58C210/meterreading/60s'
#MQTT_REGEX = 'ENERGYMETER_58C210/meterreading/60s'
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

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
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
        y = json.loads(payload)
        if "voltage" in y : voltage = float(y["voltage"])
        if "ampere" in y : ampere = float(y["ampere"])
        if "watt" in y : watt = float(y["watt"])
        if "pstkwh" in y : pstkwh = float(y["pstkwh"])
        if "heap" in y : heap = float(y["heap"])
        return SensorData(voltage, ampere, watt, pstkwh, heap)
    else:
        return None

def _send_sensor_data_to_influxdb(topic, sensor_data):
    # date object of today's date
    local = datetime.now() # UTC
    tz_Jakarta = pytz.timezone(TIMEZONE) 
    now = datetime.now(tz_Jakarta)
    # print("Current year:", now.year)
    # print("Current month:", now.month)
    # print("Current monthStr:", now.strftime("%b")) 
    # print("Current day:", now.day)
    # print("Current hour:", now.hour)
    # print("UTC:", local.strftime("%m/%d/%Y, %H:%M:%S"))
    # print("Jakarta:", now.strftime("%m/%d/%Y, %H:%M:%S"))
    
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
        write_api.write(BUCKET_AUTOGEN, ORG, json_body)
        # print('LWT')
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
                    'value': sensor_data.voltage
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
                    'value': sensor_data.ampere
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
                    'value': sensor_data.watt
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
                    'value': sensor_data.pstkwh
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
                    'value': sensor_data.heap
                }
            }
        ]
        if re.match(MQTT_TOPIC_1, topic):
            # client.write_points(json_body, retention_policy='1_hour')
            # retention 1 hour
            write_api.write(BUCKET_1_HOUR, ORG, json_body)


            # print('1s')
        elif re.match(MQTT_TOPIC_2, topic):
            # retention ionfinite
            write_api.write(BUCKET_AUTOGEN, ORG, json_body)
            # print('60s')

def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    # print(msg.topic + ' ' + str(msg.payload))
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data is not None:
        _send_sensor_data_to_influxdb(msg.topic, sensor_data)
        # print('Data sent to influxdb')

def main():


    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('MQTT Emerald Electric')
    main()