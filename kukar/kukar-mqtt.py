import os
from dotenv import load_dotenv, dotenv_values
load_dotenv('../.env')  # take environment variables from .env.
config = dotenv_values('../.env')
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

TIMEZONE='Asia/Makassar'

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

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC_LWT)
    client.subscribe(TOPIC_DMM_1s)
    client.subscribe(MQTT_TOPIC_2)

def _parse_mqtt_message(topic, payload):
    if re.match(MQTT_TOPIC_LWT, topic):
        return MqttStatus(str(payload))
    elif "data" in topic and topic.startswith("cs/v1/") and topic.endswith("/cj"):
    # elif topic.startswith("cs/v1/data") and topic.endswith("/cj") and "LVDT_1s" in topic:
        y = json.loads(payload)

        # parse datalogger detail
        station_name = y["head"]['environment']['station_name']
        table_name = y['head']['environment']['table_name']
        model = y['head']['environment']['model']
        serial_no = y['head']['environment']['serial_no']
        os_version = y['head']['environment']['os_version']
        prog_name = y['head']['environment']['prog_name']
        
        # print(y["head"]['fields'])
        keys = []
        for k in y["head"]['fields']:
            for v,z in k.items():
                if v == 'name':
                    # print(z)
                    keys.append(z)
        #     if k == 'name':
        #         my_list.append(v)
        print(keys)
        
        # parse sensor data
        ts = y["data"][0]['time']       
        
        timeKeys = []
        valsKeys = []
        combined = []
        
        for k in y["data"]:
            for v,z in k.items():
                if v == 'time':
                    
                    strLen = len(z)
                    if strLen > 19:
                        A=26-strLen
                        if A==1: z = z + '0'                
                        elif A==2: z = z + '00'
                        elif A==3: z = z + '000'
                        elif A==4: z = z + '0000'
                        elif A==5: z = z + '00000'
                            
                    receivedTs = datetime.fromisoformat(z)        
                    
                    ## Adding a timezone
                    tzOrigin = pytz.timezone(TIMEZONE)
                    tzServer = pytz.timezone("Asia/Jakarta") 
                    # tzServer = pytz.timezone("UTC")
                    
                    # original timestamp yg diterima tidak ada info timezone
                    # kita lekatkan timezone Makassar ke original timestamp
                    # source: https://www.includehelp.com/python/datetime-astimezone-method-with-example.aspx
                    dtOrigin = receivedTs.astimezone(tzOrigin)        
                    dtConverted = dtOrigin.astimezone(tzServer)
                    
                    unixTs = dtConverted.timestamp()
                    unixTs = math.floor(unixTs)-28800
                    # d1=datetime.datetime(z, sgtTZObject)
                    # d2 = d1.astimezone(sgtTZObject)
                    
                    timeKeys.append(unixTs*1000000000)
                if v == 'vals':                    
                    for i in range(len(z)):
                        z[i]=float(z[i])
                    valsKeys.append(z)
                    testDict = dict(map(lambda i,j : (i,j) , keys,z))
                    combined.append(testDict)
        
        # dict_structure=[]
        # for i in range(len(combined)):
        #     dict_structure.append({'fields':combined[i]})

        # for i in range(len(dict_structure)):
        #     dict_structure[i]['measurement']=station_name
        #     dict_structure[i]['time']=timeKeys[i]
            
        dict_structure=[]
        for i in range(len(timeKeys)):
            dict_structure.append({'measurement':station_name})
            dict_structure[i]['tags']={'table':table_name}
            dict_structure[i]['time']=timeKeys[i]
            dict_structure[i]['fields']=combined[i]
        
        print() 
        print(dict_structure)
        print()   

        # print(json.dumps(json_body, indent=4))
        return dict_structure

    elif topic.startswith("cs/v1/state"):
        print(topic)

    elif re.match(TOPIC_DMM_1s, topic):
        # print('Message match')
        # voltage = math.nan
        # ampere = math.nan
        # watt = math.nan
        # pstkwh = math.nan
        # heap = math.nan
        y = json.loads(payload)
        if "data" in y :
            timestamp = y["data"][0]['time']
            val= y["data"][0]['vals']
            # print('DMM_1s', timestamp, val)
    else:
        return None

def _send_sensor_data_to_influxdb(topic, dict_points):
    # if "LVDT_1s" in topic:
    if "data" in topic and topic.startswith("cs/v1/") and topic.endswith("/cj"):
        write_api.write(BUCKET_1_HOUR, ORG, dict_points)
        # write_api.write(BUCKET_1_HOUR, ORG, dict_points,WritePrecision = 's')
        # write_api.write(BUCKET_AUTOGEN, ORG, dict_points)

def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    # print(msg.topic + ' ' + str(msg.payload))
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data is not None:
        _send_sensor_data_to_influxdb(msg.topic, sensor_data)
        # print(msg.topic, sensor_data)
        # print('Data sent to influxdb')

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