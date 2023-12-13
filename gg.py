'''This is the websocket relay script. It should be run 
constantly in the background during the trial

@author

Kate Turley'''

from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlopen
import time
import os
from multiprocessing import parent_process, Process
from unittest import result
#from this import d
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import statistics
from kalman import SingleStateKalmanFilter
from moving_average import MovingAverageFilter
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
from casambi import Casambi
import timeit
import pytz
from metrics import Metrics
import logging
# import necessary libraries and classes
import sqlite3

from ctypes import wstring_at
from pydoc import doc
from unittest.mock import NonCallableMagicMock
from urllib import response
from urllib.request import HTTPBasicAuthHandler
from xml.sax.xmlreader import Locator
import websocket
import websockets
import time
import json
import ssl
import logging
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
from datetime import timedelta
from dateutil.tz import tzutc, tzlocal
import os
import threading
import statistics
import schedule
import threading
from casambi import Casambi
import re
from email import message
from pyexpat.errors import messages
from types import coroutine
import numpy as np
from pylab import ylim, title, ylabel, xlabel
from kalman import SingleStateKalmanFilter
from moving_average import MovingAverageFilter
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from metrics import Metrics
import multiprocessing
from multiprocessing import Pool
from queue import Queue
import concurrent.futures

utc = pytz.UTC

#get credentials from chroma config file
def get_dbDetails():
    dict_db = {}
    config = ConfigParser()
    config.read("configChroma.ini")
    dict_db['DB_api_key'] = config['dbDetails']['api_key']
    dict_db['DB_email'] = config['dbDetails']['email']
    dict_db['DB_user_password'] = config['dbDetails']['user_password']
    dict_db['DB_network_password'] = config['dbDetails']['network_password']
    dict_db['DB_token'] = config['dbDetails']['token']
    dict_db['DB_org'] = config['dbDetails']['org']
    dict_db['DB_bucket'] = config['dbDetails']['bucket']
    dict_db['DB_session'] = config['dbDetails']['session']
    dict_db['DB_id'] = config['dbDetails']['id']
    dict_db['DB_sql_path'] = config['dbDetails']['sql_path']
    #print(dict_db)
    return dict_db

#parse credentials to access data remotely
   
api_key = get_dbDetails()['DB_api_key']
email = get_dbDetails()['DB_email']
user_password = get_dbDetails()['DB_user_password']
network_password = get_dbDetails()['DB_network_password']
token = get_dbDetails()['DB_token']
org = get_dbDetails()['DB_org']
bucket =get_dbDetails()['DB_bucket']
session = get_dbDetails()['DB_session']
id = get_dbDetails()['DB_id']
sql_path = get_dbDetails()['DB_sql_path']



# instantiate casambi API
demo = Casambi(api_key=api_key,
               email=email,
               user_password=user_password,
               network_password=network_password,
               wire_id=1)



data_init = {"method": "open", "type": 1,
             "session": session,
             "id": id,
             "wire": 1}


connection = sqlite3.connect(sql_path)
cursor = connection.cursor()

query_sql = pd.read_sql_query('''
                               SELECT
                               *
                               FROM casambi_relation
                               ''', connection)
df = pd.DataFrame(query_sql, columns=[
                  'group_id', 'unit_id', 'unit_name', 'fixture_id', 'address', 'group'])


# this provides the relation between the addresses, unit ids, groups, names, fixture ids etc
#cas_rel = demo.casambi_relation()
cas_rel = df

print('CASR EL', cas_rel)
# call the sensor id/name/room relation
#cas_rel = demo.casambi_relation()

# only isolate room names in a list
room_names = cas_rel['group'].unique()


# only isolate addresses which correspond to mm wave sensors (not presence deteectors/switches/ASDs etc)
sensor_addresses = cas_rel['address'].loc[cas_rel['fixture_id']== 22328].to_list()

# on each message in the websocket receives...


def on_message(ws, message):
    try:
        response = json.loads(message)
       # print(response)
    except:
        pass

    points = []
    global covered

    # get the room name for sensor that has fired to enrich the data
    try:
        room_cas = cas_rel['group'].loc[cas_rel['unit_id']== response['id']].iloc#[0] #note you dont need the 0 indexer if only one sensor
    except:
        pass
    if response['address'] in sensor_addresses:
        # try :

        with InfluxDBClient(url="http://localhost:8086", token=token, org=org) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            body = Point("sensors") .tag("_address", response['address'])  .field("location", response['name']) .tag("_room", room_cas) .field(
                "x", response['sensors']['sensor_xPos1']['value']) .field("y", response['sensors']['sensor_yPos1']['value']) .field("z", response['sensors']['sensor_zPos1']['value'])
            points.append(body)
            body = Point("sensors") .field("location", response['name'])   .tag("_room", room_cas) .field(
                "multiple_targets", response['sensors']['sensor_MultipleTargets']['value'])
            points.append(body)
            body = Point("sensors") .tag("_address", response['address'])  .field("location", response['name']) .tag("_room", room_cas) .field(
                "x", response['sensors']['sensor_xPos2']['value']) .field("y", response['sensors']['sensor_yPos2']['value']) .field("z", response['sensors']['sensor_zPos2']['value'])
            points.append(body)
            body = Point("sensors") .field("location", response['name'])   .tag("_room", room_cas) .field(
                "multiple_targets", response['sensors']['sensor_MultipleTargets']['value'])
            points.append(body)
            body = Point("sensors") .tag("_address", response['address'])  .field("location", response['name']) .tag("_room", room_cas) .field(
                "x", response['sensors']['sensor_xPos3']['value']) .field("y", response['sensors']['sensor_yPos3']['value']) .field("z", response['sensors']['sensor_zPos3']['value'])
            points.append(body)
            body = Point("sensors") .field("location", response['name'])   .tag("_room", room_cas) .field(
                "multiple_targets", response['sensors']['sensor_MultipleTargets']['value'])
            points.append(body)
            body = Point("sensors") .tag("_address", response['address'])  .field("location", response['name']) .tag("_room", room_cas) .field(
                "x", response['sensors']['sensor_xPos4']['value']) .field("y", response['sensors']['sensor_yPos4']['value']) .field("z", response['sensors']['sensor_zPos4']['value'])
            points.append(body)
            body = Point("sensors") .field("location", response['name'])   .tag("_room", room_cas) .field(
                "multiple_targets", response['sensors']['sensor_MultipleTargets']['value'])
            points.append(body)
            write_api.write(bucket, org, points)

        # except :
         #   print('Cannot send points to database')
          #  logging.info('Cannot send points to database')


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")
    logging.info('Websocket connection has been closed')
    flag = 0
    while flag == 0:
        try:
            time.sleep(5.0)
            ws.run_forever()
            flag == 1
        except:
            print('Cannot re-connect to websocket after closure')


def on_open(ws):

    # try:
    ws.send(json.dumps(data_init))
    # except:
    #print('cannot send open data to open the websocket connection')


ws = websocket.WebSocketApp('wss://door.casambi.com/v1/bridge/',
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close,
                            header={"Sec-WebSocket-Protocol": api_key})


def main():
    # try:
    ws.run_forever()
    # except:
    #   pass


if __name__ == '__main__':

    main()
