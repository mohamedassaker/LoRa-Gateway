import glob
import os
import csv
import json
import pandas as pd
import paho.mqtt.client as paho  		    #mqtt library
import time
import serial
import re
from datetime import datetime
import sys
import numpy as np

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

maxInt = sys.maxsize
while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

ACCESS_TOKEN = 'ULTRASONICSENSOR1'    #Token of your device
broker = "demo.thingsboard.io"   	  #host name
port = 1883 					      #data listening port

def on_publish(client,userdata,result):             #create function for callback
    print("data published to thingsboard \n")
    pass
client1= paho.Client("control1")                    #create client object
client1.on_publish = on_publish                     #assign function to callback
client1.username_pw_set(ACCESS_TOKEN)               #access token from thingsboard device
client1.connect(broker,port,keepalive=120)          #establish connection

#Get to the folder path which contains the files
list_of_files = glob.glob('/home/pi/LoRa/lora_gateway/lora_gateway/util_pkt_logger/*')

#Filter the folder to get csv files only
list_of_files_csv = [k for k in list_of_files if '.csv' in k]
#print("List of csv files")
print(list_of_files_csv)

#Get the latest csv file created
latest_csv_file = max(list_of_files_csv, key=os.path.getctime)
print("Latest file is", latest_csv_file)

last_sent_index = -1

##Loop
while(True):

    file = open(latest_csv_file)
    reader = csv.reader(file)
    lines= len(list(reader))
    # print("Lines = ",lines)

    if(lines > 1):
       #Read csv file
       df = pd.read_csv(latest_csv_file, delimiter="," , quotechar='"', header=0, engine="python", quoting=3, index_col=None)
       print (df)
       # print(df.columns, df.iloc[1])

       for i in range(len(df)):
           payload = df.iloc[i].to_dict()
           #print(type(payload))
           payload = json.dumps(payload, cls=NpEncoder)
           #print('.')
        
           #check if there is a new data to be send
           if(last_sent_index<i):
               #send to the thingsboard
               last_sent_index = i
               #show latest csv data
               print (df)
               ret= client1.publish("v1/devices/me/telemetry",payload) #topic-v1/devices/me/telemetry
               print("Please check LATEST TELEMETRY field of your device")
               #print(payload)
               time.sleep(5)
