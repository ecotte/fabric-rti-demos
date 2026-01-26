# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Install Event Hub packages

# CELL ********************

! python --version
! pip install azure-eventhub==5.11.5 --upgrade --force --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Connect to Event Stream

# CELL ********************

import json
from azure.eventhub import EventHubProducerClient, EventData
import os
import socket
import random

from random import randrange

variableLibrary = notebookutils.variableLibrary.getLibrary("HealthcareBillingSystem")

#EventHubOriginal
eventHubName = variableLibrary.eventHubName 
eventHubConnString = variableLibrary.eventHubConnString
producer_events = EventHubProducerClient.from_connection_string(conn_str=eventHubConnString, eventhub_name=eventHubName)

hostname = socket.gethostname()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Send events to Event Stream

# CELL ********************

def sendToEventsHub(jsonEvent, producer):
    eventString = json.dumps(jsonEvent)
    #print(eventString) 
    event_data_batch = producer.create_batch() 
    event_data_batch.add(EventData(eventString)) 
    producer.send_batch(event_data_batch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# # Wrapper to generate events and send to event Hub

# CELL ********************

Hospital_Departments = {
  "Cardiology": {
    "department_code": "DGBW"
  },
  "Neurology": {
    "department_code": "H0RU"
  },
  "Oncology": {
    "department_code": "76VC"
  },
  "Pediatrics": {
    "department_code": "A9KZ"
  },
  "Radiology": {
    "department_code": "CR8E"
  },
  "Orthopedics": {
    "department_code": "VUG0"
  },
  "Emergency": {
    "department_code": "663P"
  },
  "Dermatology": {
    "department_code": "8450"
  },
  "Gastroenterology": {
    "department_code": "Z8IG"
  },
  "Urology": {
    "department_code": "72RR",
  }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************


# CELL ********************

import random, math, json
import random
import time
import uuid
from datetime import datetime, timedelta

class ContosoHospitalSimulator:
    def __init__(self, current_time_pas):
        ## self.grid_id = grid_id
        self.unique_id = str(uuid.uuid4())
        self.current_time = current_time_pas

    def log_event(self, activity, sensor_data):
        event = {
            "timestamp": self.current_time.strftime("%Y-%m-%d %H:%M:%S"),
            "event_id": self.unique_id,
            "source": activity,
            # "grid_id": grid_id,
            "sensor_data": sensor_data
        }
        log_event = event
        ##log_obj = json.dumps(log_event)
        #impressionEvent = json_object    
        sendToEventsHub(log_event, producer_events)
        #print(log_event)

    def get_sensor_data(self, source, Department, details):
        #sources = ["United Care-HMO", "Medicare", "Athens-HMO ", "Self Pay", "Workers Comp"]
        data = {}

        # for source in sources:
        Billed_Amount = round(random.uniform(300, 500) if random.random() < 1/1000 else random.uniform(100, 300), 0)

        # Ensure Collection Amount < Billed_Amount only 1 in 10 times
        if random.random() > 1/20:
            Collection_Amount = round(random.uniform(0.5 * Billed_Amount, Billed_Amount), 0)
        else:
            Collection_Amount = round(random.uniform(1.1 * Billed_Amount, 2 * Billed_Amount), 0)

        # Calculate price based on the difference
        percentage = round((Collection_Amount / Billed_Amount) * 100, 0)

        if percentage < 70:
            price = round(random.uniform(-10, 30), 0)
        elif percentage < 80:
            price = round(random.uniform(30, 50), 0)
        elif percentage < 90:
            price = round(random.uniform(50, 60), 0)
        elif percentage < 100:
            price = round(random.uniform(60, 100), 0)
        else:
            # print("greater")
            price = round(random.uniform((100 + ((percentage-100)/5)*10), 300), 0)

        if source in ["Self Pay", "Workers Comp"]:
            base_claims = 20
            claims_multiplier = math.floor(max((Billed_Amount - 50),0) / 10)
            Claims_Rejections = round(base_claims + claims_multiplier * 10, 0)    
        else:
            base_claims = 50
            claims_multiplier = math.floor(max((Billed_Amount - 50),0) / 10)
            Claims_Rejections = round(base_claims + claims_multiplier * 5, 0)

        #print(Billed_Amount, Collection_Amount, percentage, price, Claims_Rejections)

        data = {
            "Department": Department,
            "department_code":details['department_code'],
            "base_claims":base_claims,
            "Billed_Amount": Billed_Amount,
            "Collection_Amount": Collection_Amount,
            "Claims_Rejections": Claims_Rejections,
        }

        return data

def simulate_hospital_data():
    start_time = datetime.now() - timedelta(days=3)
    counter = 0

    while True:
        for Department, details in Hospital_Departments.items():
            current_time_pas = start_time + timedelta(seconds=80 * counter)
            grid = ContosoHospitalSimulator(current_time_pas)

            for src in ["United Care-HMO", "Medicare", "Athens-HMO ", "Self Pay", "Workers Comp"]:
                sensor_data = grid.get_sensor_data(src, Department, details)
                grid.log_event(src, sensor_data)

            counter += 1
            print(f"Counter: {counter}, Time: {current_time_pas}, Department: {Department}")

            # Determine if we need to sleep
            if current_time_pas >= datetime.now():
                time.sleep(80)
            else:
                time.sleep(1)
simulate_hospital_data()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
