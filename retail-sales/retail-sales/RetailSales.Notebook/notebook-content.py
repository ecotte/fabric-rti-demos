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
# META     },
# META     "event_stream": {
# META       "known_event_streams": [
# META         {
# META           "artifact_id": "94ccdfc4-5c17-4d91-9371-c5f9fb83b8fc",
# META           "stream_id": "94ccdfc4-5c17-4d91-9371-c5f9fb83b8fc"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

! python --version
! pip install azure-eventhub==5.11.5 --upgrade --force --quiet
! pip install semantic-link-labs --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
from azure.eventhub import EventHubProducerClient, EventData
import os
import socket
import uuid
import datetime
import random
from random import randrange
import sempy_labs as labs
import sempy_labs.variable_library as sempy_variable_library
import sempy_labs.eventstream as sempy_eventstream

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

eventstream = "RetailSalesStream"
eventstream_source_name = "CustomEndpoint-Source"

es_topology = sempy_eventstream.get_eventstream_topology(eventstream=eventstream)
es_source = es_topology[es_topology["Eventstream Source Name"] == eventstream_source_name]
es_source_id = es_source["Eventstream Source Id"].iloc[0]

es_source_connection = sempy_eventstream.get_eventstream_source_connection(eventstream=eventstream,source_id=es_source_id)
es_eventhub_name = es_source_connection["EventHub Name"].iloc[0]
es_eventhub_connstring = es_source_connection["Primary Connection String"].iloc[0]

producer_events = EventHubProducerClient.from_connection_string(conn_str=es_eventhub_connstring, eventhub_name=es_eventhub_name)

hostname = socket.gethostname()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

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

# CELL ********************

def generate_sale():
    guid = uuid.uuid4()
    lines = random.randint(1, 10)+1
    now = int(datetime.datetime.now().timestamp())
    saleLines = []
    customerKey = random.randint(1,50)
    storeKey = random.randint(1,50)


    for line in range(1,lines):
        product = random.randint(1,2517)
        quantity = random.randint(1, 10)
        cost = float(random.randint(10,500))
        price = float(random.randint(1,500))
        lineObj = {
            'LineNumber': line,
            'ProductKey': product,
            'Quantity': quantity,
            'UnitPrice': price,
            'NetPrice': price,
            'UnitCost': cost,
            'CurrencyCode': 'USD',
            'ExchangeRate': 1
        }
        saleLines.append(lineObj)

    sale = {
        'OrderKey': str(guid),
        'OrderDate': now,
        'DeliveryDate': now,
        'CustomerKey': int(customerKey),
        'StoreKey': int(storeKey),
        'lines': saleLines
    }


    saleJson = json.dumps(sale)

    return saleJson

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def sendToEventsHub(jsonEvent, producer,process_id):
    eventString = jsonEvent
    event_data_batch = producer.create_batch() 
    event_data_batch.add(EventData(eventString)) 
    producer.send_batch(event_data_batch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

while(True):
    sale = generate_sale()
    sendToEventsHub(jsonEvent=sale,producer=producer_events,process_id=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
