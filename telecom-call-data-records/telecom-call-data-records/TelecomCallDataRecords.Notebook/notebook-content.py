# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "21997ac1-5ea0-4f9d-9b1a-1d6e9c6cfd9e",
# META       "default_lakehouse_name": "CDR_Lakehouse",
# META       "default_lakehouse_workspace_id": "79a3f7b0-348e-42c8-8c14-f3bb8f528c3c",
# META       "known_lakehouses": [
# META         {
# META           "id": "21997ac1-5ea0-4f9d-9b1a-1d6e9c6cfd9e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Call Data Records (CDR) Generator for Azure Event Hub
# 
# This notebook generates streaming CDR data with the following fields:
# - `call_id`: Unique identifier for each call
# - `subscriber_id`: Customer identifier
# - `call_start_time`: When the call started
# - `call_end_time`: When the call ended
# - `duration_seconds`: Call duration
# - `call_type`: Voice, Video, or Data
# - `status`: Completed, Failed, or Dropped
# - `tower_latitude` / `tower_longitude`: Nearest tower location (within South Africa)
# - `call_quality`: Quality score (1-10)

# CELL ********************

! python --version
! pip install azure-eventhub==5.11.5 --upgrade --force --quiet
! pip install semantic-link-labs --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from azure.eventhub import EventHubProducerClient, EventData
import os
import socket
import random
import datetime
import uuid
import time
from datetime import datetime, timedelta, timezone
from random import randrange
import sempy_labs as labs
import sempy_labs.variable_library as sempy_variable_library
import sempy_labs.eventstream as sempy_eventstream

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eventstream = "TelecomCallDataRecordsStream"
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
# META   "language_group": "synapse_pyspark"
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
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.



# South Africa geographic boundaries
# Latitude: approximately -22 to -35 (southern hemisphere)
# Longitude: approximately 16 to 33

SA_LAT_MIN = -35.0
SA_LAT_MAX = -22.0
SA_LON_MIN = 16.0
SA_LON_MAX = 33.0

# CDR Configuration
CALL_TYPES = ["Voice", "Video", "Data"]
STATUS_OPTIONS = ["Completed", "Failed", "Dropped"]
STATUS_WEIGHTS = [0.85, 0.08, 0.07]  # 85% completed, 8% failed, 7% dropped

def generate_cdr():
    """Generate a single Call Data Record"""

    # Use timezone-aware UTC datetime
    call_start = datetime.now(timezone.utc)
    duration_seconds = random.randint(5, 3600)  # 5 seconds to 1 hour
    call_end = call_start + timedelta(seconds=duration_seconds)

    # Generate tower location within South Africa
    tower_latitude = round(random.uniform(SA_LAT_MIN, SA_LAT_MAX), 6)
    tower_longitude = round(random.uniform(SA_LON_MIN, SA_LON_MAX), 6)

    # Generate status with weighted probability
    status = random.choices(STATUS_OPTIONS, weights=STATUS_WEIGHTS, k=1)[0]

    # Adjust duration for failed/dropped calls
    if status == "Failed":
        duration_seconds = 0
        call_end = call_start
    elif status == "Dropped":
        duration_seconds = random.randint(5, duration_seconds // 2) if duration_seconds > 10 else duration_seconds
        call_end = call_start + timedelta(seconds=duration_seconds)

    # Quality tends to be lower for dropped calls
    if status == "Dropped":
        call_quality = random.randint(1, 5)
    elif status == "Failed":
        call_quality = random.randint(1, 3)
    else:
        call_quality = random.randint(1, 10)

    cdr = {
        "call_id": str(uuid.uuid4()),
        "subscriber_id": f"SUB-{random.randint(100000, 999999)}",
        "call_start_time": call_start.isoformat().replace("+00:00", "Z"),
        "call_end_time": call_end.isoformat().replace("+00:00", "Z"),
        "duration_seconds": duration_seconds,
        "call_type": random.choice(CALL_TYPES),
        "status": status,
        "tower_latitude": tower_latitude,
        "tower_longitude": tower_longitude,
        "call_quality": call_quality
    }

    return cdr

# Test generating a single CDR
sample_cdr = generate_cdr()
print("Sample CDR:")
print(json.dumps(sample_cdr, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def send_cdr_to_eventhub(cdr_data):
    """Send a single CDR to Event Hub"""
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )
    
    try:
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(cdr_data)))
        producer.send_batch(event_data_batch)
        print(f"Sent CDR: {cdr_data['call_id']} | Status: {cdr_data['status']} | Quality: {cdr_data['call_quality']}")
    finally:
        producer.close()

def stream_cdrs_to_eventhub(records_per_second=1, total_records=None, duration_seconds=None):
    """
    Stream CDR data to Event Hub
    
    Args:
        records_per_second: Number of CDRs to generate per second
        total_records: Total number of records to send (None for unlimited)
        duration_seconds: Duration to run in seconds (None for unlimited)
    """
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )
    
    start_time = time.time()
    records_sent = 0
    interval = 1.0 / records_per_second
    
    print(f"Starting CDR streaming at {records_per_second} records/second...")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            # Check stop conditions
            if total_records and records_sent >= total_records:
                print(f"\nReached target of {total_records} records. Stopping.")
                break
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                print(f"\nReached duration of {duration_seconds} seconds. Stopping.")
                break
            
            # Generate and send CDR
            cdr = generate_cdr()
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(cdr)))
            producer.send_batch(event_data_batch)
            
            records_sent += 1
            print(f"[{records_sent}] Call ID: {cdr['call_id'][:8]}... | "
                  f"Type: {cdr['call_type']:6} | Status: {cdr['status']:9} | "
                  f"Quality: {cdr['call_quality']:2} | "
                  f"Location: ({cdr['tower_latitude']:.4f}, {cdr['tower_longitude']:.4f})")
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\n\nStreaming stopped by user. Total records sent: {records_sent}")
    finally:
        producer.close()
        
    return records_sent

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

def stream_cdrs_to_eventhub_with_send(records_per_second=1, total_records=None, duration_seconds=None, producer=None):
    """
    Stream CDR data to Event Hub using the sendToEventsHub function.

    Args:
        records_per_second: Number of CDRs to generate per second
        total_records: Total number of records to send (None for unlimited)
        duration_seconds: Duration to run in seconds (None for unlimited)
        producer: An instance of EventHubProducerClient (must be provided)
    """
    if producer is None:
        raise ValueError("You must supply a valid EventHubProducerClient instance as 'producer'.")

    start_time = time.time()
    records_sent = 0
    interval = 1.0 / records_per_second

    print(f"Starting CDR streaming at {records_per_second} records/second...")
    print("Press Ctrl+C to stop\n")

    try:
        while True:
            # Check stop conditions
            if total_records and records_sent >= total_records:
                print(f"\nReached target of {total_records} records. Stopping.")
                break
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                print(f"\nReached duration of {duration_seconds} seconds. Stopping.")
                break

            # Generate and send CDR
            cdr = generate_cdr()
            sendToEventsHub(cdr, producer)
            records_sent += 1
            print(f"[{records_sent}] Call ID: {cdr['call_id'][:8]}... | "
                  f"Type: {cdr['call_type']:6} | Status: {cdr['status']:9} | "
                  f"Quality: {cdr['call_quality']:2} | "
                  f"Location: ({cdr['tower_latitude']:.4f}, {cdr['tower_longitude']:.4f})")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n\nStreaming stopped by user. Total records sent: {records_sent}")

    return records_sent

# Example usage: (Assumes 'producer_events' is your initialized EventHubProducerClient)
stream_cdrs_to_eventhub_with_send(records_per_second=5, duration_seconds=1000, producer=producer_events)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
