# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "event_stream": {
# META       "known_event_streams": [
# META         {
# META           "artifact_id": "008588ec-c43a-45c8-bbd0-de7f87c721d8",
# META           "stream_id": "008588ec-c43a-45c8-bbd0-de7f87c721d8"
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
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Banking Loan Clickstream Fraud Analytics
# ## Synthetic Data Generator for Real-time Streaming

# CELL ********************

import json
from azure.eventhub import EventHubProducerClient, EventData
import os
import socket
import uuid
import datetime
import random
from random import randrange
import time
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List
import sempy_labs as labs
import sempy_labs.variable_library as sempy_variable_library
import sempy_labs.eventstream as sempy_eventstream

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eventstream = "BankingLoanFraud"
eventstream_source_name = "CustomEndpoint-Source"

es_topology = sempy_eventstream.get_eventstream_topology(eventstream=eventstream)
es_source = es_topology[es_topology["Eventstream Source Name"] == eventstream_source_name]
es_source_id = es_source["Eventstream Source Id"].iloc[0]

es_source_connection = sempy_eventstream.get_eventstream_source_connection(eventstream=eventstream,source_id=es_source_id)
es_eventhub_name = es_source_connection["EventHub Name"].iloc[0]
es_eventhub_connstring = es_source_connection["Primary Connection String"].iloc[0]

producer = EventHubProducerClient.from_connection_string(conn_str=es_eventhub_connstring, eventhub_name=es_eventhub_name)

hostname = socket.gethostname()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fraud detection parameters
FRAUD_INDICATORS = {
    'VERY_LOW_CREDIT_SCORE': 500,
    'VERY_HIGH_LOAN_AMOUNT': 500000,
    'RAPID_TRANSACTION_THRESHOLD': 2,  # seconds
    'SUSPICIOUS_CREDIT_SCORE_RANGE': (400, 550)
}

STATUSES = ['Document verification', 'Eligibility check', 'Confirmed']
COUNTRIES = ['USA', 'UK', 'Canada', 'Australia', 'Germany']
DEVICES = ['Mobile', 'Desktop', 'Tablet']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class BankingLoanDataGenerator:
    def __init__(self):
        self.customer_sessions = {}  # Track customer sessions for fraud detection
        self.last_timestamp = datetime.now()
    
    def generate_customer_id(self) -> str:
        """Generate unique customer ID"""
        return f"CUST_{uuid.uuid4().hex[:8].upper()}"
    
    def determine_fraud_flags(self, data: Dict) -> Dict:
        """Determine fraud indicators for a transaction"""
        fraud_flags = {
            'is_fraud_suspected': False,
            'fraud_reasons': []
        }
        
        # Very low credit score
        if data['credit_score'] < FRAUD_INDICATORS['VERY_LOW_CREDIT_SCORE']:
            fraud_flags['is_fraud_suspected'] = True
            fraud_flags['fraud_reasons'].append('VeryLowCreditScore')
        
        # Suspicious credit score range with high loan amount
        if FRAUD_INDICATORS['SUSPICIOUS_CREDIT_SCORE_RANGE'][0] <= data['credit_score'] <= FRAUD_INDICATORS['SUSPICIOUS_CREDIT_SCORE_RANGE'][1]:
            if data['loan_amount'] > 100000:
                fraud_flags['is_fraud_suspected'] = True
                fraud_flags['fraud_reasons'].append('SuspiciousScoreLoanMismatch')
        
        # Very high loan amount
        if data['loan_amount'] > FRAUD_INDICATORS['VERY_HIGH_LOAN_AMOUNT']:
            fraud_flags['is_fraud_suspected'] = True
            fraud_flags['fraud_reasons'].append('ExcessivelyHighLoanAmount')
        
        # Status sequence validation (should follow order)
        customer_id = data['customer_id']
        if customer_id in self.customer_sessions:
            last_status = self.customer_sessions[customer_id]['last_status']
            current_status = data['status']
            
            # Check if going backwards in workflow
            if STATUSES.index(current_status) < STATUSES.index(last_status):
                fraud_flags['is_fraud_suspected'] = True
                fraud_flags['fraud_reasons'].append('InvalidStatusSequence')
            
            # Check for rapid consecutive transactions
            last_timestamp = self.customer_sessions[customer_id]['last_timestamp']
            time_diff = (data['timestamp'] - last_timestamp).total_seconds()
            if time_diff < FRAUD_INDICATORS['RAPID_TRANSACTION_THRESHOLD']:
                fraud_flags['is_fraud_suspected'] = True
                fraud_flags['fraud_reasons'].append('RapidTransactions')
        
        # Update session
        self.customer_sessions[customer_id] = {
            'last_status': data['status'],
            'last_timestamp': data['timestamp']
        }
        
        return fraud_flags
    
    def generate_event(self, customer_id: str = None, force_fraud: bool = False) -> Dict:
        """Generate a single banking loan clickstream event"""
        if customer_id is None:
            customer_id = self.generate_customer_id()
        
        # Add random time gap
        self.last_timestamp += timedelta(seconds=random.randint(1, 30))
        
        credit_score = random.randint(300, 800)
        
        # If forcing fraud, generate suspicious data
        if force_fraud:
            if random.random() > 0.5:
                credit_score = random.randint(300, 550)  # Low score
            loan_amount = random.randint(450000, 1000000)  # Very high
        else:
            loan_amount = random.randint(10000, 300000)
        
        data = {
            'customer_id': customer_id,
            'transaction_id': str(uuid.uuid4()),
            'loan_amount': loan_amount,
            'status': random.choice(STATUSES),
            'credit_score': credit_score,
            'timestamp': self.last_timestamp,
            'country': random.choice(COUNTRIES),
            'device_type': random.choice(DEVICES),
            'ip_address': f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
        }
        
        # Add fraud flags
        fraud_info = self.determine_fraud_flags(data)
        data.update(fraud_info)
        
        return data
    
    def generate_batch(self, count: int = 10, fraud_percentage: float = 0.1) -> List[Dict]:
        """Generate a batch of events"""
        events = []
        fraud_count = int(count * fraud_percentage)
        
        for i in range(count):
            force_fraud = i < fraud_count
            events.append(self.generate_event(force_fraud=force_fraud))
        
        return events

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize generator and stream events to EventHub
generator = BankingLoanDataGenerator()

sleep_seconds = 1
max_events = None  # set to an int for a finite run
batch_size = 10  # Send events in batches for efficiency

events = []
event_count = 0

print("Connecting to EventHub...")

print(f"Streaming events to EventHub ... press Stop/Interrupt to end.\n")


event_batch = producer.create_batch()

while max_events is None or event_count < max_events:
    # Generate event
    event = generator.generate_event(force_fraud=(random.random() < 0.15))
    events.append(event)
    event_count += 1
    
    # Keep a rolling window to avoid unbounded memory growth
    if len(events) > 500:
        events = events[-500:]
    
    # Convert to JSON and add to batch
    event_json = json.dumps(event, default=str)
    
    try:
        event_batch.add(EventData(event_json))
    except ValueError:
        # Batch is full, send it and create a new batch
        producer.send_batch(event_batch)
        print(f"✓ Sent batch of {event_count} events to EventHub")
        event_batch = producer.create_batch()
        event_batch.add(EventData(event_json))
    
    # Send batch every batch_size events
    if event_count % batch_size == 0:
        producer.send_batch(event_batch)
        fraud_count = sum(1 for e in events[-batch_size:] if e.get('is_fraud_suspected'))
        print(f"✓ Sent {event_count} events | Last batch: {fraud_count}/{batch_size} fraud suspected")
        event_batch = producer.create_batch()
    
    time.sleep(sleep_seconds)

# Send any remaining events in the batch
if len(event_batch) > 0:
    producer.send_batch(event_batch)
    print(f"✓ Sent final batch to EventHub")

# Latest snapshot as DataFrame
df = pd.DataFrame(events)
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
