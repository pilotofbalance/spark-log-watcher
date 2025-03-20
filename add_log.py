#!/usr/bin/env python3

import gzip
import json
import time
from datetime import datetime

def generate_event(event_type, idx):
    """
    Generates a synthetic SparkListener event object for demonstration.
    You can adjust or add fields as desired to mimic real Spark logs more closely.
    """
    base_time = int(time.time() * 1000) 
    return {
    "Event": event_type,
    "Timestamp": base_time + idx,
    "SparkVersion": "3.3.0",
    "AppName": "TestApp",
    "AppId": f"app-2023-1000events-{idx}",
    "User": "sparkUser",
    "StageID": idx % 10,
    "StageAttemptID": 0,
    "TaskInfo": {
    "TaskID": idx,
    "Index": idx % 100,
    "Attempt": 1
    },
    "JobID": idx % 5,
    "ExecutorID": str(idx % 3),
    "ExtraData": f"some_extra_data_{idx}"
    }

def main():
    event_types = [
    "SparkListenerLogStart",
    "SparkListenerApplicationStart",
    "SparkListenerApplicationEnd",
    "SparkListenerEnvironmentUpdate",
    "SparkListenerExecutorAdded",
    "SparkListenerExecutorRemoved",
    "SparkListenerBlockManagerAdded",
    "SparkListenerBlockManagerRemoved",
    "SparkListenerJobStart",
    "SparkListenerJobEnd",
    "SparkListenerStageSubmitted",
    "SparkListenerStageCompleted",
    "SparkListenerTaskStart",
    "SparkListenerTaskEnd"
    ]

    filename = f"spark-events/{int(time.time() * 1000) }-spark_event_log.gz"  
    num_events = 1000  
    delay_seconds = 0.5

    with gzip.open(filename, "wt", encoding="utf-8") as f:  
        for i in range(num_events):   
            evt_type = event_types[i % len(event_types)]  
            event_record = generate_event(evt_type, i)  
            f.write(json.dumps(event_record) + "\n")  
            f.flush() 
            print(event_record) 
            time.sleep(delay_seconds)

    print(f"Generated {filename} with {num_events} synthetic events.")  
 
main()