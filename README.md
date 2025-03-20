# Spark Event Log Viewer

This project consists of a backend Go service, a React frontend application, and a Python script for simulating Spark event logs.

## Getting Started

Follow these steps to run the project:

**1. Run the Backend Go Application:**

Go the backend folder and run:

```bash
go run .
```

This command starts the Go backend service. The service watches the spark-events directory for new or updated .gz log files. It processes these logs, converts them to Parquet files, and provides an HTTP endpoint for retrieving paginated data.

**2. Run the React Frontend Application:**

Go the frontend folder and run:

```bash
npm start
```
This command starts the React frontend application. The frontend provides a simple pagination table that displays the data from the Parquet files. It communicates with the backend's /data endpoint to fetch and display the data.

**3. Run the Python Script (Simulate Spark Event Logs):**

Execute the Python script to simulate the generation of Spark event log files. This script creates .gz files in the spark-events directory, which the backend service monitors.

```bash
python3 ./add_log.py
```

Note: Ensure you have Python installed and any required Python packages are installed before running the script.

# Project Overview

## Project Description

* **Backend Service (Go):**
    * Watches the `spark-events` directory for new or updated `.gz` log files.
    * Reads and processes the log files.
    * Converts the log data into Parquet files.
    * Provides an HTTP service with a `POST /data` endpoint.
    * The `/data` endpoint accepts `page` and `limit` parameters and returns a specific slice of records from the Parquet files along with the total number of records.
* **Frontend Application (React):**
    * Presents a simple pagination table.
    * Fetches data from the backend's `/data` endpoint.
    * Displays the data in the table.
    * Provides user controls for pagination.
* **Python Script:**
    * Simulates the generation of Spark event log files by creating `.gz` files within the watched directory.

## Prerequisites

* Go (for the backend service)
* Node.js and npm (for the React frontend)
* Python (for the log simulation script)

## Directory Structure

your-project/
├── backend/          (Go backend service)
├── frontend/         (React frontend application)
├── python_script     (Python log simulation scripts)
├── spark-events/     (Directory for simulated log files)
├── README.md


## Further Development

* Improve the frontend UI for a better user experience.
* Add more features to the backend service, such as filtering and sorting.
* Implement more realistic log simulation in the Python script.
* Add error handling and logging throughout the application.
* Add tests.