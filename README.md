# Century_health_assignment

# Dynamic Healthcare Data Pipeline

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow for processing healthcare data. The pipeline dynamically reads, transforms, merges, and inserts data from multiple healthcare datasets into a master database table.

## Features

- **Dynamic Task Creation**: Automatically generates reading and transformation tasks
- **Data Cleaning and Transformation**: Applies specific cleaning operations to each dataset
- **Data Merging**: Consolidates multiple healthcare datasets
- **Database Integration**: Inserts merged data into a database table
- **Modular and Scalable**: Easy extension with new datasets or transformation functions

## Processed Datasets

1. Conditions (`conditions.csv`)
2. Encounters (`encounters.parquet`)
3. Medications (`medications.csv`)
4. Patients (`patients.csv`)
5. Symptoms (`symptoms.csv`)
6. Patients Gender (`patient_gender.csv`)

## Directory Structure

```
.
├── dags/
│   ├── dynamic_healthcare_pipeline.py  # Main DAG file
│   └── utils/
│       ├── database.py     # Database interaction utilities
│       └── transformation.py  # Data transformation functions
└── data/
    ├── conditions.csv
    ├── encounters.parquet
    ├── medications.csv
    ├── patients.csv
    ├── symptoms.csv
    └── patient_gender.csv
```

## Requirements

- Apache Airflow
- Python
- Pandas
- PostgreSQL (or another SQL database)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-name>
   ```

2. Install dependencies:
   ```bash
   pip install pandas apache-airflow
   ```

## Setup and Execution

1. Initialize Airflow:
   ```bash
   export AIRFLOW_HOME=~/airflow
   airflow db init
   airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
   ```

2. Add the DAG:
   - Place `dynamic_healthcare_pipeline.py` in `~/airflow/dags/`

3. Start Airflow services:
   ```bash
   airflow scheduler &
   airflow webserver
   ```

4. Trigger the Pipeline:
   - Open Airflow UI at `http://localhost:8080`
   - Trigger the `dynamic_healthcare_pipeline` DAG

## Customization

- **Adding Datasets**: 
  - Add file path to `file_paths`
  - Define new cleaning function in `utils/transformation.py`

- **Changing Database**: 
  - Update connection logic in `utils/database.py`

## Pipeline Stages

1. **Reading**: Reads datasets in `.csv` and `.parquet` formats
2. **Transformation**: Applies cleaning and transformation logic
3. **Merging and Insertion**: Combines datasets and inserts into database

## Tasks

- Reading Tasks: Dynamically created for each dataset
- Transformation Tasks:
  - `clean_conditions_data`
  - `clean_encounter_data`
  - `clean_medications_data`
  - `clean_patients_data`
  - `clean_symptoms_data`
- Merge and Insert Task: Combines datasets into master table
