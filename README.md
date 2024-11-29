# Century_health_assignment

Dynamic Healthcare Data Pipeline
This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow for processing healthcare data. It dynamically reads, transforms, merges, and inserts data from multiple datasets into a master table in a database.

Features
Dynamic Task Creation: Reading and transformation tasks are dynamically created using dictionaries.
Data Cleaning and Transformation: Each dataset undergoes specific cleaning and transformation operations.
Data Merging: Integrates multiple datasets (patients, encounters, conditions, etc.) into a consolidated table.
Database Integration: The final merged data is inserted into a database table.
Modular and Scalable: Easily extend the pipeline by adding new datasets or transformation functions.
Datasets
The pipeline processes the following datasets:

Conditions (conditions.csv)
Encounters (encounters.parquet)
Medications (medications.csv)
Patients (patients.csv)
Symptoms (symptoms.csv)
Patients Gender (patient_gender.csv)
Directory Structure
plaintext
Copy code
.
├── dags/
│   ├── dynamic_healthcare_pipeline.py  # Main DAG file
│   ├── utils/
│   │   ├── database.py                 # Database interaction utilities
│   │   ├── transformation.py           # Data transformation functions
│   └── data/
│       ├── conditions.csv
│       ├── encounters.parquet
│       ├── medications.csv
│       ├── patients.csv
│       ├── symptoms.csv
│       └── patient_gender.csv
└── README.md                           # Project documentation
Requirements
Apache Airflow: Task scheduling and orchestration.
Python: For data processing and transformation.
Pandas: Data manipulation and analysis.
Database: PostgreSQL (or any other SQL database).
Python Libraries
Install the required Python libraries:

bash
Copy code
pip install pandas apache-airflow
DAG Overview
The pipeline is structured into three main stages:

Reading: Reads datasets in .csv and .parquet formats.
Transformation: Applies cleaning and transformation logic for each dataset.
Merging and Insertion: Merges datasets into a master table and inserts the result into the database.
Tasks
Reading Tasks: Dynamically created tasks to read datasets based on file paths.

Transformation Tasks:

clean_conditions_data: Cleans the conditions dataset.
clean_encounter_data: Cleans the encounters dataset.
clean_medications_data: Cleans the medications dataset.
clean_patients_data: Cleans the patients dataset and merges gender data.
clean_symptoms_data: Cleans the symptoms dataset.
Merge and Insert Task: Combines all datasets into a master table and inserts it into a database.

How to Run
Clone the Repository:

bash
Copy code
git clone <repository-url>
cd <repository-name>
Set Up Apache Airflow:

bash
Copy code
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
Add the DAG:

Place the dynamic_healthcare_pipeline.py file into the ~/airflow/dags/ directory.

Start Airflow:

bash
Copy code
airflow scheduler &
airflow webserver
Trigger the DAG:

Open the Airflow UI at http://localhost:8080 and trigger the dynamic_healthcare_pipeline DAG.

Customization
Add New Datasets: Add the file path to file_paths and, if necessary, define a new cleaning function in utils/transformation.py.

Change Database: Update the connection logic in utils/database.py.
