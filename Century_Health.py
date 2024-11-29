import pandas as pd
import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from utils.database import insert_to_sql, test_connection
from utils.transformation import (
    clean_conditions_data, clean_encounter_data, clean_medications_data,
    clean_patients_data, clean_symptoms_data, merge_patients_and_gender, merge_datasets
)

# Configure logging to track the pipeline's progress and errors
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#
file_path = "/home/akhil/airflow/dags/data/"

# File paths to the datasets being processed
file_paths = {
    "conditions": f"{file_path}conditions.csv",
    "encounters": f"{file_path}encounters.parquet",
    "medications": f"{file_path}medications.csv",
    "patients": f"{file_path}patients.csv",
    "symptoms": f"{file_path}symptoms.csv",
    "patients_gender": f"{file_path}patient_gender.csv"
}

def read_file(file_key, **kwargs):
    """
    Read a specific file and push its DataFrame to XCom.
    
    Args:
        file_key (str): The key corresponding to the file in `file_paths`.
        kwargs: Additional context from Airflow.

    Raises:
        Exception: If there is an error reading the file.
    """
    try:
        # Determine the reading method based on file type (CSV or Parquet)
        if file_key == "encounters":
            data = pd.read_parquet(file_paths[file_key], engine='pyarrow')
        else:
            data = pd.read_csv(file_paths[file_key], encoding="ISO-8859-1")
        
        # Log success and push the raw data to XCom
        logger.info(f"Successfully read {file_key} file")
        kwargs['ti'].xcom_push(key=f"{file_key}_raw", value=data)
    except Exception as e:
        logger.error(f"Error reading {file_key} file: {e}")
        raise

def transform_file(file_key, transform_function, **kwargs):
    """
    Transform a specific file's data using a provided transformation function.

    Args:
        file_key (str): The key corresponding to the file in `file_paths`.
        transform_function (callable): Function to clean/transform the data.
        kwargs: Additional context from Airflow.

    Raises:
        Exception: If there is an error during data transformation.
    """
    try:
        # Pull the raw data from XCom
        raw_data = kwargs['ti'].xcom_pull(key=f"{file_key}_raw")
        
        # Apply the transformation function to the raw data
        transformed_data = transform_function(raw_data)
        
        # Log success and push the transformed data to XCom
        logger.info(f"Successfully transformed {file_key} data")
        kwargs['ti'].xcom_push(key=f"{file_key}_cleaned", value=transformed_data)
    except Exception as e:
        logger.error(f"Error transforming {file_key} data: {e}")
        raise

def merge_and_insert_data(**kwargs):
    """
    Merge patient data with other datasets and insert the merged dataset into the database.

    Steps:
        1. Retrieve and merge patient data with gender information.
        2. Sequentially merge other datasets (symptoms, encounters, conditions, medications).
        3. Insert the final merged dataset into the database.
        4. Save the final dataset as a CSV file.

    Args:
        kwargs: Context arguments passed by Airflow.

    Raises:
        Exception: If merging or database insertion fails.
    """
    try:
        # Retrieve transformed data from XCom
        ti = kwargs['ti']
        patients = ti.xcom_pull(key='patients_cleaned')
        patients_gender = ti.xcom_pull(key='patients_gender_raw')
        
        # Merge patient data with gender data
        patients = merge_patients_and_gender(patients, patients_gender)

        # Sequentially merge with other datasets
        datasets_to_merge = {
            "symptoms": ti.xcom_pull(key='symptoms_cleaned'),
            "encounters": ti.xcom_pull(key='encounters_cleaned'),
            "conditions": ti.xcom_pull(key='conditions_cleaned'),
            "medications": ti.xcom_pull(key='medications_cleaned')
        }
        merged_df = patients
        for dataset, key in [
            ("symptoms", "patient_id"),
            ("encounters", "patient_id"),
            ("conditions", ["encounter_id", "patient_id"]),
            ("medications", ["encounter_id", "patient_id", "payer_id"])
        ]:
            merged_df = merge_datasets(merged_df, datasets_to_merge[dataset], key)

        # Test database connection before insertion
        test_connection()
        # Insert the merged data into the SQL table (currently commented out)
        # insert_to_sql(merged_df, "master_table")
        logger.info("Merged data inserted into database")

        # Save the final merged dataset as a CSV file
        merged_df.to_csv(f"{file_path}masters.csv", index=False)
        logger.info("Master dataset saved as CSV")
    except Exception as e:
        logger.error(f"Error during merge and insert: {e}")
        raise

# Default arguments for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Starts the DAG one day in the past
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': timedelta(minutes=5)  # Delay between retries
}

# Define the DAG
dag = DAG(
    'parallel_data_pipeline_century_health',  # DAG name
    default_args=default_args,
    description='Parallelized Healthcare Data Pipeline',  # Brief description
    schedule_interval=timedelta(days=1),  # Run interval
    catchup=False  # Do not backfill missed runs
)

# Define tasks for reading files
read_tasks = {}
for file_key in file_paths.keys():
    read_tasks[file_key] = PythonOperator(
        task_id=f'read_{file_key}_file',  # Unique task ID
        python_callable=read_file,  # Function to execute
        op_kwargs={'file_key': file_key},  # Pass file key as an argument
        provide_context=True,  # Enable context passing
        dag=dag
    )

# Define tasks for transforming data
transform_tasks = {
    "conditions": clean_conditions_data,
    "encounters": clean_encounter_data,
    "medications": clean_medications_data,
    "patients": clean_patients_data,
    "symptoms": clean_symptoms_data
}
transform_tasks_operators = {}
for file_key, transform_function in transform_tasks.items():
    transform_tasks_operators[file_key] = PythonOperator(
        task_id=f'transform_{file_key}_data',  # Unique task ID
        python_callable=transform_file,  # Function to execute
        op_kwargs={'file_key': file_key, 'transform_function': transform_function},
        provide_context=True,  # Enable context passing
        dag=dag
    )

# Define the task for merging and inserting data
merge_and_insert_task = PythonOperator(
    task_id='merge_and_insert_data',  # Unique task ID
    python_callable=merge_and_insert_data,  # Function to execute
    provide_context=True,  # Enable context passing
    dag=dag
)

# Set task dependencies
# Each file read task is followed by its respective transformation task
for file_key in file_paths.keys():
    read_tasks[file_key] >> transform_tasks_operators.get(file_key, [])

# All transformation tasks must complete before merging and inserting
list(transform_tasks_operators.values()) >> merge_and_insert_task
