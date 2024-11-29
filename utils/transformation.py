import pandas as pd
from datetime import datetime

# Function to clean and standardize the conditions dataset
def clean_conditions_data(df):
    """
    Transforms and standardizes the conditions dataset for consistent analysis,
    ensuring clean and uniform patient and condition information.
    """
    # Rename columns to meaningful and standardized names
    df.rename(columns={
        'PATIENT': 'patient_id',
        'ENCOUNTER': 'encounter_id',
        'CODE': 'conditional_code',
        'DESCRIPTION': 'conditional_description'
    }, inplace=True)

    # Standardize text formatting for descriptions and patient IDs
    df['conditional_description'] = df['conditional_description'].str.title()
    df['patient_id'] = df['patient_id'].str.lower()

    # Drop unnecessary columns for analysis
    df.drop(columns=['START', 'STOP', 'conditional_code', 'conditional_description'], inplace=True)

    return df

# Function to clean and process medications data
def clean_medications_data(df):
    """
    Processes medication data to create a clean, standardized dataset
    with normalized text, converted numeric values, and consistent formatting.
    """
    # Rename columns for clarity and consistency
    df.rename(columns={
        'START': 'start_date',
        'STOP': 'end_date',
        'PATIENT': 'patient_id',
        'PAYER': 'payer_id',
        'ENCOUNTER': 'encounter_id',
        'CODE': 'drug_code',
        'DESCRIPTION': 'drug_description',
        'BASE_COST': 'base_cost',
        'PAYER_COVERAGE': 'payer_coverage',
        'DISPENSES': 'dispensed_quantity',
        'TOTALCOST': 'total_cost',
        'REASONCODE': 'reason_code',
        'REASONDESCRIPTION': 'reason_description'
    }, inplace=True)

    # Normalize text data (lowercase and titlecase where applicable)
    columns_to_lowercase = ['patient_id', 'payer_id', 'encounter_id', 'drug_description']
    for col in columns_to_lowercase:
        df[col] = df[col].str.lower()

    df['drug_description'] = df['drug_description'].str.title()

    # Convert specific columns to numeric types, handling errors gracefully
    int_columns = ['drug_code', 'dispensed_quantity', 'reason_code']
    df[int_columns] = df[int_columns].apply(pd.to_numeric, errors='coerce', downcast='integer')

    float_columns = ['base_cost', 'total_cost']
    df[float_columns] = df[float_columns].apply(pd.to_numeric, errors='coerce', downcast='float')

    # Convert date columns to datetime objects and remove timezone information
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce').dt.tz_localize(None)
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce').dt.tz_localize(None)

    # Drop irrelevant or redundant columns
    df.drop(columns=['reason_description', 'reason_code', 'payer_coverage', 'start_date', 'end_date'], inplace=True)

    return df

# Function to clean and standardize patient data
def clean_patients_data(df):
    """
    Refines patient data by standardizing names, cleaning geographical information,
    and ensuring consistent data formatting across all patient records.
    """
    # Rename columns to standardized names
    df.rename(columns={
        'PATIENT_ID': 'patient_id',
        'BIRTHDATE': 'birth_date',
        'DEATHDATE': 'death_date',
        'FIRST': 'first_name',
        'LAST': 'last_name',
        'BIRTHPLACE': 'birth_place',
        'COUNTY': 'county',
        'LAT': 'latitude',
        'LON': 'longitude',
        'INCOME': 'income'
    }, inplace=True)

    # Remove irrelevant columns
    df.drop(columns=['GENDER'], inplace=True)

    # Standardize patient ID to lowercase
    df['patient_id'] = df['patient_id'].str.lower()

    # Convert birth and death dates to datetime format
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
    df['death_date'] = pd.to_datetime(df['death_date'], errors='coerce')

    # Standardize names by removing trailing digits and capitalizing
    df['first_name'] = df['first_name'].str.replace(r'\d+$', '', regex=True).str.capitalize()
    df['last_name'] = df['last_name'].str.replace(r'\d+$', '', regex=True).str.capitalize()

    # Clean and standardize birth place and county information
    df['birth_place'] = df['birth_place'].str.strip().str.title()
    df['county'] = df['county'].apply(lambda x: x[:-6].strip() if isinstance(x, str) and x.endswith('County') else x)

    # Process and clean latitude and longitude values
    df['latitude'] = df['latitude'].apply(lambda x: str(x).lstrip("'") if isinstance(x, str) else x)
    df['longitude'] = pd.to_numeric(df['longitude'].astype(str).str.strip(), errors='coerce')

    # Convert income to numeric type
    df['income'] = pd.to_numeric(df['income'], errors='coerce')

    # Ensure all column names are lowercase
    df.columns = df.columns.str.lower()

    return df

# Function to clean and process symptoms data
def clean_symptoms_data(df):
    """
    Processes symptoms data to create a clean, structured dataset
    with normalized patient identifiers and symptom information.
    """
    # Rename columns to standardized names
    df.rename(columns={
        'PATIENT': 'patient_id',
        'RACE': 'race',
        'ETHNICITY': 'symptoms_ethnicity',
        'AGE_BEGIN': 'start_age',
        'AGE_END': 'end_age',
        'PATHOLOGY': 'pathology',
        'NUM_SYMPTOMS': 'num_symptoms',
        'SYMPTOMS': 'symptoms'
    }, inplace=True)

    # Standardize text formatting for pathology and patient IDs
    df['pathology'] = df['pathology'].str.title()
    df['patient_id'] = df['patient_id'].str.lower()

    # Drop irrelevant columns
    df.drop(columns=['GENDER', 'race', 'symptoms_ethnicity'], inplace=True)

    # Ensure all column names are lowercase
    df.columns = df.columns.str.lower()

    return df

# Function to clean and transform encounter data
def clean_encounter_data(df):
    """
    Transforms encounter data to provide a clean, consistent dataset
    with standardized patient and encounter information.
    """
    # Rename columns to standardized names
    df.rename(columns={
        'Id': 'encounter_id',
        'START': 'start_date',
        'STOP': 'stop_date',
        'PATIENT': 'patient_id',
        'ORGANIZATION': 'organization',
        'REASONDESCRIPTION': 'encounter_description',
        'REASONCODE': 'reason_code',
        'DESCRIPTION': 'description',
        'ENCOUNTERCLASS': 'encounter_class',
        'PAYER': 'payer_id',
        'PROVIDER': 'provider_id',
        'CODE': 'code',
        'BASE_ENCOUNTER_COST': 'base_encounter_cost',
        'TOTAL_CLAIM_COST': 'total_claim_cost',
        'PAYER_COVERAGE': 'payer_coverage'
    }, inplace=True)

    # Standardize patient ID to lowercase
    df['patient_id'] = df['patient_id'].str.lower()

    # Ensure all column names are lowercase
    df.columns = df.columns.str.lower()

    # Drop redundant columns
    df.drop(columns=['encounter_description'], inplace=True)

    return df

# Function to merge patient data with gender data
def merge_patients_and_gender(df_patients, df_gender):
    """
    Combines patient information with gender data using patient identifier,
    creating a comprehensive consolidated dataset.
    """
    # Merge datasets on patient ID
    df_merged = pd.merge(df_patients, df_gender, left_on='patient_id', right_on='Id', how='inner')

    # Drop unnecessary columns after merging
    df_merged.drop(columns=['Id'], inplace=True)

    print("Successfully merged the datasets based on 'patient_id'.")

    return df_merged

# Generalized function to merge two datasets
def merge_datasets(dataset1, dataset2, key_columns, how='left'):
    """
    Flexible dataset merging function that allows different join strategies
    based on specified key columns.
    """
    return dataset1.merge(dataset2, on=key_columns, how=how)
