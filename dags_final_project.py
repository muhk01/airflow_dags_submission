# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Muhamad Khoirul',
    'start_date': days_ago(0),
    'email': ['muhamadkoirul@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)
#
# unzip the data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf {{ params.input_path }} -C {{ params.output_path }}',
    params={
        'input_path': '/home/project/airflow/dags/finalassignment/staging/tolldata.tgz',
        'output_path': '/home/project/airflow/dags/finalassignment/staging',
    },
    dag=dag,
)
# extract from csv file
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d "," -f 1,2,3,4 {{ params.input_path }} > {{ params.output_path }}',
    params={
        'input_path': '/home/project/airflow/dags/finalassignment/staging/vehicle-data.csv',
        'output_path': '/home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    },
    dag=dag,
)
# extract from tsv file
# cut index 5-7 replace tab with commas.
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 {{ params.input_path }} | tr -d "\r" | tr "[:blank:]" "," > {{ params.output_path }}',
    params={
        'input_path': '/home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv',
        'output_path': '/home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    },
    dag=dag,
)

# extract from fixed width file
# replace multiple space with single space, cut index 11,12, and replace space with commas.
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="sed 's/ \{1,\}/ /g' {{ params.input_path }} | cut -d ' ' -f 11,12 | tr ' ' ',' > {{ params.output_path }}",
    params={
        'input_path': '/home/project/airflow/dags/finalassignment/staging/payment-data.txt',
        'output_path': '/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    },
    dag=dag,
)


# consolidated data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d ',' {{ params.input1_path }} {{ params.input2_path }} {{ params.input3_path }}  > {{ params.output_path }}",
    params={
        'input1_path': '/home/project/airflow/dags/finalassignment/staging/csv_data.csv',
        'input2_path': '/home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
        'input3_path': '/home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
        'output_path': '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv',
    },
    dag=dag,
)

# transform data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" < {{ params.input_path }} > {{ params.output_path }}',
    params={
        'input_path': '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv',
        'output_path': '/home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    },
    dag=dag,
)
# task pipeline 
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
