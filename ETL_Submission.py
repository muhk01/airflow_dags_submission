from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, time
import json
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import pandas as pd
import xml.etree.ElementTree as ET
import requests
from sqlalchemy import create_engine
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pytz
import os

def send_email_notifications(ti):
    str_variable = Variable.get("var_submission",deserialize_json=True)
    username = str_variable["username"]
    password = str_variable["password"]
    server = str_variable["server"]
    port = str_variable["port"]
    database = str_variable["database"]
    current_time = datetime.now()
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
    
    processed_rows = ti.xcom_pull(task_ids='transform_task')['row_processed']
    mail_content = "Job Running hari ini Berhasil Tanggal " + str(current_time_str) + " Jumlah Data diproses : " + str(processed_rows)
    print(mail_content)
    send_mail(1,mail_content)
    var_submission_value = Variable.get("var_submission")
    var_submission_data = json.loads(var_submission_value)
    var_submission_data["initial_status"] = "True"
    updated_var_submission_value = json.dumps(var_submission_data)
    Variable.set("var_submission", updated_var_submission_value)

def send_mail(success, mail_content):
    current_time = datetime.now()
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
    str_variable = Variable.get("var_submission",deserialize_json=True)
    sender_address = str_variable["sender_address"] 
    sender_pass = str_variable["sender_pass"] 
    receiver_address = str_variable["receiver_address"] 

    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    if(success == 1):
        message['Subject'] = 'Job Load Hari ' + str(current_time_str)  #The subject line
    else:
        message['Subject'] = 'Error Hari ini ' + str(current_time_str)  #The subject line
    #The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, sender_pass) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()

def read_db(sql_query, username, password, server, port, database):
    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)
    df = pd.read_sql_query(sql_query, con=engine)
    return df

def read_json(url):
    response = requests.get(url)
    data = response.json()

    users_data = data["users"]
    id = [user["id"] for user in users_data]
    first_names = [user["firstName"] for user in users_data]
    last_names = [user["lastName"] for user in users_data]

    # Create a DataFrame with first name and last name
    df = pd.DataFrame({'id':id, 'firstName': first_names, 'lastName': last_names})
    return df

def read_excel(filename, sheet_name):
    if os.path.exists(filename):
        df = pd.read_excel(filename, sheet_name=sheet_name, header=1)
    else:
        mail_content = "File Tidak Ditemukan " + str(filename)
        #print(mail_content)
        send_mail(0, mail_content)
    return df

def read_xml(filename):
    file_path = filename
    with open(file_path, 'r') as file:
        xml_data = file.read()

    root = ET.fromstring(xml_data)
    ids = []
    gender_names = []

    for gender_elem in root.findall('gender'):
        gender_id = gender_elem.get('id')
        name = gender_elem.find('name').text

        ids.append(gender_id)
        gender_names.append(name)

    df = pd.DataFrame({'id': ids, 'gender_name': gender_names})
    return df

def update_df(df1, df2):
    #update df old master.xlsx with new value from daily master.xlsx
    if not df2.empty:
        df1.set_index('id', inplace=True)
        df2.set_index('id', inplace=True)
        #update value by id
        df1.update(df2)

        #add new value from new_df but filled value will take priority
        df1 = df1.combine_first(df2)

        df1.reset_index(inplace=True)
    return df1

def load_transactions_dm(ti):
    str_variable = Variable.get("var_submission",deserialize_json=True)
    username = str_variable["username"]
    password = str_variable["password"]
    server = str_variable["server"]
    port = str_variable["port"]
    database = str_variable["database"]

    sql_query = "select gender, SUM(price * quantity::numeric) as total_transactions from fact_customer_transaction group by gender;"
    df_data_mart = read_db(sql_query, username, password, server, port, database)

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)
    df_data_mart.to_sql(name='data_mart_gender_transactions',con=engine,index=False, if_exists='replace')

    
def extract_initial_data(ti):
    str_variable = Variable.get("var_submission",deserialize_json=True)
    file_path = str_variable["path_initial"]
    api_url = str_variable["api_url"]
    username = str_variable["username"]
    password = str_variable["password"]
    server = str_variable["server"]
    port = str_variable["port"]
    database = str_variable["database"]

    filename_xls = file_path + "Master_Data.xlsx"
    filename_xml = file_path + "Master_Gender.xml"
    
    sheet_category = "master_category"
    df_category = read_excel(filename_xls, sheet_category)

    sheet_category = "master_payment_method"
    df_payment = read_excel(filename_xls, sheet_category)

    sheet_category = "master_shopping_mall"
    df_mall = read_excel(filename_xls, sheet_category)

    df_gender = read_xml(filename_xml)
    
    df_user = read_json(api_url)
    
    # for initial query dates < today (2023-07-17)
    sql_query = "select * from customer_transaction where invoice_date < '2023-07-17'"
    df_transactions = read_db(sql_query, username, password, server, port, database)
    
    return {'df_category': df_category, 'df_payment': df_payment, 'df_mall': df_mall,
    'df_gender': df_gender, 'df_user': df_user, 'df_transactions': df_transactions}

def extract_daily_data(ti):
    str_variable = Variable.get("var_submission",deserialize_json=True)
    file_path = str_variable["path_initial"]
    file_path_daily = str_variable["path_daily"]
    api_url = str_variable["api_url"]
    username = str_variable["username"]
    password = str_variable["password"]
    server = str_variable["server"]
    port = str_variable["port"]
    database = str_variable["database"]

    #updated master data
    filename_xls_new = file_path_daily + "Master_Data.xlsx"
    #old master data
    filename_xls_old = file_path + "Master_Data.xlsx"

    filename_xml = file_path + "Master_Gender.xml"
    
    sheet_category = "master_category"
    df_category_old = read_excel(filename_xls_old, sheet_category)
    df_category_new = read_excel(filename_xls_new, sheet_category)
    df_category_final = update_df(df_category_old, df_category_new)

    sheet_category = "master_payment_method"
    df_payment_old = read_excel(filename_xls_old, sheet_category)
    df_payment_new = read_excel(filename_xls_new, sheet_category)
    df_payment_final = update_df(df_payment_old, df_payment_new)

    sheet_category = "master_shopping_mall"
    df_mall_old = read_excel(filename_xls_old, sheet_category)
    df_mall_new = read_excel(filename_xls_new, sheet_category)
    df_mall_final = update_df(df_mall_old, df_mall_new)

    df_gender = read_xml(filename_xml)
    
    df_user = read_json(api_url)
    
    # assume that today is 2023-07-17 / replaced with current date
    sql_query = "select * from customer_transaction where invoice_date = '2023-07-17'"
    df_transactions = read_db(sql_query, username, password, server, port, database)
    
    #print(df_payment_final)
    return {'df_category': df_category_final, 'df_payment': df_payment_final, 'df_mall': df_mall_final,
    'df_gender': df_gender, 'df_user': df_user, 'df_transactions': df_transactions}

def transform_data(ti):
    str_variable = Variable.get("var_submission",deserialize_json=True)
    username = str_variable["username"]
    password = str_variable["password"]
    server = str_variable["server"]
    port = str_variable["port"]
    database = str_variable["database"]

    run_type = ti.xcom_pull(task_ids='decide_run_initial')
    #print(run_type)
    #fetch downstream
    if(run_type == 'extract_initial_task'):
        df_user = ti.xcom_pull(task_ids='extract_initial_task')['df_user']
        df_category = ti.xcom_pull(task_ids='extract_initial_task')['df_category']
        df_payment = ti.xcom_pull(task_ids='extract_initial_task')['df_payment']
        df_mall = ti.xcom_pull(task_ids='extract_initial_task')['df_mall']
        df_gender = ti.xcom_pull(task_ids='extract_initial_task')['df_gender']
        df_transactions = ti.xcom_pull(task_ids='extract_initial_task')['df_transactions']
    else:
        df_user = ti.xcom_pull(task_ids='extract_daily_task')['df_user']
        df_category = ti.xcom_pull(task_ids='extract_daily_task')['df_category']
        df_payment = ti.xcom_pull(task_ids='extract_daily_task')['df_payment']
        df_mall = ti.xcom_pull(task_ids='extract_daily_task')['df_mall']
        df_gender = ti.xcom_pull(task_ids='extract_daily_task')['df_gender']
        df_transactions = ti.xcom_pull(task_ids='extract_daily_task')['df_transactions']

    #print(df_mall)

    #join transactions with user and get fullname
    df_trans_user = pd.merge(df_transactions, df_user, left_on='user_id', right_on='id', how='left')
    df_trans_user['full_name'] = df_trans_user['firstName'] + ' ' + df_trans_user['lastName']

    #Concat Full Name
    df_trans_user_final = df_trans_user[["invoice_no", "invoice_date", "user_id", "gender_id", "category_id", "quantity", "price", "payment_method_id", "shopping_mall_id", "full_name"]]

    #Join for each column
    df_trans_user_cat = pd.merge(df_trans_user_final, df_category, left_on='category_id', right_on='id', how='left')
    df_trans_user_cat_final = df_trans_user_cat[["invoice_no", "invoice_date", "user_id", "gender_id", "category_id", "quantity", "price", "payment_method_id", "shopping_mall_id", "full_name","category"]]

    df_trans_user_cat_pay = pd.merge(df_trans_user_cat_final, df_payment, left_on='payment_method_id', right_on='id', how='left')
    df_trans_user_cat_pay_final = df_trans_user_cat_pay[["invoice_no", "invoice_date", "user_id", "gender_id", "category_id", "quantity", "price", "payment_method_id", "shopping_mall_id", "full_name","category","payment_method"]]
    
    df_trans_user_cat_pay_mall = pd.merge(df_trans_user_cat_pay_final, df_mall, left_on='shopping_mall_id', right_on='id', how='left')
    df_trans_user_cat_pay_mall_final = df_trans_user_cat_pay_mall[["invoice_no", "invoice_date", "user_id", "gender_id", "category_id", "quantity", "price", "payment_method_id", "shopping_mall_id", "full_name","category","payment_method","shopping_mall"]]

    df_gender['id'] = df_gender['id'].astype(int) 
    df_trans_user_cat_pay_mall_gen = pd.merge(df_trans_user_cat_pay_mall_final, df_gender, left_on='gender_id', right_on='id', how='left')
    df_trans_user_cat_pay_mall_gen_final = df_trans_user_cat_pay_mall_gen[["invoice_no", "invoice_date", "user_id", "gender_id", "category_id", "quantity", "price", "payment_method_id", "shopping_mall_id", "full_name","category","payment_method","shopping_mall","gender_name"]]

    temp_df_final = df_trans_user_cat_pay_mall_gen[["invoice_no","invoice_date","user_id","full_name","gender_id","gender_name","category_id","category","quantity","price","payment_method_id","payment_method","shopping_mall_id","shopping_mall"]]

    #Drop duplicate
    df_no_duplicates = temp_df_final.drop_duplicates()

    #Replace &
    df_no_duplicates['category'] = df_no_duplicates['category'].replace("Food & Beverage", "Food and Beverage")

    #Filter DF price > 0
    df_no_duplicates['price'] = df_no_duplicates['price'].astype(float)
    final_df = df_no_duplicates.loc[df_no_duplicates['price'] > 0]
    
    #print(final_df.head())
    #write to temp table before loading
    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)
    final_df.to_sql(name='temp_data_transactions_fact',con=engine,index=False, if_exists='replace')

    #check record after new temp data loaded
    sql_count = """
    SELECT COUNT(*) as cnt FROM temp_data_transactions_fact as t1
    LEFT JOIN fact_customer_transaction as t2 
    ON t1.invoice_no = t2.invoice_no
    WHERE t2.invoice_no IS NULL;
    """
    df = pd.read_sql_query(sql_count, con=engine)
    
    
    processed_rows = df['cnt'].iloc[0]
    return {'row_processed': processed_rows}

def decide_run_initial(ti):
    str_variable = Variable.get("var_submission",deserialize_json=True)
    initial_status = str_variable["initial_status"]
    if initial_status == "True":
        return 'extract_daily_task'
    else:
        return 'extract_initial_task'


with DAG("ETL_Submission", start_date=datetime(2023, 7, 26, 14, 30, 0, tzinfo=pytz.timezone('Asia/Jakarta')),
    schedule_interval="*/5 * * * *", catchup=False) as dag:

    extract_daily_task = PythonOperator(
        task_id='extract_daily_task',
        python_callable=extract_daily_data,
    )

    extract_initial_task = PythonOperator(
        task_id='extract_initial_task',
        python_callable=extract_initial_data,
    )

    load_data_mart_task = PythonOperator(
        task_id='load_data_mart_task',
        python_callable=load_transactions_dm,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        provide_context=True,
        trigger_rule='one_success',
    )

    decide_run_task = BranchPythonOperator(
        task_id='decide_run_initial',
        python_callable=decide_run_initial
    )

    send_mail_task = PythonOperator(
        task_id='send_mail_task',
        python_callable=send_email_notifications,
    )
    
    load_table_task = PostgresOperator(
        task_id='load_table_task',
        postgres_conn_id='postgres_airflow',
        sql = """
        INSERT INTO fact_customer_transaction (invoice_no,invoice_date,user_id,full_name,gender_id,gender,category_id,category,quantity,price,payment_method_id,payment_method,shopping_mall_id,shopping_mall)
        SELECT t1.invoice_no, t1.invoice_date::date as invoice_date, 
            CAST(t1.user_id AS BIGINT) as user_id, t1.full_name, CAST(t1.gender_id AS BIGINT) as user_id, t1.gender_name, 
            CAST(t1.category_id AS BIGINT) as category_id, t1.category, t1.quantity, t1.price, CAST(t1.payment_method_id AS BIGINT) as payment_method_id, 
            t1.payment_method, CAST(t1.shopping_mall_id AS BIGINT) as shopping_mall_id, t1.shopping_mall
        FROM temp_data_transactions_fact as t1
        LEFT JOIN fact_customer_transaction as t2 
        ON t1.invoice_no = t2.invoice_no
        WHERE t2.invoice_no IS NULL;
        """
    )

    decide_run_task >> [extract_initial_task, extract_daily_task]
    [extract_initial_task, extract_daily_task] >> transform_task
    transform_task >> load_table_task >> send_mail_task >> load_data_mart_task