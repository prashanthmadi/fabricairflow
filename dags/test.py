from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime
from faker import Faker

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'insert_faker_to_mysql',
    default_args=default_args,
    description='A simple DAG to insert fake data into MySQL',
    schedule_interval='@once',
)

# Function to generate fake data and insert into MySQL
def generate_and_insert_data():
    fake = Faker()
    data = []
    for _ in range(100):  # Generate 100 records
        data.append((fake.user_name(), fake.state(), fake.zipcode()))

    mysql_hook = MySqlHook(mysql_conn_id='your_mysql_connection_id')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    cursor.executemany('INSERT INTO your_table_name (username, state, zipcode) VALUES (%s, %s, %s)', data)
    connection.commit()

    cursor.close()
    connection.close()

# Define the PythonOperator
insert_data_task = PythonOperator(
    task_id='generate_and_insert_data',
    python_callable=generate_and_insert_data,
    dag=dag,
)

# Set the task dependencies
insert_data_task