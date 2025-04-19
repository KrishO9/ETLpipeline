from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

with DAG(
    dag_id = 'etl_pipeline',
    start_date = days_ago(1),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    # Step1: Create db tables if it does not exists

    @task
    def create_table():
        ## initialziing the postgres hook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        #Creating table if it does not exist

        

        query = '''
        CREATE TABLE IF NOT EXISTS apod_data(
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
            );
        '''

        #Execute query
        postgres_hook.run(query)


    # Step2: Extract the NASA API data(APOD)

    extract_apod = SimpleHttpOperator(
        task_id = 'extract_apod',
        http_conn_id = 'nasa_api', #connection ID defined in Airflow for NASA API
        endpoint = 'planetary/apod', # NASA API endpoint for APOD
        method='GET', 
        data = {"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"}, # Use the API key from Airflow connections
        response_filter = lambda response:response.json(), # convert response to json
    )



    # Step3: Transform the data
    @task
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data


    # Step4: Load the data into Postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        #Initiailize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_conn')

        insert_query = '''
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        '''

        #Execute insert query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type'],
        ))

    # Step5 Verify the data in DBViewer



    # Step6 Define the task dependencies
    create_table() >> extract_apod ## Ensure the table is created before extracting data from API
    #Extract
    api_response = extract_apod.output
    #transform
    transformed_data = transform_apod_data(api_response)
    #load
    load_data_to_postgres(transformed_data)