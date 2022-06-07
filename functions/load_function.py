import psycopg2
import boto3
import json
import os

ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_ACCESS_KEY = os.getenv('SECRET_ACCESS_KEY')


def lambda_handler(event, context):
    message = event['Records'][0]['Sns']['Message']
    json_message = json.loads(message)
    function = json_message['function']
    print(function)
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASS')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_pass,
            port=db_port)

        cur = conn.cursor()

        cur.execute('DROP TABLE IF EXISTS Object')

        create_script = ''' CREATE TABLE IF NOT EXISTS Object (
                                id    SERIAL PRIMARY KEY,
                                name  varchar(40) NOT NULL,
                                nasa_jpl_url  varchar(255) NOT NULL,
                                api_endpoint  varchar(255) NOT NULL,
                                absolute_magnitude_h decimal,
                                estimated_diameter json,
                                is_potentially_hazardous_asteroid bool,
                                is_sentry_object bool,
                                relative_velocity json,
                                date_object date
                         )'''
        cur.execute(create_script)

        s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                                 aws_secret_access_key=SECRET_ACCESS_KEY, region_name='us-east-1')
        s3_object = s3_client.get_object(
            Bucket='data-bucket-etl-test', Key='transform/transform_data.json')
        body = s3_object['Body'].read()
        data_json = json.loads(body)

        data_objects = data_json['data_near_earth_objects']

        for data in data_objects:
            insert_script = 'INSERT INTO Object (name, nasa_jpl_url , api_endpoint, absolute_magnitude_h, estimated_diameter,is_potentially_hazardous_asteroid,is_sentry_object,relative_velocity,date_object ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
            insert_value = (data['name'],
                            data['nasa_jpl_url'],
                            data['api_endpoint'],
                            data['absolute_magnitude_h'],
                            json.dumps(data['estimated_diameter']),
                            data['is_potentially_hazardous_asteroid'],
                            data['is_sentry_object'],
                            json.dumps(data['relative_velocity']),
                            data['date'],)
            cur.execute(insert_script, insert_value)

        conn.commit()

    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
