import requests
import json
import boto3
import time
import os 

# enviroment variables
API_HOST = os.getenv('API_HOST')
API_KEY = os.getenv('API_KEY')
API_URL_OBJECTS = os.getenv('API_URL_OBJECTS')
API_URL_EVENTS = os.getenv('API_URL_EVENTS')
API_URL_PHOTOS = os.getenv('API_URL_PHOTOS')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_ACCESS_KEY = os.getenv('SECRET_ACCESS_KEY')

def lambda_handler(event, context):
    message = event['Records'][0]['Sns']['Message']
    json_message = json.loads(message)
    function = json_message['function']
    start_date = json_message['start_date']
    end_date = json_message['end_date']
    earth_date = json_message['earth_date']
    days = json_message['days']
    print(function)
    print(start_date)
    print(end_date)
    print(earth_date)
    print(days)
    url_objects = API_URL_OBJECTS
    url_events = API_URL_EVENTS
    url_photos = API_URL_PHOTOS
 
    query_string_objects = {"start_date":start_date,"end_date":end_date,"api_key":API_KEY}
    query_string_events = {"days":days}
    query_string_photos = {"earth_date":earth_date,"api_key":API_KEY}

    data_objects = requests.request("GET", url_objects, params=query_string_objects).json()
    data_events = requests.request("GET", url_events, params=query_string_events).json()
    data_photos = requests.request("GET", url_photos, params=query_string_photos).json()
    date_consult_api = int( time.time() )
    
    if len(data_objects) == 0 or len(data_events) == 0 or len(data_photos) == 0:
        print('data not found')
    else:
        if function == 'extraction' :
            api_endpoint_objects = f"{url_objects}?start_date={start_date}&end_date={end_date}&api_key={API_KEY}"
            api_endpoint_events = f"{url_events}?days={days}"
            api_endpoint_photos = f"{url_photos}?earth_date={earth_date}&api_key={API_KEY}"
            data_dict = {
                'data_api_objects': data_objects,
                'data_api_events': data_events,
                'data_api_photos': data_photos,
                'date_consult_api': date_consult_api,
                'date_load_data': time.time(),
                'api_endpoint_objects': api_endpoint_objects,
                'api_endpoint_events': api_endpoint_events,
                'api_endpoint_photos': api_endpoint_photos 
            }
            response_load = load_json(data_dict)
            if response_load[0]:
                 print(response_load[1])
                 response_lambda = run_lambda(start_date, end_date)
                 if response_lambda:
                     print('Lambda executed Successfully')
                 else:
                     print('Lambda executed Unsuccessful') 
            else:
                print(response_load[1])
           

def load_json(data):
    try:
        # save to s3
        destination_s3_bucket = 'data-bucket-etl-test'
        upload_file_key = 'extraction/extraction_data'
        filepath =  upload_file_key + ".json"
        #
        s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY,region_name='us-east-1')
        response = s3_client.put_object(
            Bucket=destination_s3_bucket, Key=filepath, Body=(bytes(json.dumps(data).encode('UTF-8')))
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            return [True,f"Successful S3 put_object response. Status - {status}"]
        else:
            return [False,f"Unsuccessful S3 put_object response. Status - {status}"]
           
    except Exception as e:
        print("Data load error: " + str(e))


def run_lambda(start_date, end_date):
    topicArn = 'arn:aws:sns:us-east-1:208060198737:TransformFunction'
    snsClient = boto3.client(
        'sns',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name='us-east-1'
    )
    publishOject = {"function": "transform", "start_date": str(start_date), "end_date": str(end_date)}
    response = snsClient.publish(TopicArn=topicArn,
                                 Message=json.dumps(publishOject),
                                 Subject='FUNCTION',
                                 MessageAttributes={"TransactionType": {"DataType": "String", "StringValue": "FUNCTION"}})

    status = response['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        return True
    else:
        return False