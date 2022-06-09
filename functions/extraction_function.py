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
    print(function)
    print(start_date)
    print(end_date)
    url = API_URL
 
    querystring = {"start_date":start_date,"end_date":end_date,"api_key":API_KEY}

    data = requests.request("GET", url, params=querystring).json()
    date_consult_api = int( time.time() )
    
    if len(data) == 0:
        print('data not found')
    else:
        if len(data['near_earth_objects']) and function == 'extraction' :
            api_endpoint = f"{url}?start_date={start_date}&end_date={end_date}&api_key={API_KEY}"
            data_dict = {
                'data_api': data,
                'date_consult_api': date_consult_api,
                'date_load_data': time.time(),
                'api_endpoint': api_endpoint 
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