import boto3
import json
import time
import os 

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
     s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY,region_name='us-east-1')
     s3_object = s3_client.get_object(Bucket='data-bucket-etl-test',Key='extraction/extraction_data.json')
     body = s3_object['Body'].read()
     data_json = json.loads(body)
     
     near_earth_objects = data_json["data_api_objects"]['near_earth_objects']
     events = data_json["data_api_events"]['events']
     photos = data_json["data_api_photos"]['photos']
     api_endpoint_objects = data_json['api_endpoint_objects']
     api_endpoint_events = data_json['api_endpoint_events']
     api_endpoint_photos = data_json['api_endpoint_photos']
     
     data_near_earth_objects = []
     data_events = []
     data_photos = []
     
     dates = [start_date,end_date]
     
     for date in dates:
         for row in near_earth_objects[date]:
             data_element = {
                  'id': row['id'],
                  'name': row['name'],
                  'nasa_jpl_url': row['nasa_jpl_url'],
                  'absolute_magnitude_h': row['absolute_magnitude_h'],
                  'estimated_diameter': row['estimated_diameter']['kilometers'],
                  'is_potentially_hazardous_asteroid': row['is_potentially_hazardous_asteroid'],
                  'relative_velocity': row['close_approach_data'][0]['relative_velocity'],
                  'is_sentry_object': row['is_sentry_object'],
                  'date': date,
                  'api_endpoint': api_endpoint_objects
             }
             data_near_earth_objects.append(data_element)
     
     for row in events:
         data_element = {
             'id': row['id'],
             'title': row['title'],
             'description': row['description'],
             'source': row['sources'][0]['url'],
             'latitude': row['geometries'][0]['coordinates'][0],
             'longitude': row['geometries'][0]['coordinates'][1],
             'api_endpoint': api_endpoint_events
         }
         data_events.append(data_element)
    
     for row in photos:
         data_element = {
             'id': row['id'],
             'camera': row['camera']['full_name'],
             'img_src': row['img_src'],
             'earth_date': row['earth_date'],
             'rover': row['rover']['name'],
             'api_endpoint': api_endpoint_photos
         }
         data_photos.append(data_element)
            
     transform_data = {
         'date_load_data': time.time(),
         'data_near_earth_objects': data_near_earth_objects,
         'data_events': data_events,
         'data_photos': data_photos
     } 
     if function == 'transform':
         response_load = load_json(transform_data)
         if response_load[0]:
             print(response_load[1])
             response_lambda = run_lambda()
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
        upload_file_key = 'transform/transform_data'
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


def run_lambda():
    topicArn = 'arn:aws:sns:us-east-1:208060198737:LoadFunction'
    snsClient = boto3.client(
        'sns',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name='us-east-1'
    )
    publishOject = {"function": "load", "limit": 50}
    response = snsClient.publish(TopicArn=topicArn,
                                 Message=json.dumps(publishOject),
                                 Subject='FUNCTION',
                                 MessageAttributes={"TransactionType": {"DataType": "String", "StringValue": "FUNCTION"}})

    status = response['ResponseMetadata']['HTTPStatusCode']
    if status == 200:
        return True
    else:
        return False