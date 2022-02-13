import boto3,os,uuid,json,datetime
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key,Attr
from flask import request, jsonify

from dotenv import load_dotenv

load_dotenv("../.env")

def connection():
    try:   
        dynamodb = boto3.resource('dynamodb',region_name="eu-west-1",aws_access_key_id=os.getenv('ACESS_KEY'), \
            aws_secret_access_key=os.getenv('SECRET_KEY'))
        
        table = dynamodb.Table('report-weather')

        print(table.creation_date_time)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return table

def create_weather(ids, sensor_id,temperature, humidity, city, country,air_pollution, dynamodb=None):
    dynamodb = connection()
    timestamp = datetime.datetime.now()
    try:   
        _ = dynamodb.put_item(
            # Data to be inserted
            Item={
                'ID': ids,
                'sensor_ID': sensor_id,
                'Date': str(timestamp),
                'Temperature': temperature,
                'Humidity': humidity,
                'City': city,
                'Country': country,
                'Air Pollution': air_pollution
            },
        )
    except ClientError as e:
         return e.response['Error']['Message']
    else:
       return get_weather_report_id(ids,sensor_id)

def create_weather_reports_batch(dynamodb=None):
    dynamodb = connection()  
    try:     
        with open('./items.json') as json_data:
            items = json.load(json_data)

            with dynamodb.batch_writer() as batch:
                # Loop through the JSON objects
                for item in items:
                    batch.put_item(Item=item)
    except ClientError as e:
        return e.response['Error']['Message']
    else:
        return dict(Status="Success", Message="Items have been uploaded to the database", Code="200")

def update_weather(ids,newMetric,value,dynamodb=None):
    dynamodb = connection()
    try:   
        _ = dynamodb.update_item(
            # Data to be inserted
            Key={
                'ID': ids,
            },
                UpdateExpression='SET {newMetric} = :val1'.format(newMetric = newMetric),
                ExpressionAttributeValues={
                ':val1': value
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
       return get_weather_report_id(ids)
    


def get_weather_report_id(id, dynamodb=None):
    dynamodb = connection()
    try:
        response = dynamodb.get_item(
            Key={'ID': id}
            )
    except ClientError as e:
        return e.response['Error']['Message']
    else:
        if 'Item' in response:
            return response['Item']
        else:
            return (dict(Status="Error", Message="Missing row in database", Code="404"))


def get_weather_report_all(dynamodb=None):
    dynamodb = connection()
    try:
        response = dynamodb.scan()
        data = response['Items']
        while 'LastEvaluatedKey' in response:
            response = dynamodb.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
       
    except ClientError as e:
        return e.response['Error']['Message']
    else:
         return data


def get_weather_report_average_tempture(city,dynamodb=None):
    count = 0
    total = 0
    total_hum = 0
    dynamodb = connection()
    try:
        response = dynamodb.scan(
        FilterExpression=Attr('City').eq(city) 
        )

        valu = [i['Temperature'] for i in response['Items']]
        for val in valu:
            total += val
            count += 1

        hum = [i['Humidity'] for i in response['Items']]
        for val in hum:
            total_hum += val
    except ClientError as e:
        return e.response['Error']['Message']
    else:
        average_temp = average_cal(total,count)  
        average_hum = average_cal(total_hum,count)  
        return average_hum,average_temp
        


def get_weather_report_average_sensor_id(sensor_id,start_date, end_date, dynamodb=None):
    count = 0
    total = 0
    total_hum = 0
    dynamodb = connection()
    try:
        if start_date != "" and end_date != "":
            response = dynamodb.scan(
                FilterExpression=Attr('sensor_ID').eq(sensor_id) & Attr('Date').between(start_date,end_date)
            )
            valu = [i['Temperature'] for i in response['Items']]
            for val in valu:
                total += val
                count += 1
                

            hum = [i['Humidity'] for i in response['Items']]
            for val in hum:
                total_hum += val

            average_temp = average_cal(total,count)  
            average_hum = average_cal(total_hum,count)  

            return average_hum,average_temp
        else:
            response = dynamodb.scan(
                FilterExpression=Attr('sensor_ID').eq(sensor_id),
                Limit=1 
            )
            hum = [i['Humidity'] for i in response['Items']]
            for val in hum:
                total_hum += val
            temp = [i['Temperature'] for i in response['Items']]
            for val in temp:
                total += val

            return total_hum, total
    except ClientError as e:
        return e.response['Error']['Message']


def average_cal(total,count,dynamodb=None):
    average = total / count
    return average