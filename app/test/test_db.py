import unittest
import boto3
from unittest.mock import Mock
from moto import mock_dynamodb2
from boto3.dynamodb.conditions import Attr
import pytest

from app import db

class DynamoDBMockTest(unittest.TestCase):

  def test_connection(self):
      dynamodb = boto3.resource('dynamodb','eu-west-1')
      table_name = 'report-weather'
      table = dynamodb.Table(table_name)
      assert db.connection() == table
  
  def test_fetch_from_table(self):
      data = {     
        'Air Pollution': 4,
        'City': 'Cork',
        'Country': 'Ireland',
        'Date': '2022-02-16 13:03:57',
        'Humidity': 70,
        'ID': 6,
        'Temperature': 12,
        'sensor_ID': 2,
        'stuff': "value"
    }
     
      res = db.get_weather_report_id(6)
      self.assertEqual(res, data)
  
  def test_fetch_average_from_table_with_city(self):
      data = (    
        30,
        15
      )   
      res = db.get_weather_report_average_tempture("Galway")
      self.assertEqual(res, data)
  
  def test_fetch_average_from_table_with_id(self):
      data = (    
        10,
        20
      )   
      res = db.get_weather_report_average_sensor_id(1,"","")
      self.assertEqual(res, data)
  
  def test_update_table(self):
      data = {     
        'Air Pollution': 4,
        'City': 'Cork',
        'Country': 'Ireland',
        'Date': '2022-02-16 13:03:57',
        'Humidity': 70,
        'ID': 6,
        'Temperature': 12,
        'sensor_ID': 2,
        'stuff': "value"
    }
     
      res = db.update_weather(6, "stuff", "value")
      self.assertEqual(res, data)
  
  def test_average_cal(self):
      data = 3.3333333333333335
     
      res = db.average_cal(10, 3)
      self.assertEqual(res, data)
  

  @mock_dynamodb2
  def test_write_into_table(self):
      dynamodb = boto3.resource('dynamodb','eu-west-1')
      table_name = 'test'
      table = dynamodb.create_table(TableName = table_name,
                                    KeySchema = [
                                    {'AttributeName': 'ID', 'KeyType': 'HASH'}],
                                    AttributeDefinitions = [
                                    {'AttributeName': 'ID', 'AttributeType': 'N'}])
      # item = {}
      data = {     
        'Air Pollution': 2,
        'City': 'Galway',
        'Country': 'Ireland',
        'Date': '2022-02-12 13:03:57',
        'Humidity': 58,
        'ID': 1,
        'Temperature': 12,
        'sensor_ID': 1
    }
      # store.write(data, table_name)
      response = table.put_item(Item=data)
      self.assertEqual(200, response['ResponseMetadata']['HTTPStatusCode'])

  