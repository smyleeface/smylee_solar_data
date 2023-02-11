import datetime

import boto3

from enphase import Enphase
from put_data.etl_data import EtlData

ssm_client = boto3.client('ssm')
dynamodb = boto3.resource('dynamodb')
dynamodb_client = dynamodb.Table('SolarData')
enphase_client = Enphase(ssm_client)
enphase_client.authenticate()


def lambda_handler(event, context):
    print(event)
    report_date = datetime.datetime.fromisoformat(event['time'][:-1])
    etl_data = EtlData(dynamodb_client, enphase_client)
    etl_data.put_day(report_date)
    etl_data.put_month(report_date)
    etl_data.put_year(report_date)
