import boto3

from enphase import Enphase

ssm_client = boto3.client('ssm')


def lambda_handler(event, context):
    enphase = Enphase(ssm_client)
    enphase.authenticate()
    enphase.get_enphase_new_token()
    enphase.update_enphase_token()
