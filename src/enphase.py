import requests
import sys


class Enphase(object):

    def __init__(self, ssm_client):
        self._ssm_client = ssm_client
        self._enphase_access_token = None
        self._enphase_api_key = None
        self._enphase_api_url = None
        self._enphase_client_id = None
        self._enphase_client_secret = None
        self._enphase_refresh_token = None
        self._enphase_system_id = None

    def get_enphase_new_token(self) -> None:
        try:
            response = requests.post(f'https://api.enphaseenergy.com/oauth/token?grant_type=refresh_token&refresh_token={self._enphase_refresh_token}',
                                     data='',
                                     auth=(self._enphase_client_id, self._enphase_client_secret))
            json_response = response.json()
            self._enphase_access_token = json_response['access_token']
            self._enphase_refresh_token = json_response['refresh_token']
        except Exception as err:
            print("Error occurred:", err)
            sys.exit()

    def update_enphase_token(self) -> None:
        try:
            self._ssm_client.put_parameter(
                Name='/enphase/access_token_v4',
                Value=self._enphase_access_token,
                Type='SecureString',
                KeyId='alias/smylee_com',
                Overwrite=True,
            )
            self._ssm_client.put_parameter(
                Name='/enphase/refresh_token_v4',
                Value=self._enphase_refresh_token,
                Type='SecureString',
                KeyId='alias/smylee_com',
                Overwrite=True,
            )

        except Exception as err:
            print("Error occurred:", err)
            sys.exit()

    def authenticate(self) -> None:
        try:
            results = self._ssm_client.get_parameters(
                Names=[
                    '/enphase/access_token_v4',
                    '/enphase/api_key_v4',
                    '/enphase/api_url',
                    '/enphase/client_id_v4',
                    '/enphase/client_secret_v4',
                    '/enphase/refresh_token_v4',
                    '/enphase/system_id',
                ],
                WithDecryption=True
            )

            self._enphase_access_token = next(
                item['Value'] for item in results['Parameters'] if item['Name'] == '/enphase/access_token_v4')
            self._enphase_api_key = next(
                item['Value'] for item in results['Parameters'] if item['Name'] == '/enphase/api_key_v4')
            self._enphase_api_url = next(
                item['Value'] for item in results['Parameters'] if item['Name'] == '/enphase/api_url')
            self._enphase_client_id = next(
                item['Value'] for item in results['Parameters'] if item['Name'] == '/enphase/client_id_v4')
            self._enphase_client_secret = next(
                item['Value'] for item in results['Parameters'] if item['Name'] == '/enphase/client_secret_v4')
            self._enphase_refresh_token = next(
                item['Value'] for item in results['Parameters'] if item['Name'] == '/enphase/refresh_token_v4')
            self._enphase_system_id = next(
                item['Value'] for item in results['Parameters'] if item['Name'] == '/enphase/system_id')

        except Exception as err:
            print("Error occurred:", err)
            sys.exit()

    def get_summary(self, start_time: float, end_time: float = None) -> {}:
        try:
            endpoint = f'{self._enphase_api_url}/systems/{self._enphase_system_id}/telemetry/production_micro?key={self._enphase_api_key}&granularity=day&start_at={start_time}'
            if end_time:
                endpoint += f'&end_time={end_time}'
            response = requests.get(endpoint,
                                    headers={
                                        'Authorization': f'Bearer {self._enphase_access_token}'
                                    })
            return response.json()

        except Exception as err:
            print("Error occurred:", err)
            sys.exit()
