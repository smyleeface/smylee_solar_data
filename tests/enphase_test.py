import unittest
from unittest.mock import patch, MagicMock, call

from src.enphase import Enphase


class TestEnphase(unittest.TestCase):

    def setUp(self) -> None:
        ssm_client_mock = MagicMock()
        ssm_client_mock.put_parameter = MagicMock()
        ssm_client_mock.get_parameters.return_value = {
            'Parameters': [
                {
                    'Name': '/enphase/access_token_v4',
                    'Value': 'foo'
                },
                {
                    'Name': '/enphase/api_key_v4',
                    'Value': 'bar'
                },
                {
                    'Name': '/enphase/api_url',
                    'Value': 'bat'
                },
                {
                    'Name': '/enphase/client_id_v4',
                    'Value': 'baz'
                },
                {
                    'Name': '/enphase/client_secret_v4',
                    'Value': 'qux'
                },
                {
                    'Name': '/enphase/refresh_token_v4',
                    'Value': 'grault'
                },
                {
                    'Name': '/enphase/system_id',
                    'Value': 'fred'
                }
            ]
        }

        access_token_mock = MagicMock()
        access_token_mock.json.return_value = {
            'access_token': 'foobar',
            'refresh_token': 'batbaz'
        }

        get_summary_mock = MagicMock()
        get_summary_mock.return_value = {
            'intervals': [
                {'end_at': 1673016600, 'devices_reporting': 8, 'powr': 1, 'enwh': 0},
                {'end_at': 1673016600, 'devices_reporting': 8, 'powr': 1, 'enwh': 0}
            ]
        }
        self._ssm_client_mock = ssm_client_mock
        self._access_token_mock = access_token_mock
        self._get_summary_mock = get_summary_mock
        self._enphase = Enphase(self._ssm_client_mock)
        self._enphase.authenticate()

    def tearDown(self) -> None:
        self._ssm_client_mock = None
        self._access_token_mock = None
        self._get_summary_mock = None
        self._enphase = None

    @patch('src.enphase.requests')
    def test_get_enphase_new_token(self, requests_mock):
        """it should get a new token from enphase and store it in the private variables
        """

        # Arrange
        requests_mock.post.return_value = self._access_token_mock

        # Act
        self._enphase.get_enphase_new_token()

        # Assert
        requests_mock.post.assert_any_call('bat/oauth/token?grant_type=refresh_token&refresh_token=grault',
                                           data='',
                                           auth=('baz', 'qux'))
        self._enphase.update_enphase_token()
        self._ssm_client_mock.put_parameter.assert_has_calls([
            call(Name='/enphase/access_token_v4', Value='foobar', Type='SecureString', KeyId='alias/smylee_com',
                 Overwrite=True),
            call(Name='/enphase/refresh_token_v4', Value='batbaz', Type='SecureString', KeyId='alias/smylee_com',
                 Overwrite=True),
        ])

    @patch('src.enphase.requests')
    def test_update_enphase_token(self, requests_mock):
        """it should update the enphase token on aws
        """

        # Arrange
        # Act
        self._enphase.update_enphase_token()

        # Assert
        self._ssm_client_mock.put_parameter.assert_has_calls([
            call(Name='/enphase/access_token_v4', Value='foo', Type='SecureString', KeyId='alias/smylee_com',
                 Overwrite=True),
            call(Name='/enphase/refresh_token_v4', Value='grault', Type='SecureString', KeyId='alias/smylee_com',
                 Overwrite=True),
        ])

    @patch('src.enphase.requests')
    def test_get_summary(self, requests_mock):
        """it should get the summary of power generation from enphase with start id only
        """

        # Arrange
        requests_mock.get.return_value = self._get_summary_mock

        # Act
        self._enphase.get_summary(1673419073)

        # Assert
        requests_mock.get.assert_any_call(f'bat/systems/fred/telemetry/production_micro?key=bar&granularity=day&start_at=1673419073',
                                          headers={
                                              'Authorization': f'Bearer foo'
                                          })

    @patch('src.enphase.requests')
    def test_get_summary_with_end_time(self, requests_mock):
        """it should get the summary of power generation from enphase with end time
        """

        # Arrange
        requests_mock.get.return_value = self._get_summary_mock

        # Act
        self._enphase.get_summary(1673419073, 1673505474)

        # Assert
        requests_mock.get.assert_any_call(f'bat/systems/fred/telemetry/production_micro?key=bar&granularity=day&start_at=1673419073&end_time=1673505474',
                                          headers={
                                              'Authorization': f'Bearer foo'
                                          })
