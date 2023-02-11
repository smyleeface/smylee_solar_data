import datetime
import unittest
from unittest.mock import patch, MagicMock, call

from src.put_data.etl_data import EtlData


class TestEtlData(unittest.TestCase):
    def setUp(self) -> None:
        self._report_date = datetime.datetime.fromisoformat('2023-01-06T18:24:22')
        self._day_summary = {
            'intervals': [
                {'enwh': 100},
                {'enwh': 300},
            ]
        }
        self._dynamodb_client_mock = MagicMock()
        self._enphase_client_mock = MagicMock()
        self._enphase_client_mock.get_summary.return_value = {
            'details': []
        }
        self._dynamodb_client_mock.query.return_value = {
            'Count': 2,
            'Items': [
                {'generated': 100},
                {'generated': 200}
            ]
        }
        self._etl_data = EtlData(self._dynamodb_client_mock, self._enphase_client_mock)

    def tearDown(self) -> None:
        self._dynamodb_client_mock.reset_mock()
        self._enphase_client_mock.reset_mock()
        self._etl_data = None

    def test_convert_to_timestamp(self):
        """it should convert a date in string format to a timestamp format
        """

        # Arrange
        # Act
        result = self._etl_data.convert_to_timestamp('2023-01-01T12:00Z')

        # Assert
        self.assertEqual(result, '1672560000')

    @patch.object(EtlData, 'calculate_year')
    def test_put_year(self, calculate_year_mock):
        """put year total into dynamodb
        """

        # Arrange
        calculate_year_mock.return_value = 300

        # Act
        self._etl_data.put_year(self._report_date)

        # Assert
        self._dynamodb_client_mock.put_item.assert_called_with(Item={'PK': 'YEAR', 'SK': '2023', 'generated': '300'})

    @patch.object(EtlData, 'calculate_month')
    def test_put_month(self, calculate_month_mock):
        """put month total into dynamodb
        """

        # Arrange
        calculate_month_mock.return_value = 300

        # Act
        self._etl_data.put_month(self._report_date)

        # Assert
        self._dynamodb_client_mock.put_item.assert_called_with(Item={'PK': 'MONTH', 'SK': '2023-01', 'generated': '300'})

    @patch.object(EtlData, 'calculate_day')
    def test_put_day(self, calculate_day_mock):
        """put day total into dynamodb
        """

        # Arrange
        calculate_day_mock.return_value = 300

        # Act
        self._etl_data.put_day(self._report_date)

        # Assert
        self._dynamodb_client_mock.put_item.assert_called_with(Item={'PK': 'DAY', 'SK': '2023-01-06', 'generated': '300'})

    def test_calculate_year(self):
        """should return total generated for the year
        """

        # Arrange

        # Act
        result = self._etl_data.calculate_year(self._report_date)

        # Assert
        self.assertEqual(result, 300)

    def test_calculate_month(self):
        """should return total generated for the month
        """

        # Arrange

        # Act
        result = self._etl_data.calculate_month(self._report_date)

        # Assert
        self.assertEqual(result, 300)

    @patch.object(EtlData, 'get_day_values')
    def test_calculate_day(self, get_day_values_mock):
        """should return total generated for the month
        """

        # Arrange
        get_day_values_mock.return_value = self._day_summary

        # Act
        result = self._etl_data.calculate_day(self._report_date)

        # Assert
        self.assertEqual(result, 0.4)

    def test_get_day_values(self):
        """should return total generated for the month
        """

        # Arrange
        self._enphase_client_mock.get_summary.return_value = self._day_summary

        # Act
        result = self._etl_data.get_day_values(self._report_date)

        # Assert
        self.assertEqual(result, self._day_summary)
