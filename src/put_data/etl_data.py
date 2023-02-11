import datetime

from boto3.dynamodb.conditions import Key


class EtlData(object):
    def __init__(self, dynamodb_client, enphase_client):
        self._dynamodb_client = dynamodb_client
        self._enphase_client = enphase_client

    def get_day_values(self, report_date: datetime) -> {}:
        reporting_day = datetime.datetime(
            year=report_date.year,
            month=report_date.month,
            day=report_date.day
        ).astimezone(datetime.timezone.utc)
        day_summary = self._enphase_client.get_summary(round(reporting_day.timestamp()))
        if day_summary.get('code', 200) != 200:
            raise Exception(day_summary['details'])
        return day_summary

    def calculate_day(self, report_date: datetime) -> float:
        day_summary = self.get_day_values(report_date)
        total = 0
        for interval in day_summary['intervals']:
            total += interval['enwh']
        return round(total * .001, 3)

    def calculate_month(self, report_date: datetime) -> float:
        record_exist = self._dynamodb_client.query(
            KeyConditionExpression=Key('PK').eq('DAY') & Key('SK').begins_with(f'{report_date.year}-{report_date.month:02d}')
        )
        total = 0
        if record_exist['Count'] == 0:
            return total
        for items in record_exist['Items']:
            total += float(items['generated'])
        return total

    def calculate_year(self, report_date: datetime) -> float:
        record_exist = self._dynamodb_client.query(
            KeyConditionExpression=Key('PK').eq('MONTH') & Key('SK').begins_with(str(report_date.year))
        )
        total = 0
        if record_exist['Count'] == 0:
            return total
        for items in record_exist['Items']:
            total += float(items['generated'])
        return total

    def put_day(self, report_date: datetime) -> None:
        day_total = {
            'PK': 'DAY',
            'SK': report_date.strftime('%Y-%m-%d'),
            'generated': str(self.calculate_day(report_date))
        }
        self._dynamodb_client.put_item(
            Item=day_total
        )

    def put_month(self, report_date: datetime) -> None:
        month_total = {
            'PK': 'MONTH',
            'SK': report_date.strftime('%Y-%m'),
            'generated': str(self.calculate_month(report_date))
        }
        self._dynamodb_client.put_item(
            Item=month_total
        )

    def put_year(self, report_date: datetime) -> None:
        year_total = {
            'PK': 'YEAR',
            'SK': report_date.strftime('%Y'),
            'generated': str(self.calculate_year(report_date))
        }
        self._dynamodb_client.put_item(
            Item=year_total
        )

    @staticmethod
    def convert_to_timestamp(report_date: str) -> str:
        report_datetime = datetime.datetime.fromisoformat(report_date[:-1])
        report_datetime = report_datetime.replace(hour=0, minute=0, second=0)
        report_timestamp = report_datetime.astimezone(datetime.timezone.utc).timestamp()
        return str(round(report_timestamp))
