import boto3
from boto3.dynamodb.conditions import Key, Attr
import sys
import json
import time
from datetime import date, datetime
import string
import os, time

#timezone changes
os.environ['TZ'] = 'America/Los_Angeles'

#global variables
today = date.today()

table_name = "smylee_solar"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)

#############get all the years
def getYear():

	try:
		#get all the values in the table for the current year & month
		record_exist = table.query(
			KeyConditionExpression=Key('type').eq('year')
		)
		output = []
		for items in record_exist['Items']:
			output.append({ 'date': items['date'], 'generated': items['generated'], 'months': [] })
		output = { 'years': output }
		return output
	except Exception as err:
		print("Error occurred:", err)
		sys.exit()

#############get all the months
def getMonths(records):

	counter = 0

	# print records
	try:
		for year_item in records:
			for year_data in records[year_item]:
				# print year_data['date']
				#get all the values in the table for the current year & month
				record_exist = table.query(
					KeyConditionExpression=Key('type').eq('month') & Key('date').begins_with(year_data['date'])
				)
				output = []
				for items in record_exist['Items']:
					output.append({ 'date': items['date'], 'generated': items['generated'], 'days': [] })
				records[year_item][counter]['months'] = output
				counter += 1
		return records
	except Exception as err:
		print("Error occurred:", err)
		sys.exit()


#############get all the days
def getDays(records):

	counter = 0
	counter2 = 0

	# print records
	try:
		for year_item in records:
			for year_data in records[year_item]:
				for month_item in year_data['months']:
					#get all the values in the table for the current year & month
					record_exist = table.query(
						KeyConditionExpression=Key('type').eq('day') & Key('date').begins_with(month_item['date'])
					)
					output = []
					for items in record_exist['Items']:
						output.append({ 'date': items['date'], 'generated': items['generated'] })
					records[year_item][counter]['months'][counter2]['days'] = output
					counter2 += 1
				counter += 1
		return records
	except Exception as err:
		print("Error occurred:", err)
		sys.exit()


#############get filtered data
def getFilter(type, filter):

	try:
		#get the latest value for the day specified
		record_exist = table.query(
			KeyConditionExpression=Key('type').eq(type) & Key('date').begins_with(filter)
		)
		output = []
		for items in record_exist['Items']:
			output.append({ 'date': items['date'], 'generated': items['generated'] })
		return output
	except Exception as err:
		print("Error occurred:", err)
		sys.exit()

def lambda_handler(event, context):
	output = []

	if event['getType'] != "all":
		type = event['getType']
		filter = event['filter']
		output = getFilter(type, filter)

	elif event['getType'] == "all":
		#run the program
		output = getDays(getMonths(getYear()))

	print json.dumps(output)

# event = json.loads('{ "getType": "day", "filter": "2016-07-03" }')
# event = json.loads('{ "getType": "day", "filter": "2016-07" }')
# event = json.loads('{ "getType": "month", "filter": "2016" }')
# event = json.loads('{ "getType": "year", "filter": "2016" }')
# event = json.loads('{ "getType": "year", "filter": "2016" }')
# event = json.loads('{ "getType": "all" }')
# lambda_handler(event, context)