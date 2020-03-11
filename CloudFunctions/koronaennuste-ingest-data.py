import base64

import requests
import datetime
import json
import ndjson
import pandas as pd

def convert_timestamp(record):
    record['date'] = record['date'][:-5]
    return record

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def ingest_pubsub(event, context):

    # import libraries
    from google.cloud import storage

    # get datetime
    now = datetime.datetime.now() + datetime.timedelta(hours=6) # Just to make sure it goes to current day


    # set storage client
    client2 = storage.Client()

    # get bucket
    bucket = client2.get_bucket('koronaennuste-bucket') # without gs://
    confirmed_latest_file = bucket.blob('latest/confirmed.csv')
    daily_statistics_latest_file = bucket.blob('latest/daily_statistics.csv')
    confirmed_archive_file = bucket.blob(f"{now.year}/{now.month}/{now.day}/confirmed.csv")
    daily_statistics_archive_file = bucket.blob(f"{now.year}/{now.month}/{now.day}/daily_statistics.csv")

    # See if csv exists
    if confirmed_archive_file.exists() == False and daily_statistics_archive_file.exists() == False:
        # copy file to google storage
        try:
            r = requests.get('https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData')
            content = json.loads(r.content)
            confirmed = content['confirmed']
            df_confirmed = pd.DataFrame(confirmed)
            for record in confirmed:
                record['date'] = record['date'][:-14]

            ## Iterate over days
            # The size of each step in days
            day_delta = datetime.timedelta(days=1)
            start_date = datetime.date(2020, 2, 26)
            print(start_date)
            end_date = datetime.date.today()
            print(end_date)
            daily_statistics = []
            for i in range((end_date - start_date).days):
                date = str(start_date + i*day_delta)
                new_cases = sum(record['date'] == date for record in confirmed)
                if i == 0:
                    total_cases = new_cases + 1
                else:
                    total_cases = daily_statistics[-1]['total_cases'] + new_cases
                daily_statistics.append({'date': date, 'new_cases': new_cases, 'total_cases': total_cases, 'active_cases': total_cases - 1})
            df_daily_statistics = pd.DataFrame(daily_statistics)
            csv_confirmed = df_confirmed.to_csv()
            csv_daily_statistics = df_daily_statistics.to_csv()

            ## Upload data
            confirmed_latest_file.upload_from_string(
                data = csv_confirmed
            )
            confirmed_archive_file.upload_from_string(
                data = csv_confirmed
            )
            daily_statistics_latest_file.upload_from_string(
                data = csv_daily_statistics
            )
            daily_statistics_archive_file.upload_from_string(
                data = csv_daily_statistics
            )
        except:
            print('Error with HTTP GET')
            raise
    else:
        print('archive_file already exists in google storage')
