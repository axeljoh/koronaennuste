import base64
import os

import requests
import datetime
import json
import pandas as pd

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def ingest_pubsub(event, context):

    # import libraries
    from google.cloud import storage

    # get datetime
    now = datetime.datetime.now()


    # set storage client
    client2 = storage.Client()

    # get bucket
    bucket = client2.get_bucket('koronaennuste-bucket') # without gs://
    confirmed_latest_file = bucket.blob('latest/confirmed.csv')
    recovered_latest_file = bucket.blob('latest/recovered.csv')
    deaths_latest_file = bucket.blob('latest/deaths.csv')
    daily_statistics_latest_file = bucket.blob('latest/daily_statistics.csv')
    confirmed_archive_file = bucket.blob(f"{now.year}/{now.month}/{now.day}/confirmed.csv")
    recovered_archive_file = bucket.blob(f"{now.year}/{now.month}/{now.day}/recovered.csv")
    deaths_archive_file = bucket.blob(f"{now.year}/{now.month}/{now.day}/deaths.csv")
    daily_statistics_archive_file = bucket.blob(f"{now.year}/{now.month}/{now.day}/daily_statistics.csv")

    # See if csv exists
    if confirmed_archive_file.exists() == False and recovered_archive_file.exists() == False and deaths_archive_file.exists() == False and daily_statistics_archive_file.exists() == False:
        # copy file to google storage
        try:
            r = requests.get(os.environ['API_URL'])
            content = json.loads(r.content)
            confirmed = content['confirmed']
            recovered = content['recovered']
            deaths = content['deaths']
            for record in confirmed:
                record['healthCareDistrict'] = record['healthCareDistrict'] or 'Tuntematon'
            for record in recovered:
                record['healthCareDistrict'] = record['healthCareDistrict'] or 'Tuntematon'
            for record in deaths:
                record['healthCareDistrict'] = record['healthCareDistrict'] or 'Tuntematon'
            df_confirmed = pd.DataFrame(confirmed)
            df_recovered = pd.DataFrame(recovered)
            df_deaths = pd.DataFrame(deaths)
            csv_confirmed = df_confirmed.to_csv()
            csv_recovered = df_recovered.to_csv()
            csv_deaths = df_deaths.to_csv()
            for record in confirmed:
                record['date'] = record['date'][:-14]
            for record in recovered:
                record['date'] = record['date'][:-14]
            for record in deaths:
                record['date'] = record['date'][:-14]
            ## Iterate over days
            # The size of each step in days
            day_delta = datetime.timedelta(days=1)
            start_date = datetime.date(2020, 2, 26)
            end_date = datetime.date.today() - datetime.imedelta(days=3)
            daily_statistics = []
            for i in range((end_date - start_date).days):
                date = str(start_date + i*day_delta)
                new_cases = sum(record['date'] == date for record in confirmed)
                new_recoveries = sum(record['date'] == date for record in recovered)
                new_deaths = sum(record['date'] == date for record in deaths)
                new_cases_last_week = new_cases + sum(record['new_cases'] for record in daily_statistics[-6:])
                if i == 0:
                    total_cases = new_cases + 1
                    total_recoveries = new_recoveries + 1
                    total_deaths = new_deaths + 0
                else:
                    total_cases = daily_statistics[-1]['total_cases'] + new_cases
                    total_recoveries = daily_statistics[-1]['total_recoveries'] + new_recoveries
                    total_deaths = daily_statistics[-1]['total_deaths'] + new_deaths
                daily_statistics.append({
                    'date': date,
                    'new_cases': new_cases,
                    'new_cases_last_week': new_cases_last_week,
                    'total_cases': total_cases,
                    'new_recoveries': new_recoveries,
                    'total_recoveries': total_recoveries,
                    'new_deaths': new_deaths,
                    'total_deaths': total_deaths,
                    'active_cases': total_cases - total_recoveries - total_deaths})
            df_daily_statistics = pd.DataFrame(daily_statistics)
            csv_daily_statistics = df_daily_statistics.to_csv()

            ## Upload data
            confirmed_latest_file.upload_from_string(
                data = csv_confirmed
            )
            confirmed_archive_file.upload_from_string(
                data = csv_confirmed
            )
            recovered_latest_file.upload_from_string(
                data = csv_recovered
            )
            recovered_archive_file.upload_from_string(
                data = csv_recovered
            )
            deaths_latest_file.upload_from_string(
                data = csv_deaths
            )
            deaths_archive_file.upload_from_string(
                data = csv_deaths
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
