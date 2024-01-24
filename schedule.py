import requests
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
from Data_extraction.extract_data import extract_and_store
from Data_extraction.update_collection import update_data
from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

@asset  
def monthly_updated_data(
    context: AssetExecutionContext):

    current_datetime = datetime.now()

    previous_month_datetime = current_datetime - relativedelta(months=1)

    previous_month_date = previous_month_datetime.date()

    
    # Date to string to name the collection
    formatted_date = current_datetime.strftime("%Y_%m_%d")

    previous_month_date_formated = previous_month_date.strftime("%Y-%m-%d")

    # format the date so that it changes the date at the "from_updated_date" filter
    api_url = "https://api.openalex.org/works?page=1&filter=publication_year:2011-2024,institutions.country_code:DK,from_updated_date:{}&api_key=DFP64JtmA3mNych5RLEHHC&sort=cited_by_count:desc&per_page=200".format(previous_month_date_formated)

    database_name = 'openalex_db'
    new_collection_name = 'updated_data_{0}'.format(formatted_date)

    extract_and_store(api_url, database_name, new_collection_name)

    update_data(new_collection_name)
    
updatedata_job = define_asset_job("updatedata_job", selection=[monthly_updated_data])

#ScheduleDefinition the job it should run and a cron schedule
updatedata_schedule = ScheduleDefinition(
    job=updatedata_job,
    cron_schedule = "0 0 1 * *",  # every month
)


defs = Definitions(
    assets=[monthly_updated_data],
    schedules=[updatedata_schedule]
)




