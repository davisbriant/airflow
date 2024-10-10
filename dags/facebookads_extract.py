import logging
from datetime import date, datetime, time, timedelta
from airflow.decorators import dag, task
import airflow
from include.utils import facebookads_utils
import os, sys
import yaml
import pendulum
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config/', 'facebookads.yaml')
with open(config_file_path) as config_file:
    config = yaml.safe_load(config_file)

default_args = {
    'owner': 'airflow'
    ,'depends_on_past': False
    ,'start_date': airflow.utils.dates.days_ago(2)
    ,'email': config['alerting']['emails']
    ,'email_on_failure': True
    ,'email_on_retry': False
    ,'retries': 0
    ,'retry_delay': timedelta(minutes=5)
}

r_session = requests.Session()
retry = Retry(connect=3, backoff_factor=5)
adapter = HTTPAdapter(max_retries=retry)
r_session.mount('http://', adapter)
r_session.mount('https://', adapter)
headers = facebookads_utils.extractReports(config, r_session).getToken()
r_session.headers.update(headers)
extractReports = facebookads_utils.extractReports(config, r_session)

# @dag(schedule_interval="@hourly", catchup=False, default_args=default_args)
@dag(schedule_interval=None, catchup=False, default_args=default_args)
def facebookads_extract():
    @task(task_id="getToken")
    def getToken():
        headers = facebookads_utils.extractReports(config, r_session).getToken()
        return headers
    @task(task_id="getAdAccounts", retries=0)
    def getAdAccounts():
        response = extractReports.getAdAccounts()
        return response
    @task(task_id="getAdCampaigns", retries=0)
    def getAdCampaigns(accountId):
        response = extractReports.getAdCampaigns(accountId)
        return response
    @task(task_id="getAdSets", retries=0)
    def getAdSets(accountId):
        response = extractReports.getAdSets(accountId)
        return response
    @task(task_id="getAds", retries=0)
    def getAds(accountId):
        response = extractReports.getAds(accountId)
        return response
    @task(task_id="getCreatives", retries=0)
    def getCreatives(accountId):
        response = extractReports.getCreatives(accountId)
        return response
    @task(task_id="getCreativePerformanceReport", retries=0)
    def getCreativePerformanceReport(accountId):
        response = extractReports.getCreativePerformanceReport(accountId)
        return response

    # token = getToken()
    adAccounts = getAdAccounts()
    adCampaigns = getAdCampaigns.expand(accountId=adAccounts)
    adSets = getAdSets.expand(accountId=adAccounts)
    ads = getAds.expand(accountId=adAccounts)
    creatives = getCreatives.expand(accountId=adAccounts)
    # creativePerformanceReport = getCreativePerformanceReport.expand(accountId=adAccounts)
    
facebookads_extract = facebookads_extract()

r_session.close()