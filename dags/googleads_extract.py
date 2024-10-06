import logging
from datetime import date, datetime, time, timedelta
from airflow.decorators import dag, task
import airflow
from include.utils import googleads_utils
import os, sys
import yaml
import pendulum
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config/', 'googleads.yaml')
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
headers = googleads_utils.extractReports(config, r_session).getToken()
r_session.headers.update(headers)
extractReports = googleads_utils.extractReports(config, r_session)

@dag(schedule_interval="@hourly", catchup=False, default_args=default_args)
# @dag(schedule_interval=None, catchup=False, default_args=default_args)
def googleads_extract():
    @task(task_id="getAdAccounts", retries=0)
    def getAdAccounts():
        response = extractReports.getAdAccounts()
        return response
    @task(task_id="getAdCampaigns", retries=0)
    def getAdCampaigns(accountId):
        response = extractReports.getAdCampaigns(accountId)
        return response
    @task(task_id="getAdGroups", retries=0)
    def getAdGroups(accountId):
        response = extractReports.getAdGroups(accountId)
        return response
    @task(task_id="getAds", retries=0)
    def getAds(accountId):
        response = extractReports.getAds(accountId)
        return response
    @task(task_id="getAdLabels", retries=0)
    def getAdLabels(accountId):
        response = extractReports.getAdLabels(accountId)
        return response
    @task(task_id="getKws", retries=0)
    def getKws(accountId):
        response = extractReports.getKws(accountId)
        return response
    @task(task_id="getKwLabels", retries=0)
    def getKwLabels(accountId):
        response = extractReports.getKwLabels(accountId)
        return response
    @task(task_id="getAdPerformanceReport", retries=0)
    def getAdPerformanceReport(accountId):
        response = extractReports.getAdPerformanceReport(accountId)
        return response
    @task(task_id="getKwPerformanceReport", retries=0)
    def getKwPerformanceReport(accountId):
        response = extractReports.getKwPerformanceReport(accountId)
        return response
    @task(task_id="getClickPerformanceReport", retries=0)
    def getClickPerformanceReport(accountId):
        response = extractReports.getClickPerformanceReport(accountId)
        return response
    @task(task_id="getChangeEventReport", retries=0)
    def getChangeEventReport(accountId):
        response = extractReports.getChangeEventReport(accountId)
        return response

    adAccounts = getAdAccounts()
    adCampaigns = getAdCampaigns.expand(accountId=adAccounts)
    adGroups = getAdGroups.expand(accountId=adAccounts)
    ads = getAds.expand(accountId=adAccounts)
    adLabels = getAdLabels.expand(accountId=adAccounts)
    kws = getKws.expand(accountId=adAccounts)
    kwLabels = getKwLabels.expand(accountId=adAccounts)
    adPerformanceReport = getAdPerformanceReport.expand(accountId=adAccounts)
    kwPerformanceReport = getKwPerformanceReport.expand(accountId=adAccounts)
    clickPerformanceReport = getClickPerformanceReport.expand(accountId=adAccounts)
    changeEventReport = getChangeEventReport.expand(accountId=adAccounts)
    
googleads_extract = googleads_extract()

r_session.close()