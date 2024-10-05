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

global r_session
r_session = requests.Session()
retry = Retry(connect=3, backoff_factor=5)
adapter = HTTPAdapter(max_retries=retry)
r_session.mount('http://', adapter)
r_session.mount('https://', adapter)

@dag(schedule_interval="@daily", catchup=False, default_args=default_args)
# @dag(schedule_interval=None, catchup=False, default_args=default_args)
def googleads_extract():
    @task(task_id="getToken", retries=0)
    def getToken():
        token = googleads_utils.extractReports(config, r_session).getToken()
        return token
    @task(task_id="getHeaders", retries=0)
    def getHeaders(accessToken):
        headers=googleads_utils.extractReports(config, r_session).getHeaders(accessToken)
        return headers
    @task(task_id="getAdAccounts", retries=0)
    def getAdAccounts(headers):
        response = googleads_utils.extractReports(config, r_session).getAdAccounts(headers)
        return response
    @task(task_id="getAdCampaigns", retries=0)
    def getAdCampaigns(accountId, headers):
        response = googleads_utils.extractReports(config, r_session).getAdCampaigns(accountId, headers)
        return response
    @task(task_id="getAdGroups", retries=0)
    def getAdGroups(accountId, headers):
        response = googleads_utils.extractReports(config, r_session).getAdGroups(accountId, headers)
        return response
    @task(task_id="getAds", retries=0)
    def getAds(accountId, headers):
        response = googleads_utils.extractReports(config, r_session).getAds(accountId, headers)
        return response
    @task(task_id="getAdLabels", retries=0)
    def getAdLabels(accountId, headers):
        response = googleads_utils.extractReports(config, r_session).getAdLabels(accountId, headers)
        return response
    @task(task_id="getKws", retries=0)
    def getKws(accountId, headers):
        response = googleads_utils.extractReports(config, r_session).getKws(accountId, headers)
        return response
    @task(task_id="getKwLabels", retries=0)
    def getKwLabels(headers, accountId):
        response = googleads_utils.extractReports(config, r_session).getKwLabels(accountId, headers)
        return response
    @task(task_id="getAdPerformanceReport", retries=0)
    def getAdPerformanceReport(accountId, headers):
        response = googleads_utils.extractReports(config, r_session).getAdPerformanceReport(accountId, headers)
        return response
    @task(task_id="getKwPerformanceReport", retries=0)
    def getKwPerformanceReport(accountId, headers):
        response = googleads_utils.extractReports(config, r_session).getKwPerformanceReport(accountId, headers)
        return response
    @task(task_id="getClickPerformanceReport", retries=0)
    def getClickPerformanceReport(accountId, headers):
        response = googleads_utils.extractReports(config, r_session).getClickPerformanceReport(accountId, headers)
        return response
    @task(task_id="getChangeEventReport", retries=0)
    def getChangeEventReport(accountId, headers):
        response = googleads_utils.extractReports(config, r_session).getClickPerformanceReport(accountId, headers)
        return response

    accessToken = getToken()
    headers = getHeaders(accessToken)
    adAccounts = getAdAccounts(headers)
    adCampaigns = getAdCampaigns.partial(headers=headers).expand(accountId=adAccounts)
    adGroups = getAdGroups.partial(headers=headers).expand(accountId=adAccounts)
    ads = getAds.partial(headers=headers).expand(accountId=adAccounts)
    adLabels = getAdLabels.partial(headers=headers).expand(accountId=adAccounts)
    kws = getKws.partial(headers=headers).expand(accountId=adAccounts)
    kwLabels = getKwLabels.partial(headers=headers).expand(accountId=adAccounts)
    adPerformanceReport = getAdPerformanceReport.partial(headers=headers).expand(accountId=adAccounts)
    kwPerformanceReport = getKwPerformanceReport.partial(headers=headers).expand(accountId=adAccounts)
    clickPerformanceReport = getClickPerformanceReport.partial(headers=headers).expand(accountId=adAccounts)
    changeEventReport = getChangeEventReport.partial(headers=headers).expand(accountId=adAccounts)
    
googleads_extract = googleads_extract()

r_session.close()