from .ddb_utils import ddbUtils
from .s3_utils import s3Utils
from datetime import date, datetime, time, timedelta
import requests
import simplejson as json
import hashlib
import sys
from pprint import pprint
import urllib
from airflow import AirflowException

class extractReports:
    def __init__(self, config, r_session):
        self.config = config
        self.r_session = r_session
        self.tableName = config['linkedin']['tableName']
        self.partKey = config['linkedin']['partKey']
        self.personId = config['linkedin']['personId']
        self.userId = config['linkedin']['userId']
        self.clientId = config['linkedin']['clientId']
        self.clientSecret = config['linkedin']['clientSecret']
        self.attWindow = config['linkedin']['attWindow']
    def hashString(self, string):
        response = hashlib.md5(string.encode('utf-8')).hexdigest()
        return response
    def getToken(self):
        response = ddbUtils(self.config).getItem(self.tableName,self.partKey,self.userId)
        payload = response['Item']['payload']
        # pprint(payload)
        refresh_token = payload[self.personId]['token']['refresh_token']
        headers = {"client_id": self.clientId, "client_secret": self.clientSecret, "Content-type": "application/json", "grant_type": "refresh_token", "refresh_token": refresh_token}
        url = "https://www.linkedin.com/oauth/v2/accessToken"
        r = self.r_session.post(url, data=headers)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'errorDetailType' in j or 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
            token = payload[self.personId]['token']['access_token']
        else:
            payload[self.personId]['token'] = j
            # pprint(payload)
            ddbUtils(self.config).putItem(self.tableName, self.partKey, self.userId, 'payload', payload)
            token = j['access_token']
        headers={'Authorization': 'Bearer {}'.format(token),  "LinkedIn-Version": "202411", "Content-type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"}
        return headers
    def getAdAccounts(self, **kwargs):
        interval = str(date.today())
        fcontents = ''
        accountIds = kwargs.get('accountIds', [])
        pageSize = 100
        nextPageToken = kwargs.get('nextPageToken',None)
        nextPageNum = kwargs.get('nextPageNum',0)
        if nextPageToken:
            url = 'https://api.linkedin.com/rest/adAccounts?pageSize={}&q=search&search=(type:(values:List(BUSINESS,ENTERPRISE)))&sortOrder=DESCENDING&pageToken={}'.format(pageSize, nextPageToken)
        else:
            url = 'https://api.linkedin.com/rest/adAccounts?pageSize={}&q=search&search=(type:(values:List(BUSINESS,ENTERPRISE)))&sortOrder=DESCENDING'.format(pageSize)
        print(url)
        fname = '{}:{}:dims-accounts:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
            # pprint(j)
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'errorDetailType' in j or 'code' in j:
            raise AirflowException(f"API response returned error message: {j}")
        elif 'elements' in j:
            elements = j['elements']
            for item in elements:
                accountIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, item['id'], url, json.dumps(item))
                # print(row)
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/accounts/{}'.format(fname))
            if 'metadata' in j:
                if 'nextPageToken' in j['metadata']:
                    nextPageToken = j['metadata']['nextPageToken']
                    nextPageNum += 1
                    self.getAdAccounts(accountId, nextPageNum=nextPageNum, campaignIds=accountIds, nextPageToken=nextPageToken)
        else:
            item = {}
            item['msg'] = 'no data'
            accountId = ''
            row = "{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, json.dumps(item))
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/accounts/{}'.format(fname))
        return accountIds
    def getAdCampaignGroups(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        campaignGroupIds = kwargs.get('campaignGroupIds', [])
        pageSize = 100
        nextPageToken = kwargs.get('nextPageToken',None)
        nextPageNum = kwargs.get('nextPageNum',0)
        if nextPageToken:
            url = 'https://api.linkedin.com/rest/adAccounts/{}/adCampaignGroups?pageSize={}&q=search&search=(status:(values:List(ACTIVE,ARCHIVED,CANCELED,DRAFT,PAUSED,PENDING_DELETION,REMOVED)))&sortOrder=DESCENDINGpageToken={}'.format(accountId, pageSize, nextPageToken)
        else:
            url = 'https://api.linkedin.com/rest/adAccounts/{}/adCampaignGroups?pageSize={}&q=search&search=(status:(values:List(ACTIVE,ARCHIVED,CANCELED,DRAFT,PAUSED,PENDING_DELETION,REMOVED)))&sortOrder=DESCENDING'.format(accountId, pageSize)
        print(url)
        fname = '{}:{}:{}:dims-campaigngroups:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'errorDetailType' in j or 'code' in j:
            raise AirflowException(f"API response returned error message: {j}")
        elif 'elements' in j:
            elements = j['elements']
            for item in elements:
                campaignGroupIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, item['id'], url, json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/campaigngroups/{}'.format(fname))
            if 'metadata' in j:
                if 'nextPageToken' in j['metadata']:
                    nextPageToken = j['metadata']['nextPageToken']
                    nextPageNum += 1
                    self.getAdCampaignGroups(accountId, nextPageNum=nextPageNum, campaignIds=campaignGroupIds, nextPageToken=nextPageToken)
        else:
            item = {}
            item['msg'] = 'no data'
            campaignGroupId = ''
            row = "{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, campaignGroupId, json.dumps(item))
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/campaigngroups/{}'.format(fname))
        return campaignGroupIds
    def getAdCampaigns(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        campaignIds = kwargs.get('campaignIds', [])
        pageSize = 100
        nextPageToken = kwargs.get('nextPageToken',None)
        nextPageNum = kwargs.get('nextPageNum',0)
        if nextPageToken:
            url = 'https://api.linkedin.com/rest/adAccounts/{}/adCampaigns?pageSize={}&q=search&search=(status:(values:List(ACTIVE,ARCHIVED,CANCELED,DRAFT,PAUSED,PENDING_DELETION,REMOVED,COMPLETED)))&sortOrder=DESCENDING&pageToken={}'.format(accountId, pageSize, nextPageToken)
        else:
            url = 'https://api.linkedin.com/rest/adAccounts/{}/adCampaigns?pageSize={}&q=search&search=(status:(values:List(ACTIVE,ARCHIVED,CANCELED,DRAFT,PAUSED,PENDING_DELETION,REMOVED,COMPLETED)))&sortOrder=DESCENDING'.format(accountId, pageSize)
        print(url)
        fname = '{}:{}:{}:dims-campaigns:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
            # print(j)
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'errorDetailType' in j or 'code' in j:
            raise AirflowException(f"API response returned error message: {j}")
        elif 'elements' in j:
            elements = j['elements']
            for item in elements:
                campaignIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, item['id'], url, json.dumps(item))
                # print(row)
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/campaigns/{}'.format(fname))
            if 'metadata' in j:
                if 'nextPageToken' in j['metadata']:
                    nextPageToken = j['metadata']['nextPageToken']
                    nextPageNum += 1
                    self.getAdCampaigns(accountId, nextPageNum=nextPageNum, campaignIds=campaignIds, nextPageToken=nextPageToken)
        else:
            item = {}
            item['msg'] = 'no data'
            campaignId = ''
            row = "{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, campaignId, json.dumps(item))
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/campaigs/{}'.format(fname))
        return campaignIds
    def getUgcPost(self, shareUrn):
        url = 'https://api.linkedin.com/rest/posts/{}'.format(urllib.parse.quote(shareUrn))
        print(url)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'errorDetailType' in j:
            raise AirflowException(f"API response returned error message: {j}")
        return(j)
    def getAdInmailContent(self, adInMailContentId):
        url = 'https://api.linkedin.com/rest/adInMailContents/{}'.format(urllib.parse.quote(adInMailContentId.split(':')[-1]))
        print(url)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'errorDetailType' in j:
            raise AirflowException(f"API response returned error message: {j}")
        return(j)
    
    def getAdCreatives(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        creativeIds = kwargs.get('creativeIds',[])
        pageSize = 100
        nextPageToken = kwargs.get('nextPageToken',None)
        nextPageNum = kwargs.get('nextPageNum',0)
        if nextPageToken:
            url = 'https://api.linkedin.com/rest/adAccounts/{}/creatives?pageSize={}&campaigns=List()&q=criteria&sortOrder=DESCENDING&pageToken={}'.format(accountId, pageSize, nextPageToken)
        else:
            url = 'https://api.linkedin.com/rest/adAccounts/{}/creatives?pageSize={}&campaigns=List()&q=criteria&sortOrder=DESCENDING'.format(accountId, pageSize)
        print(url)
        fname = '{}:{}:{}:dims-creatives:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
            # pprint(j)
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'errorDetailType' in j or 'code' in j:
            raise AirflowException(f"API response returned error message: {j}")
        elif 'elements' in j:
            elements = j['elements']
            for item in elements:
                creativeIds.append(item['id'].split(':')[-1])
                creativeId = item['id']
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/creatives/{}'.format(fname))
            if 'metadata' in j:
                if 'nextPageToken' in j['metadata']:
                    nextPageToken = j['metadata']['nextPageToken']
                    nextPageNum += 1
                    self.getAdCreatives(accountId, nextPageNum=nextPageNum, creativeIds=creativeIds, nextPageToken=nextPageToken)
        else:
            item = {}
            item['msg'] = 'no data'
            creativeId = ''
            row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, creativeId, json.dumps(item))
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/creatives/{}'.format(fname))
        return creativeIds

    def getCreativePerformanceReport(self, accountId):
        def doGetCreativePerformanceReport(accountId, reportStart, reportEnd, startDate, endDate, **kwargs):
            fcontents = kwargs.get('fcontents','')
            url = 'https://api.linkedin.com/rest/adAnalytics?q=analytics&{}&{}&timeGranularity=DAILY&accounts=urn%3Ali%3AsponsoredAccount%3A{}&pivot=CREATIVE&fields=dateRange,impressions,clicks,landingPageClicks,companyPageClicks,totalEngagements,costInUsd,externalWebsiteConversions,externalWebsitePostClickConversions,externalWebsitePostViewConversions,oneClickLeads,pivot,pivotValue'.format(reportStart, reportEnd, accountId)
            print(url)
            fname = '{}:{}:{}:facts-creatives:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, startDate, endDate)
            r = self.r_session.get(url)
            try:
                j = r.json()
                # print(j)
            except Exception as e:
                raise AirflowException(f"Error occurred processing API response: {e}")
            if 'errorDetailType' in j:
                raise AirflowException(f"API response returned error message: {j}")
            elif 'elements' in j:
                elements = j['elements']
                for item in elements:
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, json.dumps(item))
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents, 'facts/creatives/{}'.format(fname))
            else:
                item = {}
                item['msg'] = 'no data'
                pivotId = ''
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, json.dumps(item))
                if fcontents == '':
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents, 'facts/creatives/{}'.format(fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = self.attWindow)
        delta = endDate - startDate
        for i in range(delta.days):
            interval = startDate + timedelta(days=i)
            reportStart = 'dateRange.start.year={}&dateRange.start.month={}&dateRange.start.day={}'.format(interval.year,interval.month,interval.day)
            reportEnd = 'dateRange.end.year={}&dateRange.end.month={}&dateRange.end.day={}'.format(interval.year,interval.month,interval.day)
            doGetCreativePerformanceReport(accountId, reportStart, reportEnd, interval, interval)