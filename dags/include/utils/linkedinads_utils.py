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
        headers = {"client_id": self.clientId, "client_secret": self.clientSecret, "Content-type": "application/w-www-form-urlencoded", "grant_type": "refresh_token", "refresh_token": refresh_token}
        url = "https://www.linkedin.com/oauth/v2/accessToken"
        r = self.r_session.post(url, data=headers)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
            # token = payload[self.personId]['token']['access_token']
        payload[self.personId]['token'] = j
        # pprint(payload)
        ddbUtils(self.config).putItem(self.tableName, self.partKey, self.userId, 'payload', payload)
        token = j['access_token']
        headers={'Authorization': 'Bearer {}'.format(token)}
        return headers
    def getAdAccounts(self, **kwargs):
        interval = str(date.today())
        start = kwargs.get('start','start=0')
        fcontents = ''
        accountIds = kwargs.get('accountIds', [])
        pageSize = 99
        nextPageNum = kwargs.get('nextPageNum',0)
        url = 'https://api.linkedin.com/v2/adAccountsV2?count={}&q=search&search.type.values[0]=BUSINESS&search.type.values[1]=ENTERPRISE&sort.field=ID&sort.order=DESCENDING&{}'.format(pageSize, start)
        print(url)
        fname = '{}:{}:dims-accounts:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
        elif 'elements' in j:
            elements = j['elements']
            for item in elements:
                accountIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, item['id'], url, json.dumps(item))
                # print(row)
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/accounts/{}'.format(fname))
            if 'paging' in j:
                if 'links' in j['paging']:
                    links = j['paging']['links']
                    if any(link['rel'] == 'next' for link in links):
                        for link in links:
                            if link['rel'] == 'next':
                                start = link['href'].split('&')[-1]
                                nextPageNum += 1
                                self.getAdAccounts(start=start, nextPageNum=nextPageNum, accountIds=accountIds)
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
        start = kwargs.get('start','start=0')
        fcontents = ''
        campaignGroupIds = kwargs.get('campaignGroupIds', [])
        pageSize = 99
        nextPageNum = kwargs.get('nextPageNum',0)
        url = 'https://api.linkedin.com/v2/adCampaignGroupsV2?count={}&q=search&search.account.values[0]=urn:li:sponsoredAccount:{}&sort.field=ID&sort.order=DESCENDING&{}'.format(pageSize, accountId, start)
        print(url)
        fname = '{}:{}:{}:dims-campaigngroups:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
        elif 'elements' in j:
            elements = j['elements']
            for item in elements:
                campaignGroupIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, item['id'], url, json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/campaigngroups/{}'.format(fname))
            if 'paging' in j:
                if 'links' in j['paging']:
                    links = j['paging']['links']
                    if any(link['rel'] == 'next' for link in links):
                        for link in links:
                            if link['rel'] == 'next':
                                start = link['href'].split('&')[-1]
                                nextPageNum += 1
                                self.getAdCampaignGroups(accountId, start=start, nextpageNum=nextPageNum, campaignGroupIds=campaignGroupIds)
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
        start = kwargs.get('start','start=0')
        fcontents = ''
        campaignIds = kwargs.get('campaignIds', [])
        pageSize = 99
        nextPageNum = kwargs.get('nextPageNum',0)
        url = 'https://api.linkedin.com/v2/adCampaignsV2?count={}&q=search&search.account.values[0]=urn:li:sponsoredAccount:{}&sort.field=ID&sort.order=DESCENDING&{}'.format(pageSize, accountId, start)
        print(url)
        fname = '{}:{}:{}:dims-campaigns:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
        elif 'elements' in j:
            elements = j['elements']
            for item in elements:
                campaignIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, item['id'], url, json.dumps(item))
                # print(row)
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/campaigns/{}'.format(fname))
            if 'paging' in j:
                if 'links' in j['paging']:
                    links = j['paging']['links']
                    if any(link['rel'] == 'next' for link in links):
                        for link in links:
                            if link['rel'] == 'next':
                                start = link['href'].split('&')[-1]
                                nextPageNum += 1
                                self.getAdCampaigns(accountId, start=start, nextPageNum=nextPageNum, campaignIds=campaignIds)
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
        url = 'https://api.linkedin.com/v2/ugcPosts/{}'.format(urllib.parse.quote(shareUrn))
        print(url)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
        return(j)
    def getAdInmailContent(self, adInMailContentId):
        url = 'https://api.linkedin.com/v2/adInMailContentsV2/{}'.format(urllib.parse.quote(adInMailContentId.split(':')[-1]))
        print(url)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
        return(j)
    def getAdCreatives(self, accountId, **kwargs):
        interval = str(date.today())
        start = kwargs.get('start','start=0')
        fcontents = ''
        creativeIds = kwargs.get('creativeIds',[])
        pageSize = 99
        nextPageNum = kwargs.get('nextPageNum',0)
        url = 'https://api.linkedin.com/v2/adCreativesV2?&count={}&q=search&search.account.values[0]=urn:li:sponsoredAccount:{}&sort.field=ID&sort.order=DESCENDING&{}'.format(pageSize, accountId, start)
        print(url)
        fname = '{}:{}:{}:dims-creatives:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
        elif 'elements' in j:
            elements = j['elements']
            for item in elements:
                creativeIds.append(item['id'])
                creativeType = item['type']
                # print(creativeType)
                creativeId = item['id']
                if creativeType == 'TEXT_AD':
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                elif creativeType == 'SPOTLIGHT_V2':
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                elif creativeType == 'SPONSORED_STATUS_UPDATE':
                    urn = item['variables']['data']['com.linkedin.ads.SponsoredUpdateCreativeVariables']['activity']
                    post = self.getUgcPost(urn)
                    item['post'] = post
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                elif creativeType == 'SPONSORED_VIDEO':
                    urn = item['variables']['data']['com.linkedin.ads.SponsoredVideoCreativeVariables']['userGeneratedContentPost']
                    post = self.getUgcPost(urn)
                    item['post'] = post
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                elif creativeType == 'SPONSORED_INMAILS':
                    adInMailContentId = item['variables']['data']['com.linkedin.ads.SponsoredInMailCreativeVariables']['content']
                    message = self.getAdInmailContent(adInMailContentId)
                    item['message'] = message
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                elif creativeType == 'SPONSORED_MESSAGE':
                    adInMailContentId = item['variables']['data']['com.linkedin.ads.SponsoredInMailCreativeVariables']['content']
                    message = self.getAdInmailContent(adInMailContentId)
                    item['message'] = message
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                elif creativeType == 'SPONSORED_UPDATE_CAROUSEL':
                    urn = item['variables']['data']['com.linkedin.ads.SponsoredUpdateCarouselCreativeVariables']['activity']
                    post = self.getUgcPost(urn)
                    item['post'] = post
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                elif creativeType == 'FOLLOW_COMPANY_V2':
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                elif creativeType == 'JOBS_V2':
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                else:
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, creativeId, url, json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/creatives/{}'.format(fname))
            if 'paging' in j:
                if 'links' in j['paging']:
                    links = j['paging']['links']
                    if any(link['rel'] == 'next' for link in links):
                        for link in links:
                            if link['rel'] == 'next':
                                start = link['href'].split('&')[-1]
                                nextPageNum += 1
                                self.getAdCreatives(accountId, start=start, nextPageNum=nextPageNum, creativeIds=creativeIds)
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
            start = kwargs.get('start','start=0')
            fcontents = kwargs.get('fcontents','')
            pageSize = 999999
            nextPageNum = kwargs.get('nextPageNum',0)
            url = 'https://api.linkedin.com/v2/adAnalyticsV2?count={}&{}&q=analytics&{}&{}&timeGranularity=DAILY&accounts=urn%3Ali%3AsponsoredAccount%3A{}&pivot=CREATIVE&fields=dateRange,impressions,clicks,landingPageClicks,companyPageClicks,totalEngagements,costInUsd,externalWebsiteConversions,externalWebsitePostClickConversions,externalWebsitePostViewConversions,oneClickLeads,pivot,pivotValue'.format(pageSize, start, reportStart, reportEnd, accountId)
            print(url)
            fname = '{}:{}:{}:facts-creatives:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, startDate, endDate, pageSize, nextPageNum)
            r = self.r_session.get(url)
            try:
                j = r.json()
            except Exception as e:
                raise AirflowException(f"Error occurred processing API response {e}")
            if 'error' in j:
                raise AirflowException(f"API response returned error message: {j}")
            elif 'elements' in j:
                elements = j['elements']
                for item in elements:
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, json.dumps(item))
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents, 'facts/creatives/{}'.format(fname))
                if 'paging' in j:
                    if 'links' in j['paging']:
                        links = j['paging']['links']
                        if any(link['rel'] == 'next' for link in links):
                            for link in links:
                                if link['rel'] == 'next':
                                    start = link['href'].split('&')[-1]
                                    nextPageNum += 1
                                    doGetCreativePerformanceReport(accountId, reportStart, reportEnd, startDate, endDate, start=start, nextPageNum=nextPageNum)
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