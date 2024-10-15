from .ddb_utils import ddbUtils
from .s3_utils import s3Utils
from datetime import date, datetime, time, timedelta
import requests
import simplejson as json
import hashlib
import sys
from pprint import pprint
import urllib

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
        j = r.json()
        # pprint(j)
        payload[self.personId]['token'] = j
        # pprint(payload)
        ddbUtils(self.config).putItem(self.tableName, self.partKey, self.userId, 'payload', payload)
        token = j['access_token']
        headers={'Authorization': 'Bearer {}'.format(token)}
        return headers
    def getAdAccounts(self, **kwargs):
        start = kwargs.get('start','start=0')
        fcontents = kwargs.get('fcontents','')
        accountIds = kwargs.get('accountIds', [])
        url = 'https://api.linkedin.com/v2/adAccountsV2?count=99&q=search&search.type.values[0]=BUSINESS&search.type.values[1]=ENTERPRISE&sort.field=ID&sort.order=DESCENDING&{}'.format(start)
        print(url)
        r = self.r_session.get(url)
        j = r.json()
        fname = '{}:{}:dims-accounts'.format(self.hashString(self.userId), self.personId)
        if 'elements' in j:
            elements = j['elements']
            for item in elements:
                accountIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, item['id'], url, json.dumps(item))
                # print(row)
                fcontents += row
            if 'paging' in j:
                if 'links' in j['paging']:
                    links = j['paging']['links']
                    if any(link['rel'] == 'next' for link in links):
                        for link in links:
                            if link['rel'] == 'next':
                                start = link['href'].split('&')[-1]
                                self.getAdAccounts(start=start, fcontents=fcontents, accountIds=accountIds)
                    else:
                        s3Utils(self.config).writeToS3(fcontents,'dims/accounts/{}'.format(fname))
                else:
                    s3Utils(self.config).writeToS3(fcontents,'dims/accounts/{}'.format(fname))
            else:
                s3Utils(self.config).writeToS3(fcontents,'dims/accounts/{}'.format(fname))
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
        start = kwargs.get('start','start=0')
        fcontents = kwargs.get('fcontents','')
        campaignGroupIds = kwargs.get('campaignGroupIds', [])
        url = 'https://api.linkedin.com/v2/adCampaignGroupsV2?count=99&q=search&search.account.values[0]=urn:li:sponsoredAccount:{}&sort.field=ID&sort.order=DESCENDING&{}'.format(accountId, start)
        fname = '{}:{}:{}:dims-campaigngroups'.format(self.hashString(self.userId), self.personId, accountId)
        r = self.r_session.get(url)
        j = r.json()
        fname = '{}:{}:{}:dims-campaigngroups'.format(self.hashString(self.userId), self.personId, accountId)
        if 'elements' in j:
            elements = j['elements']
            for item in elements:
                campaignGroupIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, item['id'], url, json.dumps(item))
                fcontents += row
            if 'paging' in j:
                if 'links' in j['paging']:
                    links = j['paging']['links']
                    if any(link['rel'] == 'next' for link in links):
                        for link in links:
                            if link['rel'] == 'next':
                                start = link['href'].split('&')[-1]
                                self.getAdCampaignGroups(accountId, start=start, fcontents=fcontents, campaignGroupIds=campaignGroupIds)
                    else:
                        s3Utils(self.config).writeToS3(fcontents,'dims/campaigngroups/{}'.format(fname))
                else:
                    s3Utils(self.config).writeToS3(fcontents,'dims/campaigngroups/{}'.format(fname))
            else:
                s3Utils(self.config).writeToS3(fcontents,'dims/campaigngroups/{}'.format(fname))
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
        start = kwargs.get('start','start=0')
        fcontents = kwargs.get('fcontents','')
        campaignIds = kwargs.get('campaignIds', [])
        url = 'https://api.linkedin.com/v2/adCampaignsV2?count=99&q=search&search.account.values[0]=urn:li:sponsoredAccount:{}&sort.field=ID&sort.order=DESCENDING&{}'.format(accountId, start)
        print(url)
        r = self.r_session.get(url)
        j = r.json()
        fname = '{}:{}:{}:dims-campaigns'.format(self.hashString(self.userId), self.personId, accountId)
        if 'elements' in j:
            elements = j['elements']
            for item in elements:
                campaignIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, item['id'], url, json.dumps(item))
                # print(row)
                fcontents += row
            if 'paging' in j:
                if 'links' in j['paging']:
                    links = j['paging']['links']
                    if any(link['rel'] == 'next' for link in links):
                        for link in links:
                            if link['rel'] == 'next':
                                start = link['href'].split('&')[-1]
                                self.getAdCampaigns(accountId, start=start, fcontents=fcontents, campaignIds=campaignIds)
                    else:
                        s3Utils(self.config).writeToS3(fcontents,'dims/campaigns/{}'.format(fname))
                else:
                    s3Utils(self.config).writeToS3(fcontents,'dims/campaigns/{}'.format(fname))
            else:
                s3Utils(self.config).writeToS3(fcontents,'dims/campaigns/{}'.format(fname))
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
        r = self.r_session.get(url)
        j = r.json()
        return(j)
    def getAdInmailContent(self, adInMailContentId):
        url = 'https://api.linkedin.com/v2/adInMailContentsV2/{}'.format(urllib.parse.quote(adInMailContentId.split(':')[-1]))
        r = self.r_session.get(url)
        j = r.json()
        return(j)
    def getAdCreatives(self, accountId, **kwargs):
        start = kwargs.get('start','start=0')
        fcontents = kwargs.get('fcontents','')
        creativeIds = kwargs.get('creativeIds',[])
        url = 'https://api.linkedin.com/v2/adCreativesV2?&count=99&q=search&search.account.values[0]=urn:li:sponsoredAccount:{}&sort.field=ID&sort.order=DESCENDING&{}'.format(accountId, start)
        print(url)
        fname = '{}:{}:{}:match-tables-creatives'.format(self.hashString(self.userId), self.personId, accountId)
        r = self.r_session.get(url)
        j = r.json()
        if 'elements' in j:
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
            if 'paging' in j:
                if 'links' in j['paging']:
                    links = j['paging']['links']
                    if any(link['rel'] == 'next' for link in links):
                        for link in links:
                            if link['rel'] == 'next':
                                start = link['href'].split('&')[-1]
                                self.getAdCreatives(accountId, start=start, fcontents=fcontents, creativeIds=creativeIds)
                    else:
                        s3Utils(self.config).writeToS3(fcontents,'dims/campaigns/{}'.format(fname))
                else:
                    s3Utils(self.config).writeToS3(fcontents,'dims/creatives/{}'.format(fname))
            else:
                s3Utils(self.config).writeToS3(fcontents,'dims/creatives/{}'.format(fname))
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
            url = 'https://api.linkedin.com/v2/adAnalyticsV2?count=999999&{}&q=analytics&{}&{}&timeGranularity=DAILY&accounts=urn%3Ali%3AsponsoredAccount%3A{}&pivot=CREATIVE&fields=dateRange,impressions,clicks,landingPageClicks,companyPageClicks,totalEngagements,costInUsd,externalWebsiteConversions,externalWebsitePostClickConversions,externalWebsitePostViewConversions,oneClickLeads,pivot,pivotValue'.format(start, reportStart, reportEnd, accountId)
            # print(url)
            r = self.r_session.get(url)
            j = r.json()
            fname = '{}:{}:{}:facts-creatives:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, startDate, endDate)
            if 'elements' in j:
                elements = j['elements']
                for item in elements:
                    row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, json.dumps(item))
                    fcontents += row
                if 'paging' in j:
                    if 'links' in j['paging']:
                        links = j['paging']['links']
                        if any(link['rel'] == 'next' for link in links):
                            for link in links:
                                if link['rel'] == 'next':
                                    start = link['href'].split('&')[-1]
                                    doGetCreativePerformanceReport(accountId, reportStart, reportEnd, startDate, endDate, start=start, fcontents=fcontents)
                        else:
                            s3Utils(self.config).writeToS3(fcontents, 'facts/creatives/{}'.format(fname))
                    else:
                        s3Utils(self.config).writeToS3(fcontents, 'facts/creatives/{}'.format(fname))
                else:
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