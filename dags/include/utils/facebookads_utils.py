from .ddb_utils import ddbUtils
from .s3_utils import s3Utils
from datetime import date, datetime, time, timedelta
import requests
import simplejson as json
import hashlib
import sys
import urllib
from pprint import pprint
from airflow import AirflowException

class extractReports:
    def __init__(self, config, r_session):
        self.config = config
        self.r_session = r_session
        self.tableName = config['facebook']['tableName']
        self.partKey = config['facebook']['partKey']
        self.personId = config['facebook']['personId']
        self.userId = config['facebook']['userId']
        self.clientId = config['facebook']['clientId']
        self.clientSecret = config['facebook']['clientSecret']
        self.attWindow = config['facebook']['attWindow']
    def hashString(self, string):
        response = hashlib.md5(string.encode('utf-8')).hexdigest()
        return response
    def getToken(self):
        response = ddbUtils(self.config).getItem(self.tableName,self.partKey,self.userId)
        payload = response['Item']['payload']
        refresh_token = payload[self.personId]['token']['access_token']
        url = 'https://graph.facebook.com/oauth/access_token?grant_type=fb_exchange_token&client_id={}&client_secret={}&fb_exchange_token={}'.format(self.clientId, self.clientSecret, refresh_token)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
        payload[self.personId]['token'] = j
        ddbUtils(self.config).putItem(self.tableName, self.partKey, self.userId, 'payload', payload)
        token = j['access_token']
        headers={'Authorization': 'Bearer {}'.format(token)}
        return headers
    def getAdAccounts(self, **kwargs):
        interval = str(date.today())
        fcontents = ''
        after = kwargs.get('after','after=')
        pageSize = 199
        nextPageNum = kwargs.get('nextPageNum',0)
        accountIds = kwargs.get('accountIds',[])
        fields = [
          'id',
          'account_id',
          'owner',
          'partner',
          'name',
          'end_advertiser',
          'end_advertiser_name',
          'business',
          'business_name',
          'currency',
          'funding_source',
          'funding_source_details',
          'timezone_id',
          'timezone_name',
          'timezone_offset_hours_utc',
          'is_personal',
          'capabilities'
        ]
        fields = ','.join(fields)
        url = 'https://graph.facebook.com/v21.0/{}/adaccounts?limit={}&fields={}&{}'.format(self.personId, pageSize, fields, after)
        fname = '{}:{}:dims-accounts:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, interval, interval, pageSize, nextPageNum)
        print(url)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j['error']}")
        elif 'data' in j:
            items = j['data']
            for item in items:
                accountIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, url, item['id'], json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/accounts/{}'.format(fname))
            if 'paging' in j:
                if 'cursors' in j['paging']:
                    if 'after' in j['paging']['cursors']:
                        after = 'after={}'.format(j['paging']['cursors']['after'])
                        nextPageNum += 1
                        self.getAdAccounts(nextPageNum=nextPageNum, after=after, accountIds=accountIds)
        else:
            item = {}
            item['msg'] = 'no data'
            accountId = ''
            row = "{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, url, accountId, json.dumps(item))  
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/accounts/{}'.format(fname))
        return accountIds
    def getAdCampaigns(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageNum = kwargs.get('nextPageNum',0)
        after = kwargs.get('after','after=')
        pageSize = 199
        campaignIds = kwargs.get('campaignIds',[])
        fields = [
          'adlabels',
          'bid_strategy',
          'buying_type',
          'daily_budget',
          'execution_options',
          'effective_status',
          'iterative_split_test_configs',
          'lifetime_budget',
          'promoted_object',
          'source_campaign_id',
          'special_ad_categories',
          'spend_cap',
          'status',
          'topline_id',
          'upstream_events',
          'name',
          'objective',
        ]
        fields = ','.join(fields)
        params = {
          'effective_status': ['ACTIVE','PAUSED','ARCHIVED','IN_PROCESS','WITH_ISSUES'],
        }
        parameters = []
        keys = params.keys()
        for key in keys:
            val = params[key]
            parameters.append('{}={}'.format(key, val))
        parameters = '&'.join(parameters)
        url = 'https://graph.facebook.com/v21.0/{}/campaigns?limit={}&{}&fields={}&{}'.format(accountId, pageSize, parameters, fields, after)
        print(url)
        fname = '{}:{}:{}:dims-campaigns:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j['error']}")
        elif 'data' in j:
            items = j['data']
            for item in items:
                campaignIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, item['id'], json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/campaigns/{}'.format(fname))
            if 'paging' in j:
                if 'cursors' in j['paging']:
                    if 'after' in j['paging']['cursors']:
                        after = 'after={}'.format(j['paging']['cursors']['after'])
                        nextPageNum += 1
                        self.getAdCampaigns(accountId, nextPageNum=nextPageNum, after=after, campaignIds=campaignIds)
        else:
            item = {}
            item['msg'] = 'no data'
            campaignId = ''
            row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, campaignId, json.dumps(item))
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/campaigns/{}'.format(fname))
        return campaignIds
    def getAdSets(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageNum = kwargs.get('nextPageNum',0)
        after = kwargs.get('after','after=')
        pageSize = 199
        adsetIds = kwargs.get('adsetIds',[])
        fields = [
          'name',
          'effective_status',
          'adlabels',
          'adset_schedule',
          'attribution_spec',
          'bid_amount',
          'bid_strategy',
          'billing_event',
          'campaign_id',
          'campaign_spec',
          'creative_sequence',
          'daily_budget',
          # 'daily_imps',
          'daily_min_spend_target',
          'daily_spend_cap',
          'destination_type',
          'end_time',
          'execution_options',
          'frequency_control_specs',
          'is_dynamic_creative',
          'lifetime_budget',
          'lifetime_imps',
          'lifetime_min_spend_target',
          'lifetime_spend_cap',
          'multi_optimization_goal_weight',
          'optimization_goal',
          'optimization_sub_event',
          'pacing_type',
          'promoted_object',
          'rf_prediction_id',
          'source_adset_id',
          'start_time',
          'status',
          'targeting',
          'time_based_ad_rotation_id_blocks',
          'time_based_ad_rotation_intervals',
          'time_start',
          'time_stop',
          'tune_for_category',
          'objective',
        ]
        fields = ','.join(fields)
        params = {
          'effective_status': ['ACTIVE','PAUSED','ARCHIVED','IN_PROCESS','WITH_ISSUES'],
        }
        parameters = []
        keys = params.keys()
        for key in keys:
            val = params[key]
            parameters.append('{}={}'.format(key, val))
        parameters = '&'.join(parameters)
        url = 'https://graph.facebook.com/v21.0/{}/adsets?limit={}&{}&fields={}&{}'.format(accountId, pageSize, parameters, fields, after)
        print(url)
        fname = '{}:{}:{}:dims-adsets:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j['error']}")
        elif 'data' in j:
            items = j['data']
            for item in items:
                adsetIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, item['id'], json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/adsets/{}'.format(fname))
            if 'paging' in j:
                if 'cursors' in j['paging']:
                    if 'after' in j['paging']['cursors']:
                        after = 'after={}'.format(j['paging']['cursors']['after'])
                        nextPageNum += 1
                        self.getAdSets(accountId, nextPageNum=nextPageNum, after=after, adsetIds=adsetIds)
        else:
            item = {}
            item['msg'] = 'no data'
            adsetId = ''
            row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, adsetId, json.dumps(item))
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/adsets/{}'.format(fname))
        return adsetIds
    def getAds(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageNum = kwargs.get('nextPageNum',0)
        after = kwargs.get('after','after=')
        pageSize = 199
        adIds = kwargs.get('adIds',[])
        fields = [
          'name',
          'id',
          'status',
          'campaign',
          'campaign_id',
          'creative',
          'effective_status',
          'preview_shareable_link',
          'adset',
          'adset_id',
          'bid_amount',
          'targeting',
          'created',
        ]
        fields = ','.join(fields)
        params = {
          'effective_status': ['ACTIVE','PAUSED','ARCHIVED','IN_PROCESS','WITH_ISSUES'],
        }
        parameters = []
        keys = params.keys()
        for key in keys:
            val = params[key]
            parameters.append('{}={}'.format(key, val))
        parameters = '&'.join(parameters)
        url = 'https://graph.facebook.com/v21.0/{}/ads?limit={}&{}&fields={}&{}'.format(accountId, pageSize, parameters, fields, after)
        print(url)
        fname = '{}:{}:{}:dims-ads:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j['error']}")
        elif 'data' in j:
            items = j['data']
            for item in items:
                adIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, item['id'], json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/ads/{}'.format(fname))
            if 'paging' in j:
                if 'cursors' in j['paging']:
                    if 'after' in j['paging']['cursors']:
                        after = 'after={}'.format(j['paging']['cursors']['after'])
                        nextPageNum += 1
                        self.getAds(accountId, nextPageNum=nextPageNum, after=after, adsetIds=adIds)
        else:
            item = {}
            item['msg'] = 'no data'
            adId = ''
            row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, adId, json.dumps(item))
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/ads/{}'.format(fname))
        return adIds
    def getCreatives(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageNum = kwargs.get('nextPageNum',0)
        after = kwargs.get('after','after=')
        pageSize = 199
        creativeIds = kwargs.get('creativeIds',[])
        fields = [
          'id',
          'account_id',
          'actor_id',
          'adlabels',
          'applink_treatment',
          'asset_feed_spec',
          'authorization_category',
          'body',
          'branded_content_sponsor_page_id',
          'bundle_folder_id',
          'call_to_action_type',
          'categorization_criteria',
          'category_media_source',
          'destination_set_id',
          'dynamic_ad_voice',
          'effective_authorization_category',
          'effective_instagram_media_id',
          'effective_instagram_story_id',
          'effective_object_story_id',
          'enable_direct_install',
          # 'enable_launch_instant_app',
          'image_crops',
          'image_hash',
          'image_url',
          'instagram_actor_id',
          'instagram_permalink_url',
          'instagram_story_id',
          'interactive_components_spec',
          'link_destination_display_url',
          'link_og_id',
          'link_url',
          'messenger_sponsored_message',
          'name',
          'object_id',
          'object_store_url',
          'object_story_id',
          'object_story_spec',
          'object_type',
          'object_url',
          'place_page_set_id',
          'platform_customizations',
          'playable_asset_id',
          'portrait_customizations',
          'product_set_id',
          'recommender_settings',
          'status',
          'template_url',
          'template_url_spec',
          'thumbnail_url',
          'title',
          'url_tags',
          'use_page_actor_override',
          'video_id',
        ]
        fields = ','.join(fields)
        params = {
          'effective_status': ['ACTIVE','PAUSED','ARCHIVED','IN_PROCESS','WITH_ISSUES'],
        }
        parameters = []
        keys = params.keys()
        for key in keys:
            val = params[key]
            parameters.append('{}={}'.format(key, val))
        parameters = '&'.join(parameters)
        url = 'https://graph.facebook.com/v21.0/{}/adcreatives?limit={}&{}&fields={}&{}'.format(accountId, pageSize, parameters, fields, after)
        print(url)
        fname = '{}:{}:{}:dims-creatives:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j['error']}")
        elif 'data' in j:
            items = j['data']
            for item in items:
                creativeIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, item['id'], json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/creatives/{}'.format(fname))
            if 'paging' in j:
                if 'cursors' in j['paging']:
                    if 'after' in j['paging']['cursors']:
                        after = 'after={}'.format(j['paging']['cursors']['after'])
                        nextPageNum += 1
                        self.getCreatives(accountId, nextPageNum=nextPageNum, after=after, creativeIds=creativeIds)
        else: 
            item = {}
            item['msg'] = 'no data'
            creativeId = ''
            row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, creativeId, json.dumps(item))
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/creatives/{}'.format(fname))
            
        return creativeIds
    def getCustomConversions(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageNum = kwargs.get('nextPageNum',0)
        after = kwargs.get('after','after=')
        pageSize = 199
        conversionIds = kwargs.get('conversionIds',[])
        fields = [
          'id',
          'account_id',
          'name',
          'aggration_rule',
          'business',
          'creation_time',
          'custom_event_type',
          'data_sources',
          'default_conversion_value',
          'description',
          'event_source_type',
          'first_fired_time',
          'is_archived',
          'is_unavailable',
          'last_fired_time',
          'offline_conversion_data_set',
          'pixel',
          'retention_days',
          'rule'
        ]
        fields = ','.join(fields)
        url = 'https://graph.facebook.com/v21.0/{}/customconversions?limit={}&fields={}&{}'.format(accountId, pageSize, fields, after)
        print(url)
        fname = '{}:{}:{}:dims-conversions:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, interval, interval, pageSize, nextPageNum)
        r = self.r_session.get(url)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response: {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j['error']}")
        elif 'data' in j:
            items = j['data']
            for item in items:
                conversionIds.append(item['id'])
                row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, item['id'], json.dumps(item))
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/conversions/{}'.format(fname))
            if 'paging' in j:
                if 'cursors' in j['paging']:
                    if 'after' in j['paging']['cursors']:
                        after = 'after={}'.format(j['paging']['cursors']['after'])
                        nextPageNum += 1
                        self.getCustomConversions(accountId, nextPageNum=nextPageNum, after=after, conversionIds=conversionIds)
        else:
            item = {}
            item['msg'] = 'no data'
            conversionId = ''
            row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, conversionId, json.dumps(item))
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/conversions/{}'.format(fname))
        return conversionIds
    def getAdPerformanceReport(self, accountId):
        def doGetAdPerformanceReport(accountId, startDate, endDate, **kwargs):
            fcontents = ''
            nextPageNum = kwargs.get('nextPageNum',0)
            after = kwargs.get('after','after=')
            pageSize = 999999
            fields = [
                'campaign_id',
                'campaign_name',
                'adset_id',
                'adset_name',
                'ad_id',
                'ad_name',
                'date_start',
                'date_stop',
                'impressions',
                # 'labels',
                'objective',
                'clicks',
                'outbound_clicks',
                'spend',
                'conversions',
                'actions',
                # 'ad_format_asset', 
                # 'body_asset', 
                # 'call_to_action_asset', 
                # 'description_asset', 
                # 'image_asset', 
                # 'link_url_asset', 
                # 'title_asset', 
                # 'video_asset', 
            ]
            fields = ','.join(fields)
            params = {
                'time_range': {'since':startDate,'until':endDate},
                'use_account_attribution_setting': 'true',
                'time_increment': 1,
                'filtering': [],
                'level': 'ad',
                'action_report_time': 'conversion',
                'breakdowns': [ 
                    # 'ad_format_asset', 
                    # 'age', 
                    # 'body_asset', 
                    # 'call_to_action_asset', 
                    # 'country', 
                    # 'description_asset', 
                    # 'gender', 
                    # 'image_asset', 
                    # 'impression_device', 
                    # 'link_url_asset', 
                    # 'product_id', 
                    # 'region', 
                    # 'title_asset', 
                    # 'video_asset', 
                    # 'dma', 
                    # 'frequency_value', 
                    # 'hourly_stats_aggregated_by_advertiser_time_zone', 
                    # 'hourly_stats_aggregated_by_audience_time_zone', 
                    # 'place_page_id', 
                    'publisher_platform',
                    'platform_position', 
                    'device_platform'
                ],
                'action_breakdowns' : [
                    'action_type', 
                    'action_target_id',
                    'action_destination',
                    'action_device'
                ],
                
            }
            parameters = []
            keys = params.keys()
            for key in keys:
                val = params[key]
                parameters.append('{}={}'.format(key, val))
            parameters = '&'.join(parameters)
            url = 'https://graph.facebook.com/v21.0/{}/insights?limit={}&level=ad&{}&fields={}&{}'.format(accountId, pageSize, parameters, fields, after)
            print(url)
            fname = '{}:{}:{}:facts-ads:{}:{}:{}:{}'.format(self.hashString(self.userId), self.personId, accountId, startDate, endDate, pageSize, nextPageNum)
            r = self.r_session.get(url)
            try:
                j = r.json()
            except Exception as e:
                raise AirflowException(f"Error occurred processing API response: {e}")
            if 'error' in j:
                raise AirflowException(f"API response returned error message: {j['error']}")
            elif 'data' in j:
                items = j['data']
                for item in items:
                    row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, item['id'], json.dumps(item))
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents, 'facts/ads/{}'.format(fname))
                if 'paging' in j:
                    if 'cursors' in j['paging']:
                        if 'after' in j['paging']['cursors']:
                            after = 'after={}'.format(j['paging']['cursors']['after'])
                            nextPageNum += 1
                            doGetAdPerformanceReport(accountId, startDate, endDate, pageSize, nextPageNum=nextPageNum, after=after)
            else:
                item = {}
                item['msg'] = 'no data'
                conversionId = ''
                row = "{}\t{}\t{}\t{}\t{}\t{}\n".format(self.hashString(self.userId), self.personId, accountId, url, conversionId, json.dumps(item))
                if fcontents == '':
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents, 'facts/ads/{}'.format(fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = self.attWindow)
        delta = endDate - startDate
        for i in range(delta.days):
            interval = str(startDate + timedelta(days=i))
            doGetAdPerformanceReport(accountId, interval, interval)