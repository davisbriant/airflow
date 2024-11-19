from .ddb_utils import ddbUtils
from .s3_utils import s3Utils
from datetime import date, datetime, time, timedelta
import requests
import simplejson as json
import hashlib
from pprint import pprint
from airflow import AirflowException

class extractReports:
    def __init__(self, config, r_session):
        self.config = config
        self.r_session = r_session
        self.tableName = config['googleads']['tableName']
        self.partKey = config['googleads']['partKey']
        self.personId = config['googleads']['personId']
        self.userId = config['googleads']['userId']
        self.clientId = config['googleads']['clientId']
        self.clientSecret = config['googleads']['clientSecret']
        self.developerToken = config['googleads']['developerToken']
        self.mccId = config['googleads']['mccId']
        self.attWindow = config['googleads']['attWindow']
    def hashString(self, string):
        response = hashlib.md5(string.encode('utf-8')).hexdigest()
        return response
    def getToken(self, **kwargs):
        response = ddbUtils(self.config).getItem(self.tableName,self.partKey,self.userId)
        payload = response['Item']['payload']
        # pprint(payload)
        refresh_token = kwargs.get('refresh_token', payload[self.personId]['token']['refresh_token'])
        headers = {"client_id": self.clientId, "client_secret": self.clientSecret, "grant_type": "refresh_token", "refresh_token": refresh_token}
        url = "https://oauth2.googleapis.com/token"
        r = self.r_session.post(url, json=headers)
        try:
            j = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in j:
            raise AirflowException(f"API response returned error message: {j}")
        obj = {}
        obj['token'] = j
        obj['token']['refresh_token'] = refresh_token
        obj['personInfo'] = payload[self.personId]['personInfo']
        payload[self.personId] = obj
        # pprint(payload)
        ddbUtils(self.config).putItem(self.tableName, self.partKey, self.userId, 'payload', payload)
        token = j['access_token']
        headers={'Authorization': 'Bearer {}'.format(token), 'login-customer-id': '{}'.format(self.mccId), 'developer-token': '{}'.format(self.developerToken)}
        return headers
    def getAdAccounts(self, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageToken = kwargs.get('nextPageToken', None)
        pageSize = 1000
        nextPageNum = kwargs.get('nextPageNum',0)
        accounts = kwargs.get('accounts', [])
        query = 'SELECT customer_client.client_customer, customer_client.level, customer_client.manager, customer_client.descriptive_name, customer_client.currency_code, customer_client.time_zone, customer_client.id, customer_client.status FROM customer_client WHERE customer_client.level > 0'
        url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(self.mccId)
        fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:dims-accounts:{interval}:{interval}:{pageSize}:{nextPageNum}'
        payload = {"query": query, "pageToken": nextPageToken}
        r = self.r_session.post(url, json=payload)
        try:
            response = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in response:
            raise AirflowException(f"API response returned error message: {response}")
        elif "results" in response:
            for item in response['results']:
                isManager = item['customerClient']['manager']
                if isManager == False and item['customerClient']['status'] == 'ENABLED':
                    accounts.append(item['customerClient']['id'])
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{item['customerClient']['id']}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/accounts/{}'.format(fname))
            if "nextPageToken" in response:
                nextPageToken = response['nextPageToken']
                nextPageNum += 1
                self.getAdAccounts(nextPageNum=nextPageNum, nextPageToken=nextPageToken, accounts=accounts)
        else:
            item = {}
            item['msg'] = 'no data'
            adAccountId = 'none'
            row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{adAccountId}\t{url}\t{query}\t{json.dumps(item)}\n"
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/accounts/{}'.format(fname))
        return accounts
    def getAdCampaigns(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageToken = kwargs.get('nextPageToken', None)
        pageSize = 1000
        nextPageNum = kwargs.get('nextPageNum',0)
        campaigns = kwargs.get('campaigns', [])
        query = 'SELECT campaign.id, campaign.name, campaign.base_campaign, campaign.start_date, campaign.end_date, campaign.optimization_score, campaign.labels, campaign.campaign_budget, campaign.tracking_url_template, campaign.final_url_suffix, campaign.status FROM campaign'
        url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
        fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:dims-campaigns:{interval}:{interval}:{pageSize}:{nextPageNum}'
        payload = {"query": query, "pageToken": nextPageToken}
        r = self.r_session.post(url, json=payload)
        try:
            response = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in response:
            raise AirflowException(f"API response returned error message: {response}")
        elif "results" in response:
            for item in response['results']:
                campaigns.append(item['campaign']['id'])
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{item['campaign']['id']}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/campaigns/{}'.format(fname))
            if "nextPageToken" in response:
                nextPageToken = response['nextPageToken']
                nextPageNum += 1
                self.getAdCampaigns(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken, campaigns=campaigns)
        else:
            item = {}
            item['msg'] = 'no data'
            adCampaignId = 'none'
            row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{adCampaignId}\t{url}\t{query}\t{json.dumps(item)}\n"
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/campaigns/{}'.format(fname))
        return campaigns
    def getAdGroups(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageToken = kwargs.get('nextPageToken', None)
        pageSize = 1000
        nextPageNum = kwargs.get('nextPageNum',0)
        adgroups = kwargs.get('adgroups', [])
        query = 'SELECT ad_group.id, ad_group.name, ad_group.status FROM ad_group'
        url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
        fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:dims-adgroups:{interval}:{interval}:{pageSize}:{nextPageNum}'
        payload = {"query": query, "pageToken": nextPageToken}
        r = self.r_session.post(url, json=payload)
        try:
            response = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in response:
            raise AirflowException(f"API response returned error message: {response}")
        elif "results" in response:
            for item in response['results']:
                adgroups.append(item['adGroup']['id'])
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{item['adGroup']['id']}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/adgroups/{}'.format(fname))
            if "nextPageToken" in response:
                nextPageToken = response['nextPageToken']
                nextPageNum += 1
                self.getAdGroups(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken, adgroups=adgroups)
        else:
            item = {}
            item['msg'] = 'no data'
            adGroupId = 'none'
            row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{adGroupId}\t{url}\t{query}\t{json.dumps(item)}\n"
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/adgroups/{}'.format(fname))
        return adgroups
    def getAds(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageToken = kwargs.get('nextPageToken', None)
        pageSize = 1000
        nextPageNum = kwargs.get('nextPageNum',0)
        ads = kwargs.get('ads', [])
        query = 'SELECT ad_group_ad.action_items, ad_group_ad.ad.added_by_google_ads, ad_group_ad.ad.app_ad.descriptions, ad_group_ad.ad.app_ad.headlines, ad_group_ad.ad.app_ad.html5_media_bundles, ad_group_ad.ad.app_ad.images, ad_group_ad.ad.app_ad.mandatory_ad_text, ad_group_ad.ad.app_ad.youtube_videos, ad_group_ad.ad.app_engagement_ad.descriptions, ad_group_ad.ad.app_engagement_ad.headlines, ad_group_ad.ad.app_engagement_ad.images, ad_group_ad.ad.app_engagement_ad.videos, ad_group_ad.ad.app_pre_registration_ad.descriptions, ad_group_ad.ad.app_pre_registration_ad.headlines, ad_group_ad.ad.app_pre_registration_ad.images, ad_group_ad.ad.app_pre_registration_ad.youtube_videos, ad_group_ad.ad.call_ad.business_name, ad_group_ad.ad.call_ad.call_tracked, ad_group_ad.ad.call_ad.conversion_action, ad_group_ad.ad.call_ad.conversion_reporting_state, ad_group_ad.ad.call_ad.country_code, ad_group_ad.ad.call_ad.description1, ad_group_ad.ad.call_ad.description2, ad_group_ad.ad.call_ad.disable_call_conversion, ad_group_ad.ad.call_ad.headline1, ad_group_ad.ad.call_ad.headline2, ad_group_ad.ad.call_ad.path1, ad_group_ad.ad.call_ad.path2, ad_group_ad.ad.call_ad.phone_number, ad_group_ad.ad.call_ad.phone_number_verification_url, ad_group_ad.ad.demand_gen_carousel_ad.business_name, ad_group_ad.ad.demand_gen_carousel_ad.call_to_action_text, ad_group_ad.ad.demand_gen_carousel_ad.carousel_cards, ad_group_ad.ad.demand_gen_carousel_ad.description, ad_group_ad.ad.demand_gen_carousel_ad.headline, ad_group_ad.ad.demand_gen_carousel_ad.logo_image, ad_group_ad.ad.demand_gen_multi_asset_ad.business_name, ad_group_ad.ad.demand_gen_multi_asset_ad.call_to_action_text, ad_group_ad.ad.demand_gen_multi_asset_ad.descriptions, ad_group_ad.ad.demand_gen_multi_asset_ad.headlines, ad_group_ad.ad.demand_gen_multi_asset_ad.lead_form_only, ad_group_ad.ad.demand_gen_multi_asset_ad.logo_images, ad_group_ad.ad.demand_gen_multi_asset_ad.marketing_images, ad_group_ad.ad.demand_gen_multi_asset_ad.portrait_marketing_images, ad_group_ad.ad.demand_gen_multi_asset_ad.square_marketing_images, ad_group_ad.ad.demand_gen_product_ad.breadcrumb1, ad_group_ad.ad.demand_gen_product_ad.breadcrumb2, ad_group_ad.ad.demand_gen_product_ad.business_name, ad_group_ad.ad.demand_gen_product_ad.call_to_action, ad_group_ad.ad.demand_gen_product_ad.description, ad_group_ad.ad.demand_gen_product_ad.headline, ad_group_ad.ad.demand_gen_product_ad.logo_image, ad_group_ad.ad.demand_gen_video_responsive_ad.breadcrumb1, ad_group_ad.ad.demand_gen_video_responsive_ad.breadcrumb2, ad_group_ad.ad.demand_gen_video_responsive_ad.business_name, ad_group_ad.ad.demand_gen_video_responsive_ad.call_to_actions, ad_group_ad.ad.demand_gen_video_responsive_ad.descriptions, ad_group_ad.ad.demand_gen_video_responsive_ad.headlines, ad_group_ad.ad.demand_gen_video_responsive_ad.logo_images, ad_group_ad.ad.demand_gen_video_responsive_ad.long_headlines, ad_group_ad.ad.demand_gen_video_responsive_ad.videos, ad_group_ad.ad.device_preference, ad_group_ad.ad.display_upload_ad.display_upload_product_type, ad_group_ad.ad.display_upload_ad.media_bundle, ad_group_ad.ad.display_url, ad_group_ad.ad.expanded_dynamic_search_ad.description, ad_group_ad.ad.expanded_dynamic_search_ad.description2, ad_group_ad.ad.expanded_text_ad.description, ad_group_ad.ad.expanded_text_ad.description2, ad_group_ad.ad.expanded_text_ad.headline_part1, ad_group_ad.ad.expanded_text_ad.headline_part2, ad_group_ad.ad.expanded_text_ad.headline_part3, ad_group_ad.ad.expanded_text_ad.path1, ad_group_ad.ad.expanded_text_ad.path2, ad_group_ad.ad.final_app_urls, ad_group_ad.ad.final_mobile_urls, ad_group_ad.ad.final_url_suffix, ad_group_ad.ad.final_urls, ad_group_ad.ad.hotel_ad, ad_group_ad.ad.id, ad_group_ad.ad.image_ad.image_asset.asset, ad_group_ad.ad.image_ad.image_url, ad_group_ad.ad.image_ad.mime_type, ad_group_ad.ad.image_ad.name, ad_group_ad.ad.image_ad.pixel_width, ad_group_ad.ad.image_ad.pixel_height, ad_group_ad.ad.image_ad.preview_image_url, ad_group_ad.ad.image_ad.preview_pixel_height, ad_group_ad.ad.image_ad.preview_pixel_width, ad_group_ad.ad.legacy_app_install_ad, ad_group_ad.ad.legacy_responsive_display_ad.accent_color, ad_group_ad.ad.legacy_responsive_display_ad.allow_flexible_color, ad_group_ad.ad.legacy_responsive_display_ad.business_name, ad_group_ad.ad.legacy_responsive_display_ad.call_to_action_text, ad_group_ad.ad.legacy_responsive_display_ad.description, ad_group_ad.ad.legacy_responsive_display_ad.format_setting, ad_group_ad.ad.legacy_responsive_display_ad.logo_image, ad_group_ad.ad.legacy_responsive_display_ad.long_headline, ad_group_ad.ad.legacy_responsive_display_ad.main_color, ad_group_ad.ad.legacy_responsive_display_ad.marketing_image, ad_group_ad.ad.legacy_responsive_display_ad.price_prefix, ad_group_ad.ad.legacy_responsive_display_ad.promo_text, ad_group_ad.ad.legacy_responsive_display_ad.short_headline, ad_group_ad.ad.legacy_responsive_display_ad.square_logo_image, ad_group_ad.ad.legacy_responsive_display_ad.square_marketing_image, ad_group_ad.ad.local_ad.call_to_actions, ad_group_ad.ad.local_ad.descriptions, ad_group_ad.ad.local_ad.headlines, ad_group_ad.ad.local_ad.logo_images, ad_group_ad.ad.local_ad.marketing_images, ad_group_ad.ad.local_ad.path1, ad_group_ad.ad.local_ad.path2, ad_group_ad.ad.local_ad.videos, ad_group_ad.ad.name, ad_group_ad.ad.resource_name, ad_group_ad.ad.responsive_display_ad.accent_color, ad_group_ad.ad.responsive_display_ad.allow_flexible_color, ad_group_ad.ad.responsive_display_ad.business_name, ad_group_ad.ad.responsive_display_ad.call_to_action_text, ad_group_ad.ad.responsive_display_ad.control_spec.enable_asset_enhancements, ad_group_ad.ad.responsive_display_ad.control_spec.enable_autogen_video, ad_group_ad.ad.responsive_display_ad.descriptions, ad_group_ad.ad.responsive_display_ad.format_setting, ad_group_ad.ad.responsive_display_ad.headlines, ad_group_ad.ad.responsive_display_ad.logo_images, ad_group_ad.ad.responsive_display_ad.long_headline, ad_group_ad.ad.responsive_display_ad.main_color, ad_group_ad.ad.responsive_display_ad.marketing_images, ad_group_ad.ad.responsive_display_ad.price_prefix, ad_group_ad.ad.responsive_display_ad.promo_text, ad_group_ad.ad.responsive_display_ad.square_logo_images, ad_group_ad.ad.responsive_display_ad.square_marketing_images, ad_group_ad.ad.responsive_display_ad.youtube_videos, ad_group_ad.ad.responsive_search_ad.descriptions, ad_group_ad.ad.responsive_search_ad.headlines, ad_group_ad.ad.responsive_search_ad.path1, ad_group_ad.ad.responsive_search_ad.path2, ad_group_ad.ad.shopping_comparison_listing_ad.headline, ad_group_ad.ad.shopping_product_ad, ad_group_ad.ad.shopping_smart_ad, ad_group_ad.ad.smart_campaign_ad.descriptions, ad_group_ad.ad.smart_campaign_ad.headlines, ad_group_ad.ad.system_managed_resource_source, ad_group_ad.ad.text_ad.description1, ad_group_ad.ad.text_ad.description2, ad_group_ad.ad.text_ad.headline, ad_group_ad.ad.tracking_url_template, ad_group_ad.ad.travel_ad, ad_group_ad.ad.type, ad_group_ad.ad.url_collections, ad_group_ad.ad.url_custom_parameters, ad_group_ad.ad.video_ad.bumper.action_button_label, ad_group_ad.ad.video_ad.bumper.action_headline, ad_group_ad.ad.video_ad.bumper.companion_banner.asset, ad_group_ad.ad.video_ad.in_feed.description1, ad_group_ad.ad.video_ad.in_feed.description2, ad_group_ad.ad.video_ad.in_feed.headline, ad_group_ad.ad.video_ad.in_feed.thumbnail, ad_group_ad.ad.video_ad.in_stream.action_button_label, ad_group_ad.ad.video_ad.in_stream.action_headline, ad_group_ad.ad.video_ad.in_stream.companion_banner.asset, ad_group_ad.ad.video_ad.non_skippable.action_button_label, ad_group_ad.ad.video_ad.non_skippable.action_headline, ad_group_ad.ad.video_ad.non_skippable.companion_banner.asset, ad_group_ad.ad.video_ad.out_stream.description, ad_group_ad.ad.video_ad.out_stream.headline, ad_group_ad.ad.video_ad.video.asset, ad_group_ad.ad.video_responsive_ad.breadcrumb1, ad_group_ad.ad.video_responsive_ad.breadcrumb2, ad_group_ad.ad.video_responsive_ad.call_to_actions, ad_group_ad.ad.video_responsive_ad.companion_banners, ad_group_ad.ad.video_responsive_ad.descriptions, ad_group_ad.ad.video_responsive_ad.headlines, ad_group_ad.ad.video_responsive_ad.long_headlines, ad_group_ad.ad.video_responsive_ad.videos, ad_group_ad.ad_group, ad_group_ad.ad_strength, ad_group_ad.labels, ad_group_ad.policy_summary.approval_status, ad_group_ad.policy_summary.policy_topic_entries, ad_group_ad.policy_summary.review_status, ad_group_ad.primary_status, ad_group_ad.primary_status_reasons, ad_group_ad.resource_name, ad_group_ad.status FROM ad_group_ad'
        url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
        fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:dims-ads:{interval}:{interval}:{pageSize}:{nextPageNum}'
        payload = {"query": query, "pageToken": nextPageToken}
        r = self.r_session.post(url, json=payload)
        try:
            response = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in response:
            raise AirflowException(f"API response returned error message: {response}")
        elif "results" in response:
            for item in response['results']:
                resourceId = item['adGroupAd']['resourceName'].split('/')[-1]
                ads.append(resourceId)
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/ads/{}'.format(fname))
            if "nextPageToken" in response:
                nextPageToken = response['nextPageToken']
                nextPageNum += 1
                self.getAds(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken, ads=ads)
        else:
            item = {}
            item['msg'] = 'no data'
            adId = 'none'
            row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{adId}\t{url}\t{query}\t{json.dumps(item)}\n"
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/ads/{}'.format(fname))
        return ads
    def getAdLabels(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageToken = kwargs.get('nextPageToken', None)
        pageSize = 1000
        nextPageNum = kwargs.get('nextPageNum',0)
        adLabels = kwargs.get('adLabels', [])
        query = 'SELECT ad_group_ad.ad.id, label.resource_name, label.name FROM ad_group_ad_label'
        url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
        fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:dims-adlabels:{interval}:{interval}:{pageSize}:{nextPageNum}'
        payload = {"query": query, "pageToken": nextPageToken}
        r = self.r_session.post(url, json=payload)
        try:
            response = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in response:
            raise AirflowException(f"API response returned error message: {response}")
        elif "results" in response:
            for item in response['results']:
                resourceId = item['adGroupAdLabel']['resourceName'].split('/')[-1]
                adLabels.append(resourceId)
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/adlabels/{}'.format(fname))
            if "nextPageToken" in response:
                nextPageToken = response['nextPageToken']
                nextPageNum += 1
                self.getAdLabels(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken, adLabels=adLabels)
        else:
            item = {}
            item['msg'] = 'no data'
            adId = 'none'
            row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{adId}\t{url}\t{query}\t{json.dumps(item)}\n"
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/adlabels/{}'.format(fname))
        return adLabels
    def getKws(self, accountId, **kwargs):
        interval = str(date.today())
        nextPageToken = kwargs.get('nextPageToken', None)
        pageSize = 1000
        nextPageNum = kwargs.get('nextPageNum',0)
        fcontents = ''
        kws = kwargs.get('kws', [])
        query = 'SELECT ad_group_criterion.criterion_id, ad_group_criterion.approval_status, ad_group_criterion.keyword.text, ad_group_criterion.final_mobile_urls, ad_group_criterion.final_url_suffix, ad_group_criterion.final_urls, ad_group_criterion.negative, ad_group_criterion.keyword.match_type, ad_group_criterion.status, ad_group_criterion.system_serving_status, ad_group_criterion.tracking_url_template, ad_group_criterion.url_custom_parameters, ad_group_criterion.topic.topic_constant FROM ad_group_criterion'
        url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
        fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:dims-kws:{interval}:{interval}:{pageSize}:{nextPageNum}'
        payload = {"query": query, "pageToken": nextPageToken}
        r = self.r_session.post(url, json=payload)
        try:
            response = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in response:
            raise AirflowException(f"API response returned error message: {response}")
        elif "results" in response:
            for item in response['results']:
                resourceId = item['adGroupCriterion']['resourceName'].split('/')[-1]
                kws.append(resourceId)
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/kws/{}'.format(fname))
            if "nextPageToken" in response:
                nextPageToken = response['nextPageToken']
                nextPageNum += 1
                self.getKws(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken, kws=kws)
        else:
            item = {}
            item['msg'] = 'no data'
            kwId = 'none'
            row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{kwId}\t{url}\t{query}\t{json.dumps(item)}\n"
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/kws/{}'.format(fname))
        return kws
    def getKwLabels(self, accountId, **kwargs):
        interval = str(date.today())
        fcontents = ''
        nextPageToken = kwargs.get('nextPageToken', None)
        pageSize = 1000
        nextPageNum = kwargs.get('nextPageNum',0)
        kwLabels = kwargs.get('kwLabels', [])
        query = 'SELECT ad_group_criterion.criterion_id, label.resource_name, label.name FROM ad_group_criterion_label'
        url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
        fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:dims-kwlabels:{interval}:{interval}:{pageSize}:{nextPageNum}'
        payload = {"query": query, "pageToken": nextPageToken}
        r = self.r_session.post(url, json=payload)
        try:
            response = r.json()
        except Exception as e:
            raise AirflowException(f"Error occurred processing API response {e}")
        if 'error' in response:
            raise AirflowException(f"API response returned error message: {response}")
        elif "results" in response:
            for item in response['results']:
                resourceId = item['adGroupCriterionLabel']['resourceName'].split('/')[-1]
                kwLabels.append(resourceId)
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents, 'dims/kwlabels/{}'.format(fname))
            if "nextPageToken" in response:
                nextPageToken = response['nextPageToken']
                nextPageNum += 1
                self.getKwLabels(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken, kwLabels=kwLabels)
        else:
            item = {}
            item['msg'] = 'no data'
            kwId = 'none'
            row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{kwId}\t{url}\t{query}\t{json.dumps(item)}\n"
            if fcontents == '':
                fcontents += row
            s3Utils(self.config).writeToS3(fcontents,'dims/kwlabels/{}'.format(fname))
        return kwLabels
    def getAdPerformanceReport(self, accountId):
        def doGetAdPerformanceReport(accountId, startDate, endDate, **kwargs):
            fcontents = ''
            nextPageToken = kwargs.get('nextPageToken', None)
            pageSize = 1000
            nextPageNum = kwargs.get('nextPageNum',0)
            query = "SELECT metrics.absolute_top_impression_percentage, metrics.active_view_impressions, metrics.active_view_measurability, metrics.active_view_measurable_cost_micros, metrics.active_view_measurable_impressions, metrics.active_view_viewability, ad_group_ad.ad_group, segments.ad_network_type, metrics.all_conversions_value, metrics.all_conversions, ad_group.base_ad_group, campaign.base_campaign, campaign.id, metrics.clicks, metrics.conversions_value, metrics.conversions, metrics.cost_micros, metrics.cross_device_conversions, metrics.current_model_attributed_conversions_value, metrics.current_model_attributed_conversions, segments.date, segments.device, metrics.engagements, customer.id, metrics.gmail_forwards, metrics.gmail_saves, metrics.gmail_secondary_clicks, ad_group_ad.ad.id, metrics.impressions, metrics.interaction_event_types, metrics.interactions, metrics.top_impression_percentage, metrics.video_quartile_p100_rate, metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p75_rate, metrics.video_views, metrics.view_through_conversions FROM ad_group_ad WHERE segments.date BETWEEN '{}' AND '{}'".format(startDate, endDate)
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:facts-ads:{startDate}:{endDate}:{nextPageNum}'
            payload = {"query": query, "pageToken": nextPageToken}
            r = self.r_session.post(url, json=payload)
            try:
                response = r.json()
            except Exception as e:
                raise AirflowException(f"Error occurred processing API response {e}")
            if 'error' in response:
                raise AirflowException(f"API response returned error message: {response}")
            elif "results" in response:
                for item in response['results']:
                    resourceId = item['adGroupAd']['resourceName'].split('/')[-1]
                    row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents, 'facts/ads/{}'.format(fname))
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    nextPageNum += 1
                    self.getAdPerformanceReport(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken)
            else:
                item = {}
                item['msg'] = 'no data'
                adId = 'none'
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{adId}\t{url}\t{query}\t{json.dumps(item)}\n"
                if fcontents == '':
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents,'facts/ads/{}'.format(fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = self.attWindow)
        delta = endDate - startDate
        for i in range(delta.days):
            interval = str(startDate + timedelta(days=i))
            doGetAdPerformanceReport(accountId, interval, interval)
    def getKwPerformanceReport(self, accountId):
        def doGetKwPerformanceReport(accountId, startDate, endDate, **kwargs):
            fcontents = ''
            nextPageToken = kwargs.get('nextPageToken', None)
            pageSize = 1000
            nextPageNum = kwargs.get('nextPageNum',0)
            query = "SELECT metrics.absolute_top_impression_percentage, metrics.active_view_impressions, metrics.active_view_measurability, metrics.active_view_measurable_cost_micros, metrics.active_view_measurable_impressions, metrics.active_view_viewability, ad_group.id, segments.ad_network_type, metrics.all_conversions_value, metrics.all_conversions, ad_group.base_ad_group, campaign.base_campaign, campaign.id, metrics.clicks, metrics.conversions_value, metrics.conversions, metrics.cost_micros, ad_group_criterion.effective_cpc_bid_micros, ad_group_criterion.effective_cpc_bid_source, ad_group_criterion.effective_cpm_bid_micros, ad_group_criterion.quality_info.creative_quality_score, metrics.cross_device_conversions, metrics.current_model_attributed_conversions_value, metrics.current_model_attributed_conversions, segments.date, segments.device, metrics.engagements, ad_group_criterion.position_estimates.estimated_add_clicks_at_first_position_cpc, ad_group_criterion.position_estimates.estimated_add_cost_at_first_position_cpc, customer.id, ad_group_criterion.position_estimates.first_page_cpc_micros, ad_group_criterion.position_estimates.first_position_cpc_micros, metrics.gmail_forwards, metrics.gmail_saves, metrics.gmail_secondary_clicks, ad_group_criterion.criterion_id, metrics.impressions, metrics.interaction_rate, metrics.interaction_event_types, metrics.interactions, ad_group_criterion.quality_info.post_click_quality_score, ad_group_criterion.quality_info.quality_score, metrics.search_absolute_top_impression_share, metrics.search_budget_lost_absolute_top_impression_share, metrics.search_budget_lost_top_impression_share, metrics.search_exact_match_impression_share, metrics.search_impression_share, ad_group_criterion.quality_info.search_predicted_ctr, metrics.search_rank_lost_absolute_top_impression_share, metrics.search_rank_lost_impression_share, metrics.search_rank_lost_top_impression_share, metrics.search_top_impression_share, metrics.top_impression_percentage, ad_group_criterion.position_estimates.top_of_page_cpc_micros, metrics.video_quartile_p100_rate, metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p75_rate, metrics.video_views, metrics.view_through_conversions FROM keyword_view WHERE segments.date BETWEEN '{}' AND '{}'".format(startDate, endDate)
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:facts-kws:{startDate}:{endDate}:{nextPageNum}'
            payload = {"query": query, "pageToken": nextPageToken}
            r = self.r_session.post(url, json=payload)
            try:
                response = r.json()
            except Exception as e:
                raise AirflowException(f"Error occurred processing API response {e}")
            if 'error' in response:
                raise AirflowException(f"API response returned error message: {response}")
            elif "results" in response:
                for item in response['results']:
                    resourceId = item['adGroupCriterion']['resourceName'].split('/')[-1]
                    row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents, 'facts/kws/{}'.format(fname))
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    nextPageNum += 1
                    doGetKwPerformanceReport(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken)
            else:
                item = {}
                item['msg'] = 'no data'
                kwId = 'none'
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{kwId}\t{url}\t{query}\t{json.dumps(item)}\n"
                if fcontents == '':
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents,'facts/kws/{}'.format(fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = self.attWindow)
        delta = endDate - startDate
        for i in range(delta.days):
            interval = str(startDate + timedelta(days=i))
            doGetKwPerformanceReport(accountId, interval, interval)
    def getClickPerformanceReport(self, accountId):
        def doGetClickPerformanceReport(accountId, startDate, endDate, **kwargs):
            fcontents = ''
            nextPageToken = kwargs.get('nextPageToken', None)
            pageSize = 1000
            nextPageNum = kwargs.get('nextPageNum',0)
            query = "SELECT click_view.ad_group_ad, click_view.area_of_interest.city, click_view.area_of_interest.country, click_view.area_of_interest.metro, click_view.area_of_interest.most_specific, click_view.area_of_interest.region, click_view.campaign_location_target, click_view.gclid, click_view.keyword, click_view.keyword_info.match_type, click_view.keyword_info.text, click_view.location_of_presence.city, click_view.location_of_presence.country, click_view.location_of_presence.metro, click_view.location_of_presence.most_specific, click_view.location_of_presence.region, click_view.page_number, click_view.resource_name, click_view.user_list, metrics.clicks, customer.id, customer.manager, segments.date, segments.device, segments.slot, segments.click_type, segments.ad_network_type, campaign.id, ad_group.id, ad_group.name, campaign.name, customer.resource_name FROM click_view WHERE segments.date BETWEEN '{}' AND '{}'".format(startDate, endDate)
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:facts-clks:{startDate}:{endDate}:{nextPageNum}'
            payload = {"query": query, "pageToken": nextPageToken}
            r = self.r_session.post(url, json=payload)
            try:
                response = r.json()
            except Exception as e:
                raise AirflowException(f"Error occurred processing API response {e}")
            if 'error' in response:
                raise AirflowException(f"API response returned error message: {response}")
            elif "results" in response:
                for item in response['results']:
                    row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{item['clickView']['gclid']}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents, 'facts/clks/{}'.format(fname))
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    nextPageNum += 1
                    doGetClickPerformanceReport(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken)
            else:
                item = {}
                item['msg'] = 'no data'
                clickId = 'none'
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{clickId}\t{url}\t{query}\t{json.dumps(item)}\n"
                if fcontents == '':
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents,'facts/clks/{}'.format(fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = 90)
        delta = endDate - startDate
        for i in range(delta.days):
            interval = str(startDate + timedelta(days=i))
            doGetClickPerformanceReport(accountId, interval, interval)
    def getChangeEventReport(self, accountId):
        def doGetChangeEventReport(accountId, startDate, endDate, **kwargs):
            fcontents = ''
            nextPageToken = kwargs.get('nextPageToken', None)
            pageSize = 1000
            nextPageNum = kwargs.get('nextPageNum',0)
            query = "SELECT change_event.resource_name, change_event.change_date_time, change_event.change_resource_name, change_event.user_email, change_event.client_type, change_event.change_resource_type, change_event.old_resource, change_event.new_resource, change_event.resource_change_operation, change_event.changed_fields FROM change_event WHERE change_event.change_date_time <= '{} 23:59:59' AND change_event.change_date_time >= '{} 00:00:00' ORDER BY change_event.change_date_time DESC LIMIT 10000".format(endDate, startDate)
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{self.hashString(self.userId)}:{self.hashString(self.personId)}:{accountId}:chevs:{startDate}:{endDate}:{nextPageNum}'
            payload = {"query": query, "pageToken": nextPageToken}
            r = self.r_session.post(url, json=payload)
            try:
                response = r.json()
            except Exception as e:
                raise AirflowException(f"Error occurred processing API response {e}")
            if 'error' in response:
                raise AirflowException(f"API response returned error message: {response}")
            elif "results" in response:
                for item in response['results']:
                    resourceId = item['changeEvent']['resourceName'].split('/')[-1]
                    row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents, 'chevs/{}'.format(fname))
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    nextPageNum += 1
                    doGetChangeEventReport(accountId, nextPageNum=nextPageNum, nextPageToken=nextPageToken)
            else:
                item = {}
                item['msg'] = 'no data'
                chevId = 'none'
                row = f"{self.hashString(self.userId)}\t{self.hashString(self.personId)}\t{accountId}\t{chevId}\t{url}\t{query}\t{json.dumps(item)}\n"
                if fcontents == '':
                    fcontents += row
                s3Utils(self.config).writeToS3(fcontents,'chevs/{}'.format(fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = 29)
        delta = endDate - startDate
        for i in range(delta.days):
            interval = str(startDate + timedelta(days=i))
            doGetChangeEventReport(accountId, interval, interval)
