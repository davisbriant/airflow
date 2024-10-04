import logging
from datetime import date, datetime, time, timedelta
from airflow.decorators import dag, task
import airflow
from include.utils import ddb_utils, s3_utils
import os, sys
import yaml
import pendulum
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pprint import pprint
import simplejson as json
import hashlib

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

@dag(schedule_interval="@daily", catchup=False, default_args=default_args)
# @dag(schedule_interval=None, catchup=False, default_args=default_args)
def googleads():
    global r_session
    r_session = requests.Session()
    retry = Retry(connect=3, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    r_session.mount('http://', adapter)
    r_session.mount('https://', adapter)
    def hashString(string):
        try:
            response = hashlib.md5(string.encode('utf-8')).hexdigest()
            return response
        except Exception as e:
            return e
    @task(task_id="getToken", retries=0)
    def getToken(userId, personId):
        response = ddb_utils.getItem(config['aws']['keyId'], config['aws']['secretAccessKey'], config['googleads']['tableName'], config['dynamodb']['region'], config['googleads']['partKey'], userId)
        payload = response['Item']['payload']
        refresh_token = payload[personId]['token']['refresh_token']
        headers = {"client_id": config['googleads']['clientId'], "client_secret": config['googleads']['clientSecret'], "grant_type": "refresh_token", "refresh_token": refresh_token}
        url = "https://oauth2.googleapis.com/token"
        r = r_session.post(url, json=headers)
        j = r.json()
        j['refresh_token'] = refresh_token
        obj = {}
        obj['token'] = j
        obj['personInfo'] = payload[personId]['personInfo']
        payload[personId] = obj
        ddb_utils.putItem(config['aws']['keyId'],config['aws']['secretAccessKey'],config['googleads']['tableName'], config['dynamodb']['region'], config['googleads']['partKey'], personId, 'payload', payload)
        token = j['access_token']
        return token
    @task(task_id="getHeaders", retries=0)
    def getHeaders(accessToken):
        headers={'Authorization': 'Bearer {}'.format(accessToken), 'login-customer-id': '{}'.format(config['googleads']['mccId']), 'developer-token': '{}'.format(config['googleads']['developerToken'])}
        return headers
    @task(task_id="getAdAccounts", retries=0)
    def getAdAccounts(headers, mccId, userId, personId):
        def doGetAdAccounts(headers, mccId, userId, personId, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            accountList = kwargs.get('accountList', [])
            query = 'SELECT customer_client.client_customer, customer_client.level, customer_client.manager, customer_client.descriptive_name, customer_client.currency_code, customer_client.time_zone, customer_client.id FROM customer_client WHERE customer_client.level > 0'
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(mccId)
            fname = f'{hashString(userId)}:{hashString(personId)}:dims-accounts'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            print(response)
            if "results" in response:
                for item in response['results']:
                    isManager = item['customerClient']['manager']
                    if isManager == False:
                        accountList.append(item['customerClient']['id'])
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{item['customerClient']['id']}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetAdAccounts(headers, mccId, userId, personId, fcontents=fcontents, nextPageToken=nextPageToken, accountList=accountList)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}dims/accounts/{}'.format(config['s3']['reports_prefix_ga'],fname))
                return accountList
            else:
                item = {}
                item['msg'] = 'no data'
                adAccountId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{adAccountId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}dims/accounts/{}'.format(config['s3']['reports_prefix_ga'],fname))
                return None
        response = doGetAdAccounts(headers, mccId, userId, personId)
        return response
    @task(task_id="getAdCampaigns", retries=0)
    def getAdCampaigns(userId, personId, accountId, headers):
        def doGetAdCampaigns(userId, personId, accountId, headers, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = 'SELECT campaign.id, campaign.name, campaign.base_campaign, campaign.start_date, campaign.end_date, campaign.optimization_score, campaign.labels, campaign.campaign_budget, campaign.tracking_url_template, campaign.final_url_suffix, campaign.status FROM campaign'
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:dims-campaigns'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{item['campaign']['id']}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetAdCampaigns(userId, personId, accountId, headers, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}dims/campaigns/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                adCampaignId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{adCampaignId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}dims/campaigns/{}'.format(config['s3']['reports_prefix_ga'],fname))
        doGetAdCampaigns(userId, personId, accountId, headers)
    @task(task_id="getAdGroups", retries=0)
    def getAdGroups(userId, personId, accountId, headers):
        def doGetAdGroups(userId, personId, accountId, headers, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = 'SELECT ad_group.id, ad_group.name, ad_group.status FROM ad_group'
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:dims-adgroups'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{item['adGroup']['id']}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetAdGroups(userId, personId, accountId, headers, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}dims/adgroups/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                adGroupId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{adGroupId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}dims/adgroups/{}'.format(config['s3']['reports_prefix_ga'],fname))
        doGetAdGroups(userId, personId, accountId, headers)
    @task(task_id="getAds", retries=0)
    def getAds(userId, personId, accountId, headers):
        def doGetAds(userId, personId, accountId, headers, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = 'SELECT ad_group_ad.action_items, ad_group_ad.ad.added_by_google_ads, ad_group_ad.ad.app_ad.descriptions, ad_group_ad.ad.app_ad.headlines, ad_group_ad.ad.app_ad.html5_media_bundles, ad_group_ad.ad.app_ad.images, ad_group_ad.ad.app_ad.mandatory_ad_text, ad_group_ad.ad.app_ad.youtube_videos, ad_group_ad.ad.app_engagement_ad.descriptions, ad_group_ad.ad.app_engagement_ad.headlines, ad_group_ad.ad.app_engagement_ad.images, ad_group_ad.ad.app_engagement_ad.videos, ad_group_ad.ad.app_pre_registration_ad.descriptions, ad_group_ad.ad.app_pre_registration_ad.headlines, ad_group_ad.ad.app_pre_registration_ad.images, ad_group_ad.ad.app_pre_registration_ad.youtube_videos, ad_group_ad.ad.call_ad.business_name, ad_group_ad.ad.call_ad.call_tracked, ad_group_ad.ad.call_ad.conversion_action, ad_group_ad.ad.call_ad.conversion_reporting_state, ad_group_ad.ad.call_ad.country_code, ad_group_ad.ad.call_ad.description1, ad_group_ad.ad.call_ad.description2, ad_group_ad.ad.call_ad.disable_call_conversion, ad_group_ad.ad.call_ad.headline1, ad_group_ad.ad.call_ad.headline2, ad_group_ad.ad.call_ad.path1, ad_group_ad.ad.call_ad.path2, ad_group_ad.ad.call_ad.phone_number, ad_group_ad.ad.call_ad.phone_number_verification_url, ad_group_ad.ad.demand_gen_carousel_ad.business_name, ad_group_ad.ad.demand_gen_carousel_ad.call_to_action_text, ad_group_ad.ad.demand_gen_carousel_ad.carousel_cards, ad_group_ad.ad.demand_gen_carousel_ad.description, ad_group_ad.ad.demand_gen_carousel_ad.headline, ad_group_ad.ad.demand_gen_carousel_ad.logo_image, ad_group_ad.ad.demand_gen_multi_asset_ad.business_name, ad_group_ad.ad.demand_gen_multi_asset_ad.call_to_action_text, ad_group_ad.ad.demand_gen_multi_asset_ad.descriptions, ad_group_ad.ad.demand_gen_multi_asset_ad.headlines, ad_group_ad.ad.demand_gen_multi_asset_ad.lead_form_only, ad_group_ad.ad.demand_gen_multi_asset_ad.logo_images, ad_group_ad.ad.demand_gen_multi_asset_ad.marketing_images, ad_group_ad.ad.demand_gen_multi_asset_ad.portrait_marketing_images, ad_group_ad.ad.demand_gen_multi_asset_ad.square_marketing_images, ad_group_ad.ad.demand_gen_product_ad.breadcrumb1, ad_group_ad.ad.demand_gen_product_ad.breadcrumb2, ad_group_ad.ad.demand_gen_product_ad.business_name, ad_group_ad.ad.demand_gen_product_ad.call_to_action, ad_group_ad.ad.demand_gen_product_ad.description, ad_group_ad.ad.demand_gen_product_ad.headline, ad_group_ad.ad.demand_gen_product_ad.logo_image, ad_group_ad.ad.demand_gen_video_responsive_ad.breadcrumb1, ad_group_ad.ad.demand_gen_video_responsive_ad.breadcrumb2, ad_group_ad.ad.demand_gen_video_responsive_ad.business_name, ad_group_ad.ad.demand_gen_video_responsive_ad.call_to_actions, ad_group_ad.ad.demand_gen_video_responsive_ad.descriptions, ad_group_ad.ad.demand_gen_video_responsive_ad.headlines, ad_group_ad.ad.demand_gen_video_responsive_ad.logo_images, ad_group_ad.ad.demand_gen_video_responsive_ad.long_headlines, ad_group_ad.ad.demand_gen_video_responsive_ad.videos, ad_group_ad.ad.device_preference, ad_group_ad.ad.display_upload_ad.display_upload_product_type, ad_group_ad.ad.display_upload_ad.media_bundle, ad_group_ad.ad.display_url, ad_group_ad.ad.expanded_dynamic_search_ad.description, ad_group_ad.ad.expanded_dynamic_search_ad.description2, ad_group_ad.ad.expanded_text_ad.description, ad_group_ad.ad.expanded_text_ad.description2, ad_group_ad.ad.expanded_text_ad.headline_part1, ad_group_ad.ad.expanded_text_ad.headline_part2, ad_group_ad.ad.expanded_text_ad.headline_part3, ad_group_ad.ad.expanded_text_ad.path1, ad_group_ad.ad.expanded_text_ad.path2, ad_group_ad.ad.final_app_urls, ad_group_ad.ad.final_mobile_urls, ad_group_ad.ad.final_url_suffix, ad_group_ad.ad.final_urls, ad_group_ad.ad.hotel_ad, ad_group_ad.ad.id, ad_group_ad.ad.image_ad.image_asset.asset, ad_group_ad.ad.image_ad.image_url, ad_group_ad.ad.image_ad.mime_type, ad_group_ad.ad.image_ad.name, ad_group_ad.ad.image_ad.pixel_width, ad_group_ad.ad.image_ad.pixel_height, ad_group_ad.ad.image_ad.preview_image_url, ad_group_ad.ad.image_ad.preview_pixel_height, ad_group_ad.ad.image_ad.preview_pixel_width, ad_group_ad.ad.legacy_app_install_ad, ad_group_ad.ad.legacy_responsive_display_ad.accent_color, ad_group_ad.ad.legacy_responsive_display_ad.allow_flexible_color, ad_group_ad.ad.legacy_responsive_display_ad.business_name, ad_group_ad.ad.legacy_responsive_display_ad.call_to_action_text, ad_group_ad.ad.legacy_responsive_display_ad.description, ad_group_ad.ad.legacy_responsive_display_ad.format_setting, ad_group_ad.ad.legacy_responsive_display_ad.logo_image, ad_group_ad.ad.legacy_responsive_display_ad.long_headline, ad_group_ad.ad.legacy_responsive_display_ad.main_color, ad_group_ad.ad.legacy_responsive_display_ad.marketing_image, ad_group_ad.ad.legacy_responsive_display_ad.price_prefix, ad_group_ad.ad.legacy_responsive_display_ad.promo_text, ad_group_ad.ad.legacy_responsive_display_ad.short_headline, ad_group_ad.ad.legacy_responsive_display_ad.square_logo_image, ad_group_ad.ad.legacy_responsive_display_ad.square_marketing_image, ad_group_ad.ad.local_ad.call_to_actions, ad_group_ad.ad.local_ad.descriptions, ad_group_ad.ad.local_ad.headlines, ad_group_ad.ad.local_ad.logo_images, ad_group_ad.ad.local_ad.marketing_images, ad_group_ad.ad.local_ad.path1, ad_group_ad.ad.local_ad.path2, ad_group_ad.ad.local_ad.videos, ad_group_ad.ad.name, ad_group_ad.ad.resource_name, ad_group_ad.ad.responsive_display_ad.accent_color, ad_group_ad.ad.responsive_display_ad.allow_flexible_color, ad_group_ad.ad.responsive_display_ad.business_name, ad_group_ad.ad.responsive_display_ad.call_to_action_text, ad_group_ad.ad.responsive_display_ad.control_spec.enable_asset_enhancements, ad_group_ad.ad.responsive_display_ad.control_spec.enable_autogen_video, ad_group_ad.ad.responsive_display_ad.descriptions, ad_group_ad.ad.responsive_display_ad.format_setting, ad_group_ad.ad.responsive_display_ad.headlines, ad_group_ad.ad.responsive_display_ad.logo_images, ad_group_ad.ad.responsive_display_ad.long_headline, ad_group_ad.ad.responsive_display_ad.main_color, ad_group_ad.ad.responsive_display_ad.marketing_images, ad_group_ad.ad.responsive_display_ad.price_prefix, ad_group_ad.ad.responsive_display_ad.promo_text, ad_group_ad.ad.responsive_display_ad.square_logo_images, ad_group_ad.ad.responsive_display_ad.square_marketing_images, ad_group_ad.ad.responsive_display_ad.youtube_videos, ad_group_ad.ad.responsive_search_ad.descriptions, ad_group_ad.ad.responsive_search_ad.headlines, ad_group_ad.ad.responsive_search_ad.path1, ad_group_ad.ad.responsive_search_ad.path2, ad_group_ad.ad.shopping_comparison_listing_ad.headline, ad_group_ad.ad.shopping_product_ad, ad_group_ad.ad.shopping_smart_ad, ad_group_ad.ad.smart_campaign_ad.descriptions, ad_group_ad.ad.smart_campaign_ad.headlines, ad_group_ad.ad.system_managed_resource_source, ad_group_ad.ad.text_ad.description1, ad_group_ad.ad.text_ad.description2, ad_group_ad.ad.text_ad.headline, ad_group_ad.ad.tracking_url_template, ad_group_ad.ad.travel_ad, ad_group_ad.ad.type, ad_group_ad.ad.url_collections, ad_group_ad.ad.url_custom_parameters, ad_group_ad.ad.video_ad.bumper.action_button_label, ad_group_ad.ad.video_ad.bumper.action_headline, ad_group_ad.ad.video_ad.bumper.companion_banner.asset, ad_group_ad.ad.video_ad.in_feed.description1, ad_group_ad.ad.video_ad.in_feed.description2, ad_group_ad.ad.video_ad.in_feed.headline, ad_group_ad.ad.video_ad.in_feed.thumbnail, ad_group_ad.ad.video_ad.in_stream.action_button_label, ad_group_ad.ad.video_ad.in_stream.action_headline, ad_group_ad.ad.video_ad.in_stream.companion_banner.asset, ad_group_ad.ad.video_ad.non_skippable.action_button_label, ad_group_ad.ad.video_ad.non_skippable.action_headline, ad_group_ad.ad.video_ad.non_skippable.companion_banner.asset, ad_group_ad.ad.video_ad.out_stream.description, ad_group_ad.ad.video_ad.out_stream.headline, ad_group_ad.ad.video_ad.video.asset, ad_group_ad.ad.video_responsive_ad.breadcrumb1, ad_group_ad.ad.video_responsive_ad.breadcrumb2, ad_group_ad.ad.video_responsive_ad.call_to_actions, ad_group_ad.ad.video_responsive_ad.companion_banners, ad_group_ad.ad.video_responsive_ad.descriptions, ad_group_ad.ad.video_responsive_ad.headlines, ad_group_ad.ad.video_responsive_ad.long_headlines, ad_group_ad.ad.video_responsive_ad.videos, ad_group_ad.ad_group, ad_group_ad.ad_strength, ad_group_ad.labels, ad_group_ad.policy_summary.approval_status, ad_group_ad.policy_summary.policy_topic_entries, ad_group_ad.policy_summary.review_status, ad_group_ad.primary_status, ad_group_ad.primary_status_reasons, ad_group_ad.resource_name, ad_group_ad.status FROM ad_group_ad'
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:dims-ads'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    resourceId = item['adGroupAd']['resourceName'].split('/')[-1]
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetAds(userId, personId, accountId, headers, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}dims/ads/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                adId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{adId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}dims/ads/{}'.format(config['s3']['reports_prefix_ga'],fname))
        doGetAds(userId, personId, accountId, headers)
    @task(task_id="getAdLabels", retries=0)
    def getAdLabels(userId, personId, accountId, headers):
        def doGetAdLabels(userId, personId, accountId, headers, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = 'SELECT ad_group_ad.ad.id, label.resource_name, label.name FROM ad_group_ad_label'
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:dims-adlabels'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    resourceId = item['adGroupAdLabel']['resourceName'].split('/')[-1]
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetAdLabels(userId, personId, accountId, headers, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}dims/adlabels/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                adId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{adId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}dims/adlabels/{}'.format(config['s3']['reports_prefix_ga'],fname))
        doGetAdLabels(userId, personId, accountId, headers)
    @task(task_id="getKws", retries=0)
    def getKws(headers, userId, personId, accountId):
        def doGetKws(headers, userId, personId, accountId, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = 'SELECT ad_group_criterion.criterion_id, ad_group_criterion.approval_status, ad_group_criterion.keyword.text, ad_group_criterion.final_mobile_urls, ad_group_criterion.final_url_suffix, ad_group_criterion.final_urls, ad_group_criterion.negative, ad_group_criterion.keyword.match_type, ad_group_criterion.status, ad_group_criterion.system_serving_status, ad_group_criterion.tracking_url_template, ad_group_criterion.url_custom_parameters, ad_group_criterion.topic.topic_constant FROM ad_group_criterion'
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:dims-kws'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    resourceId = item['adGroupCriterion']['resourceName'].split('/')[-1]
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetKws(headers, userId, personId, accountId, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}dims/kws/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                kwId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{kwId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}dims/kws/{}'.format(config['s3']['reports_prefix_ga'],fname))
        doGetKws(headers, userId, personId, accountId)
    @task(task_id="getKwLabels", retries=0)
    def getKwLabels(headers, userId, personId, accountId):
        def doGetKwLabels(headers, userId, personId, accountId, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = 'SELECT ad_group_criterion.criterion_id, label.resource_name, label.name FROM ad_group_criterion_label'
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:dims-kwlabels'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    resourceId = item['adGroupCriterionLabel']['resourceName'].split('/')[-1]
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetKwLabels(headers, userId, personId, accountId, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}dims/kwlabels/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                kwId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{kwId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}dims/kwlabels/{}'.format(config['s3']['reports_prefix_ga'],fname))
        doGetKwLabels(headers, userId, personId, accountId)
    @task(task_id="getAdPerformanceReport", retries=0)
    def getAdPerformanceReport(userId, personId, accountId, headers):
        def doGetAdPerformanceReport(userId, personId, accountId, headers, startDate, endDate, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = "SELECT metrics.absolute_top_impression_percentage, metrics.active_view_impressions, metrics.active_view_measurability, metrics.active_view_measurable_cost_micros, metrics.active_view_measurable_impressions, metrics.active_view_viewability, ad_group_ad.ad_group, segments.ad_network_type, metrics.all_conversions_value, metrics.all_conversions, ad_group.base_ad_group, campaign.base_campaign, campaign.id, metrics.clicks, metrics.conversions_value, metrics.conversions, metrics.cost_micros, metrics.cross_device_conversions, metrics.current_model_attributed_conversions_value, metrics.current_model_attributed_conversions, segments.date, segments.device, metrics.engagements, customer.id, metrics.gmail_forwards, metrics.gmail_saves, metrics.gmail_secondary_clicks, ad_group_ad.ad.id, metrics.impressions, metrics.interaction_event_types, metrics.interactions, metrics.top_impression_percentage, metrics.video_quartile_p100_rate, metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p75_rate, metrics.video_views, metrics.view_through_conversions FROM ad_group_ad WHERE segments.date BETWEEN '{}' AND '{}'".format(startDate, endDate)
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:facts-ads:{startDate}:{endDate}'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    resourceId = item['adGroupAd']['resourceName'].split('/')[-1]
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetAdPerformanceReport(userId, personId, accountId, headers, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}facts/ads/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                adId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{adId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}facts/ads/{}'.format(config['s3']['reports_prefix_ga'],fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = config['googleads']['attWindow'])
        delta = endDate - startDate
        for i in range(delta.days):
            interval = startDate + timedelta(days=i)
            doGetAdPerformanceReport(userId, personId, accountId, headers, interval, interval)
    @task(task_id="getKwPerformanceReport", retries=0)
    def getKwPerformanceReport(userId, personId, accountId, headers):
        def doGetKwPerformanceReport(userId, personId, accountId, headers, startDate, endDate, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = "SELECT metrics.absolute_top_impression_percentage, metrics.active_view_impressions, metrics.active_view_measurability, metrics.active_view_measurable_cost_micros, metrics.active_view_measurable_impressions, metrics.active_view_viewability, ad_group.id, segments.ad_network_type, metrics.all_conversions_value, metrics.all_conversions, ad_group.base_ad_group, campaign.base_campaign, campaign.id, metrics.clicks, metrics.conversions_value, metrics.conversions, metrics.cost_micros, ad_group_criterion.effective_cpc_bid_micros, ad_group_criterion.effective_cpc_bid_source, ad_group_criterion.effective_cpm_bid_micros, ad_group_criterion.quality_info.creative_quality_score, metrics.cross_device_conversions, metrics.current_model_attributed_conversions_value, metrics.current_model_attributed_conversions, segments.date, segments.device, metrics.engagements, ad_group_criterion.position_estimates.estimated_add_clicks_at_first_position_cpc, ad_group_criterion.position_estimates.estimated_add_cost_at_first_position_cpc, customer.id, ad_group_criterion.position_estimates.first_page_cpc_micros, ad_group_criterion.position_estimates.first_position_cpc_micros, metrics.gmail_forwards, metrics.gmail_saves, metrics.gmail_secondary_clicks, ad_group_criterion.criterion_id, metrics.impressions, metrics.interaction_rate, metrics.interaction_event_types, metrics.interactions, ad_group_criterion.quality_info.post_click_quality_score, ad_group_criterion.quality_info.quality_score, metrics.search_absolute_top_impression_share, metrics.search_budget_lost_absolute_top_impression_share, metrics.search_budget_lost_top_impression_share, metrics.search_exact_match_impression_share, metrics.search_impression_share, ad_group_criterion.quality_info.search_predicted_ctr, metrics.search_rank_lost_absolute_top_impression_share, metrics.search_rank_lost_impression_share, metrics.search_rank_lost_top_impression_share, metrics.search_top_impression_share, metrics.top_impression_percentage, ad_group_criterion.position_estimates.top_of_page_cpc_micros, metrics.video_quartile_p100_rate, metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p75_rate, metrics.video_views, metrics.view_through_conversions FROM keyword_view WHERE segments.date BETWEEN '{}' AND '{}'".format(startDate, endDate)
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:facts-kws:{startDate}:{endDate}'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    resourceId = item['adGroupCriterion']['resourceName'].split('/')[-1]
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetKwPerformanceReport(userId, personId, accountId, headers, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}facts/kws/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                kwId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{kwId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}facts/kws/{}'.format(config['s3']['reports_prefix_ga'],fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = config['googleads']['attWindow'])
        delta = endDate - startDate
        for i in range(delta.days):
            interval = startDate + timedelta(days=i)
            doGetKwPerformanceReport(userId, personId, accountId, headers, interval, interval)
    @task(task_id="getClickPerformanceReport", retries=0)
    def getClickPerformanceReport(userId, personId, accountId, headers):
        def doGetClickPerformanceReport(userId, personId, accountId, headers, startDate, endDate, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = "SELECT click_view.ad_group_ad, click_view.area_of_interest.city, click_view.area_of_interest.country, click_view.area_of_interest.metro, click_view.area_of_interest.most_specific, click_view.area_of_interest.region, click_view.campaign_location_target, click_view.gclid, click_view.keyword, click_view.keyword_info.match_type, click_view.keyword_info.text, click_view.location_of_presence.city, click_view.location_of_presence.country, click_view.location_of_presence.metro, click_view.location_of_presence.most_specific, click_view.location_of_presence.region, click_view.page_number, click_view.resource_name, click_view.user_list, metrics.clicks, customer.id, customer.manager, segments.date, segments.device, segments.slot, segments.click_type, segments.ad_network_type, campaign.id, ad_group.id, ad_group.name, campaign.name, customer.resource_name FROM click_view WHERE segments.date BETWEEN '{}' AND '{}'".format(startDate, endDate)
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:facts-clks:{startDate}:{endDate}'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{item['clickView']['gclid']}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetClickPerformanceReport(userId, personId, accountId, headers, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}facts/clks/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                clickId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{clickId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}facts/clks/{}'.format(config['s3']['reports_prefix_ga'],fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = 90)
        delta = endDate - startDate
        for i in range(delta.days):
            interval = startDate + timedelta(days=i)
            doGetClickPerformanceReport(userId, personId, accountId, headers, interval, interval)
    @task(task_id="getChangeEventReport", retries=0)
    def getChangeEventReport(userId, personId, accountId, headers):
        def doGetChangeEventReport(userId, personId, accountId, headers, startDate, endDate, **kwargs):
            fcontents = kwargs.get('fcontents', '')
            nextPageToken = kwargs.get('nextPageToken', '')
            query = "SELECT change_event.resource_name, change_event.change_date_time, change_event.change_resource_name, change_event.user_email, change_event.client_type, change_event.change_resource_type, change_event.old_resource, change_event.new_resource, change_event.resource_change_operation, change_event.changed_fields FROM change_event WHERE change_event.change_date_time <= '{} 23:59:59' AND change_event.change_date_time >= '{} 00:00:00' ORDER BY change_event.change_date_time DESC LIMIT 10000".format(endDate, startDate)
            url = 'https://googleads.googleapis.com/v17/customers/{}/googleAds:search'.format(accountId)
            fname = f'{hashString(userId)}:{hashString(personId)}:{accountId}:chevs:{startDate}:{endDate}'
            payload = {"query": query, "pageToken": nextPageToken}
            r_session.headers.update(headers)
            r = r_session.post(url, json=payload)
            response = r.json()
            if "results" in response:
                for item in response['results']:
                    resourceId = item['changeEvent']['resourceName'].split('/')[-1]
                    row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{resourceId}\t{url}\t{query}\t{json.dumps(item)}\n"
                    fcontents += row
                if "nextPageToken" in response:
                    nextPageToken = response['nextPageToken']
                    doGetChangeEventReport(userId, personId, accountId, headers, fcontents=fcontents, nextPageToken=nextPageToken)
                else:
                    s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents, config['s3']['bucket'],'{}chevs/{}'.format(config['s3']['reports_prefix_ga'],fname))
            else:
                item = {}
                item['msg'] = 'no data'
                chevId = 'none'
                row = f"{hashString(userId)}\t{hashString(personId)}\t{accountId}\t{chevId}\t{url}\t{query}\t{json.dumps(item)}\n"
                fcontents += row
                s3_utils.writeToS3(config['aws']['keyId'], config['aws']['secretAccessKey'], fcontents,config['s3']['bucket'],'{}chevs/{}'.format(config['s3']['reports_prefix_ga'],fname))
        endDate = date.today()
        startDate = date.today() - timedelta(days = 30)
        delta = endDate - startDate
        for i in range(delta.days):
            interval = startDate + timedelta(days=i)
            doGetChangeEventReport(userId, personId, accountId, headers, interval, interval)

    accessToken = getToken(config['googleads']['userId'],config['googleads']['personId'])
    headers = getHeaders(accessToken)
    adAccounts = getAdAccounts(headers, config['googleads']['mccId'], config['googleads']['userId'],config['googleads']['personId'])
    adCampaigns = getAdCampaigns.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    adGroups = getAdGroups.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    ads = getAds.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    adLabels = getAdLabels.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    kws = getKws.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    kwLabels = getKwLabels.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    adPerformanceReport = getAdPerformanceReport.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    kwPerformanceReport = getKwPerformanceReport.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    clickPerformanceReport = getClickPerformanceReport.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    changeEventReport = getChangeEventReport.partial(headers=headers,userId=config['googleads']['userId'],personId=config['googleads']['personId']).expand(accountId=adAccounts)
    r_session.close()

googleads = googleads()

