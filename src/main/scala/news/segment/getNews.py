# -*- coding: utf-8 -*-
'''
Coding Just for Fun
Created by burness on 16/4/11.
'''

import sys, urllib, urllib2, json

url = 'http://apis.baidu.com/showapi_open_bus/channel_news/channel_news'


req = urllib2.Request(url)

req.add_header("apikey", "2525e730e2c27269c818f7e954d33c51")

resp = urllib2.urlopen(req)
content = resp.read()
if(content):
    # print content
    for i in  json.loads(content)['showapi_res_body']['channelList']:
        category_id = i['channelId']
        category_name = i['name']
        print category_name

