# -*- coding: utf-8 -*-
__author__ = 'Mx'


from flask import jsonify
from gevent.wsgi import WSGIServer

import flask
from flask import Flask
from flask import request
from elasticsearch import Elasticsearch
from flask import Response

import time
import datetime
import json
import copy

import sys
import os
reload(sys)
sys.setdefaultencoding( "utf-8" )

#############################################################





#############################################################
# configurations
confs = {
    "dev":{
        "bind_ip": "0.0.0.0"
        ,"port": 51220
        ,"es_hosts": [{"host": "172.17.6.150", "port": 9201}]
    }
    ,"prd":{
        "bind_ip": "10.31.205.185"
        ,"port": 51220
        ,"es_hosts": [{"host": "127.0.0.1", "port": 9201}]

    }
}
setting = confs['dev']
if os.path.exists('product_env'):
    setting = confs['prd']
#############################################################
# templates
templates = {}
#############################################################
# util functions
def check_params(r):
    rfunc = r.form if str(r.method).upper() == 'POST' else r.args
    result = {}
    result['id'] = rfunc.get('id',0)
    result['size'] = rfunc.get('size',4)
    result['page'] = rfunc.get('page',1)

    result['id'],result['size'],result['page'] = int(result['id']),int(result['size']),int(result['page'])
    if result['id'] == 0: raise Exception('Invalid id:'+str(result['id']))
    if result['size'] <= 0: raise Exception('Invalid size:'+str(result['size']))
    if result['page'] <= 0: raise Exception('Invalid page:'+str(result['page']))
    return result

def api_response(result):
    resp = flask.Response(result)
    resp.headers['content-type'] = 'application/json; charset=utf-8'
    return resp

def get_resource(type,id,es_client):
    result = es_client.search(index='rs'+type,doc_type='r',body={'query':{'term':{type+'_id':id}}})
    if result.has_key('hits') and result['hits'].has_key('hits'):
        r=  result['hits']['hits']
        if isinstance(r,list) and len(r)>0:
            return r[0]['_source']
    return None

def unwrap_es_result(resp,source):
    try:
        result = []
        for a in resp['hits']['hits']:
            t = {}
            if source:
                for s in source:
                    t[s] = a['_source'][s]
            else:
                for s in  a['_source'].keys():
                    t[s] = a['_source'][s]
            result.append(t)
        return  {
            'ok':True,
            'reason':'',
            'data': {
                'list': result,
                'total':resp['hits']['total']
            }
        }
    except Exception,e:
        print 'ES_NO_HITS_FOUND:',resp
        return {'ok':False,'reason':'ES_NO_HITS_FOUND'}

def time_score(dtime):
    if not isinstance(dtime,datetime.datetime):
        dtime = datetime.datetime.strptime(dtime[:19],"%Y-%m-%dT%H:%M:%S")
    days = (datetime.datetime.now() - dtime).days
    # print days
    stage = [7,30,90,180,360,1080,3650,7300,18250]
    for i in range(10):
        if stage[i] > days:
            return 10-i          #1-10

def recommend_by_feature(type, feature, es_client, size, page, least_match=1, source=None, self_id=None):
    # score_vec_int_feature, score_vec_str_feature, score_vec_cnt_feature = 0.3, 0.4, 0.1
    score_vec_int_feature, score_vec_str_feature, score_vec_cnt_feature = 1,1,1
    index_name = 'rs' + type
    loc_source = [type+'_id']

    if 'film' == type:
        vec_int_feature = ['story_place', 'main_style', 'award_information', 'story_source', 'theme_type',
                        'traditional_type', 'story_time']  #7, factor:0.3
        vec_str_feature = ['area', 'type', 'tag']  #3, factor:0.4
        vec_cnt_feature = ['film_article_cnt', 'wemedia_video_cnt', 'comment_cnt', 'film_list_cnt', 'play_cnt', 'favorite_cnt']  #6, factor:0.1
        score_function = "_score" \
                         "+0.1*Math.log(1+doc['comment_cnt'].value + doc['play_cnt'].value + doc['favorite_cnt'].value + doc['film_article_cnt'].value + doc['wemedia_video_cnt'].value + doc['film_list_cnt'].value)" \
                         "+0.05*doc['pingfen'].value" \
                         "+0.5*doc['rec_level'].value" \
                         "+0.1*"+str(time_score(feature['pubdate_m']))
        filter = [{ "term":{"video_status":1} }]                #'video_status'==1  filter
    elif 'article' == type:
        vec_int_feature = ['tag','feature','view','scene','rel_films']
        vec_str_feature = []
        vec_cnt_feature = ['view_cnt','comment_cnt','like_cnt']
        score_function = "_score" \
                         "+0.1*Math.log(1+doc['comment_cnt'].value + doc['view_cnt'].value + doc['like_cnt'].value)" \
                         "+0.1*"+str(time_score(feature['publish_time']))
        filter = []
    elif 'video' == type:
        vec_int_feature = ['rel_films']
        vec_str_feature = ['tags']
        vec_cnt_feature = ['comment_cnt','play_cnt','like_cnt']
        score_function = "_score" \
                         "+0.1*Math.log(1+doc['comment_cnt'].value + doc['play_cnt'].value + doc['like_cnt'].value)" \
                         "+0.1*"+str(time_score(feature['publish_time']))
        filter = []
    else:
        raise Exception('Invalid recommend type:'+str(type))

    features = []
    for f in vec_int_feature:
        vec = feature[f]
        for i in vec:
            features.append({
                "bool" :
                {
                    "boost": score_vec_int_feature,
                    "should":[ {"match": {f: i}} ]
                }
            })
    for f in vec_str_feature:
        vec = feature[f]
        for i in vec:
            features.append({
                "bool" :
                {
                    "boost": score_vec_str_feature,
                    "should":[ {"term": {f: i}} ]
                }
            })
    if self_id:
        filter.append({ "bool":{"must_not":{"term":{type+'_id':self_id} }}})

    body = {
        "size": size,
        "from": (page-1)*size,
        # "_source": ["story_place","feats_effect"],
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "should": features
                        ,"filter": filter          #'video_status'==1  filter
                        ,"minimum_should_match": least_match
                    }
                }
                ,"script_score": {
                    "script": {
                        "lang": "painless",
                        "inline": score_function
                    }
                }
            }
        }
    }
    if source is not None:
        loc_source.extend(source)
        body['_source'] = loc_source
    else:
        loc_source=None

    # print json.dumps(body,ensure_ascii=False)
    result = es_client.search(index_name,doc_type=None,body=json.dumps(body,ensure_ascii=False))

    # print json.dumps(result,ensure_ascii=False)
    return unwrap_es_result(result,loc_source)



def recommend(type, id, size=10, page=0, es_client=None):
    # print type,id
    if not es_client:
        es_client = Elasticsearch(hosts=setting['es_hosts'])
    feature = get_resource(type,id,es_client)
    if not feature:
        raise Exception('No feature source found:'+type+','+str(id))
    # print feature
    return recommend_by_feature(type,feature,es_client,size=size,page=page,source=[],self_id=id)

#############################################################
# flask functions, routers
app = Flask(__name__)

@app.route("/recommend/film", methods=['GET','POST'])
def recommend_film():
    try:
        p = check_params(request)
        result = recommend('film',p['id'],size=p['size'],page=p['page'])
        return api_response(json.dumps(result,ensure_ascii=False))
    except Exception,e:
        print 'recommend_film error:', str(e)
        return api_response(json.dumps({'ok':False, 'reason':str(e)}))

@app.route("/recommend/article", methods=['GET','POST'])
def recommend_article():
    try:
        p = check_params(request)
        result = recommend('article',p['id'],size=p['size'],page=p['page'])
        return api_response(json.dumps(result,ensure_ascii=False))
    except Exception,e:
        print 'recommend_article error:', str(e)
        return api_response(json.dumps({'ok':False, 'reason':str(e)}))

@app.route("/recommend/video", methods=['GET','POST'])
def recommend_video():
    try:
        p = check_params(request)
        result = recommend('video',p['id'],size=p['size'],page=p['page'])
        return api_response(json.dumps(result,ensure_ascii=False))
    except Exception,e:
        print 'recommend_video error:', str(e)
        return api_response(json.dumps({'ok':False, 'reason':str(e)}))



#############################################################
def test01():
    # es_client = Elasticsearch(hosts=setting['es_hosts'])
    # print json.dumps(get_resource('film',101633,es_client),ensure_ascii=False)

    # print time_score(datetime.datetime.strptime('2017-5-10 00:00:00',"%Y-%m-%d %H:%M:%S"))

    print recommend('film',101633)


    pass

if __name__ == '__main__':
    # test01()
    WSGIServer((setting['bind_ip'], setting['port']), app).serve_forever()