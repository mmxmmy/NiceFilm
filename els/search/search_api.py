#!/usr/bin/python
#! -*- coding:utf-8 -*-
__author__ = 'Mx'

#
# import sys
# reload(sys)
# sys.setdefaultencoding( "utf-8" )

from flask import jsonify
from gevent.wsgi import WSGIServer

import flask
from flask import Flask
from flask import request
from elasticsearch import Elasticsearch
from flask import Response

import time
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
        ,"port": 51219
        ,"es_hosts": [{"host": "172.17.6.150", "port": 9201}]
    }
    ,"prd":{
        "bind_ip": "10.31.205.185"
        ,"port": 51219
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
def check_params(r,func_name):
    rfunc = r.form if str(r.method).upper() == 'POST' else r.args
    result = {}
    if 'search_userinfo'==func_name:
        result['key'] = rfunc.get('key','')
        result['from'] = rfunc.get('from',0)
        result['size'] = rfunc.get('size',10)
        result['highlight'] = rfunc.get('highlight')
        result['highlight'] = False if result['highlight'] is None else True
        result['ver'] = rfunc.get('ver','').strip()

        result['key'] = fine_query(result['key'])
        if result['key'] == '': raise Exception('Empty keyword.')

        return result
    elif 'search_mediainfo'==func_name:
        result['key'] = rfunc.get('key','')
        result['from'] = rfunc.get('from',0)
        result['size'] = rfunc.get('size',10)
        result['highlight'] = rfunc.get('highlight')
        result['highlight'] = False if result['highlight'] is None else True
        result['ver'] = rfunc.get('ver','').strip()
        result['media_type'] = rfunc.get('media_type',-1)

        result['key'] = fine_query(result['key'])
        if result['key'] == '': raise Exception('Empty keyword.')

        return result
    elif 'search_filminfo'==func_name:
        result['key'] = rfunc.get('key','')
        result['from'] = rfunc.get('from',0)
        result['size'] = rfunc.get('size',10)
        result['highlight'] = rfunc.get('highlight')
        result['highlight'] = False if result['highlight'] is None else True
        result['status'] = rfunc.get('status',-1)
        result['ver'] = rfunc.get('ver','').strip()

        result['key'] = fine_query(result['key'])
        if result['key'] == '': raise Exception('Empty keyword.')

        return result
    elif 'search_filmmaker'==func_name:
        result['key'] = rfunc.get('key','')
        result['from'] = rfunc.get('from',0)
        result['size'] = rfunc.get('size',10)
        result['highlight'] = rfunc.get('highlight')
        result['highlight'] = False if result['highlight'] is None else True
        result['ver'] = rfunc.get('ver','').strip()

        result['key'] = fine_query(result['key'])
        if result['key'] == '': raise Exception('Empty keyword.')
        return result

    elif 'suggest'==func_name:
        result['key'] = rfunc.get('key','')
        result['size'] = rfunc.get('size',10)
        result['field'] = rfunc.get('field','')
        result['index'] = rfunc.get('index','')
        result['ver'] = rfunc.get('ver','').strip()

        result['key'] = fine_query(result['key'])
        if result['key'] == '': raise Exception('Empty keyword.')
        if result['index'] == '': raise Exception('Empty index.')
        if result['field'] == '':
            field_map = {"filminfo":"name", "filmmaker":"name", "userinfo":"username", "mediainfo":"title"}
            if field_map.has_key(result['index']):
                result['field'] = field_map[result['index']]
            else:
                raise Exception('Empty field.')
        return result

    print 'Unknow function name:', func_name
    return None

def fine_query(key):
    # type: 1, 汉字, 普通
    return (key.strip(), 1)

def do_es_search_query(body,es_client,_index,_type):
    result = es_client.search(index=_index,doc_type=_type,body=body)

    # print result

    fresult = {'ok':False, 'data':{}}
    if result.has_key('hits'):
        fresult['ok'], resp= True, {}
        resp['total'] = result['hits'].get('total',0)
        hits = result['hits'].get('hits',[])
        data = []

        if hits and isinstance(hits,list) > 0:
            for hit in hits:
                if hit.has_key('_source'):
                    _source = hit['_source']
                    highlight = hit.get('highlight',{})
                    if highlight and isinstance(highlight,dict):
                        for key,value in highlight.iteritems():
                            raw_key = key
                            if key.endswith('.py') or key.endswith(".jp"):
                                raw_key = key[:-3]

                            if (not isinstance(_source.get(raw_key,-1),int)) and (isinstance(value,list) or isinstance(value,[].__class__)):
                                value = value[0]
                            _source[raw_key+'.hl'] = value
                    data.append(_source)
        resp['data'] = data
        fresult['data'] = resp

    return fresult

def load_template(template_name, nv_list, v_list ):
    '''
    :param filename:    模板名,在下级目录
    :param nv_list:     名称和值同时匹配时,替代
    :param v_list:      名称匹配替代
    :return:            json 对象, 用于es query
    '''
    if not templates.has_key(template_name):
        try:
            with open('./template/' + template_name + '.txt', 'r') as file:
                str = file.read()
                str = str.decode('utf-8')
                # print str
                str = unicode(str)
                templates[template_name] = json.loads(str,encoding='utf-8')
        except Exception,e:
            print 'Load template error:', str(e),'|:',template_name
            return None
    return render_template(templates[template_name], nv_list,v_list)

def replace_dict(dict_a, nv_list, v_list):
    # if not (isinstance(dict_a,dict), isinstance(dict_a, list)):
    #     return
    nvkeys = nv_list.keys()
    vkeys = v_list.keys()
    if isinstance(dict_a, [].__class__) or isinstance(dict_a, list):
        for item in dict_a:
             if isinstance(item,dict) or isinstance(dict_a, list):
                replace_dict(item,nv_list, v_list) #自我调用实现无限遍历
    if isinstance(dict_a, dict):
        for key,value in dict_a.iteritems():
            if isinstance(value,dict):
                replace_dict(value,nv_list, v_list) #自我调用实现无限遍历
            elif isinstance(value, list):
                replace_dict(value,nv_list, v_list) #自我调用实现无限遍历
            else:
                # if value is str or value is unichr:
                #     print value
                #     value =  unicode(value, "utf-8")
                if str(value) in vkeys:
                    dict_a[key] = v_list[str(value)]
                    # print value,":",v_list[str(value)]
                if (key+str(value)) in nvkeys:
                    dict_a[key] = nv_list[key+str(value)]


def render_template(template, nv_list, v_list):
    '''
    :param template:    模板,json 对象
    :param nv_list:     名称和值同时匹配时,替代
    :param v_list:      名称匹配替代
    :return:            json 对象, 用于es query
    '''
    t = copy.deepcopy(template)
    replace_dict(t,nv_list,v_list)
    return t

def add_highlight(q, highlight,fields,color='e61616'):
    # print 'hilight=',highlight
    if highlight == 1:
        # print 'jump in highlight',highlight
        f = {}
        for field in fields:
            f[field] = {}
        hl = {"pre_tags":'<font color="#'+color+'">',"post_tags":'</font>',"fields":f}
        q['highlight'] = hl

def modify_field(q,path,value):
    tmp = q
    plist = path.split('.')
    for p in plist[:-1]:
        tmp = tmp[p]
    tmp[plist[-1]] = value

def api_response(result):
    resp = flask.Response(result)
    resp.headers['content-type'] = 'application/json; charset=utf-8'
    return resp


#############################################################
# flask functions, routers
app = Flask(__name__)


@app.route("/", methods=['GET', 'POST'])
def index():
    return api_response(jsonify({'suc':1, 'msg':'search api online', 'setting':setting}))


@app.route("/parrel_test", methods=['GET', 'POST'])
def parrel_test():
    time.sleep(0.1)
    return api_response(jsonify({'suc':1, 'msg':'sleep 10s'}))



@app.route("/search/userinfo", methods=['GET','POST'])
def search_userinfo():
    try:
        p = check_params(request, 'search_userinfo')

        mbody = load_template('search_userinfo', v_list= {"来日方长":p["key"][0]}           #注意要用 p["key"][0]
                      , nv_list={"size100":p["size"], "from0":p["from"]})
        add_highlight(mbody,p["highlight"],["username"])
        modify_field(mbody,'_source',["id", "username","username.py", "rank"])

        # print json.dumps(mbody,ensure_ascii=False)
        result =  do_es_search_query(mbody,
                Elasticsearch(hosts=setting['es_hosts']),
                'nfsearch'+p['ver'],
                'userinfo')
        return api_response(json.dumps(result,ensure_ascii=False))
    except Exception,e:
        print 'search_userinfo error:', str(e)
        return api_response(json.dumps({'ok':False, 'reason':str(e)}))


@app.route("/search/mediainfo", methods=['GET','POST'])
def search_mediainfo():
    try:
        p = check_params(request, 'search_mediainfo')

        mbody = load_template('search_mediainfo', v_list= {"忙里偷闲":p["key"][0]}           #注意要用 p["key"][0]
                      , nv_list={"size100":p["size"], "from0":p["from"]})
        add_highlight(mbody,p["highlight"],["title"])
        modify_field(mbody,'_source',["id", "media_type", "title", "rank"])
        if p['media_type'] >= 0:
            modify_field(mbody,'query.function_score.query.bool.filter',{"term":{"media_type":p['media_type']}})

        # print json.dumps(mbody,ensure_ascii=False)
        result =  do_es_search_query(mbody,
                Elasticsearch(hosts=setting['es_hosts']),
                'nfsearch'+p['ver'],
                'mediainfo')
        return api_response(json.dumps(result,ensure_ascii=False))
    except Exception,e:
        print 'search_mediainfo error:', str(e)
        return api_response(json.dumps({'ok':False, 'reason':str(e)}))

@app.route("/search/filminfo", methods=['GET','POST'])
def search_filminfo():
    try:
        p = check_params(request, 'search_filminfo')

        mbody = load_template('search_filminfo', v_list= {"剑心":p["key"][0]}           #注意要用 p["key"][0]
                      , nv_list={"size100":p["size"], "from0":p["from"]})
        add_highlight(mbody,p["highlight"],["name", "alias"])
        modify_field(mbody,'_source',["fid", "doubanid", "name", "alias", "status", "pingfen"])

        # 目前写死10
        # if p['status'] >= 0:
        #     modify_field(mbody,'query.function_score.query.bool.filter',{"term":{"status":p['status']}})

        # print json.dumps(mbody,ensure_ascii=False)
        result =  do_es_search_query(mbody,
                Elasticsearch(hosts=setting['es_hosts']),
                'filminfo'+p['ver'],
                'filminfo')
        return api_response(json.dumps(result,ensure_ascii=False))
    except Exception,e:
        print 'search_filminfo error:', str(e)
        return api_response(json.dumps({'ok':False, 'reason':str(e)}))


@app.route("/search/filmmaker", methods=['GET','POST'])
def search_filmmaker():
    try:
        p = check_params(request, 'search_filmmaker')

        mbody = load_template('search_filmmaker', v_list= {"杨恭":p["key"][0]}           #注意要用 p["key"][0]
                      , nv_list={"size100":p["size"], "from0":p["from"]})
        add_highlight(mbody,p["highlight"],["name","alias_cn","alias_en"])
        modify_field(mbody,'_source',["fmid", "name", "alias_en", "alias_cn", "occupation", "cover_image", "status", "pingfen"])

        # 目前写死10
        # if p['status'] >= 0:
        #     modify_field(mbody,'query.function_score.query.bool.filter',{"term":{"status":p['status']}})

        # print json.dumps(mbody,ensure_ascii=False)
        result =  do_es_search_query(mbody,
                Elasticsearch(hosts=setting['es_hosts']),
                'filmmaker'+p['ver'],
                'filmmaker')
        return api_response(json.dumps(result,ensure_ascii=False))
    except Exception,e:
        print 'filmmaker error:', str(e)
        return api_response(json.dumps({'ok':False, 'reason':str(e)}))


@app.route("/suggest", methods=['GET','POST'])
def suggest():
    try:
        p = check_params(request, 'suggest')
        mbody = {
            "_source": [ p['field'] ],
            "suggest": {
                "auto_suggest": {
                    "prefix": p['key'][0],
                    "completion": {
                        "field": p['field']+".sg",
                        "size" : p['size']
                    }
                }
            }
        }
        es_client =  Elasticsearch(hosts=setting['es_hosts'])
        print mbody
        result = es_client.search(index=p['index']+p['ver'],doc_type=None,body=json.dumps(mbody,ensure_ascii=False))
        return api_response(json.dumps(result,ensure_ascii=False))
    except Exception,e:
        print 'suggest error:', str(e)
        return api_response(json.dumps({'ok':False, 'reason':str(e)}))




#############################################################
def test_load_template():
    v_list = {"契约":"变形金刚"}
    nv_list = {"size20":10}
    query = load_template('search_filminfo',nv_list,v_list)
    print json.dumps(query,ensure_ascii=False)



if __name__ == '__main__':
    #start up
    WSGIServer((setting['bind_ip'], setting['port']), app).serve_forever()

    # test_load_template()