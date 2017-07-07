# -*- coding:utf8 -*-

__author__ = 'Mx'

import json
import elasticsearch
import datetime
import pymongo
import urllib,urllib2
from multiprocessing import Process
import os
import multiprocessing
import os, time, random

from multiprocessing.dummy import Pool as ThreadPool
import urllib
from urllib import quote

def filminfo_search(keyword = '变形金刚2'):
    uri = '/filminfo/_search'

    body = {
    "query": {
            "function_score": {
                "query": {

                   "bool": {
                     "must":[
                        {"multi_match": {
                            "query": keyword,
                            "fields":["name","alias"],
                            "minimum_should_match": "70%"
                        }}
                     ],
                     "should": [
                        {"multi_match": {
                            "query": keyword,
                            "fields":["name.shingles","alias.shingles"],
                            "type": "best_fields",
                            "tie_breaker":0
                        }}
                     ],
                     "filter": {
                        "term": { "status": 10}
                     }
                  }
                }
                ,"functions":[
                    {
                        "exp": {
                            "pubdate_m": {
                                  "origin": "now",
                                  "scale": "250d",
                                  "offset": "100d",
                                  "decay" : 0.5
                            }
                        }
                        ,"weight": 0.2
                    }
                    ,{
                        "field_value_factor": {
                            "field": "pingfen",
                            "factor": 0.2,
                            "modifier": "log1p",
                            "missing": 4.0
                        }
                        ,"weight": 1
                    }
                ]
                , "score_mode": "sum"

            }
        },
        "size":20,
        "from":0,
        "_source": ["name", "alias", "name.py", "pingfen","pubdate_m"]
    }

    settings = {
        # 'host':'106.15.137.205'
        'host':'172.17.6.150'
        ,'port':'9201'
        ,'header':{'Authorization':'Basic a2liYW5hOmVzbmZfbXhANTE1'}
    }

    url = 'http://' + settings['host'] + ":" + settings['port'] + uri
    mrequest = urllib2.Request(url=url, headers=settings['header'], data=json.dumps(body))
    response = urllib2.urlopen(mrequest)
    result = response.read()
    return result

def filminfo_search2(keyword = '变形金刚2'):


    settings = {
        'host':'172.17.6.150'
        ,'port':'51219'
    }

    url = 'http://' + settings['host'] + ":" + settings['port'] + '/search/filminfo?ver=_v7'+'&key='+quote(keyword.strip())
    # print url
    try:
        mrequest = urllib2.Request(url=url)
        response = urllib2.urlopen(mrequest)
        result = response.read()
        # print len(result)
        return result
    except Exception, e:
        print 'error:', str(e),'|',url
        return ''


def filminfo_search3(keyword = '变形金刚2'):
    settings = {
        # 'host':'127.0.0.1'
        'host':'172.17.6.150'
        ,'port':'51219'
    }

    url = 'http://' + settings['host'] + ":" + settings['port'] + '/parrel_test'
    # print url
    try:
        mrequest = urllib2.Request(url=url)
        response = urllib2.urlopen(mrequest)
        result = response.read()
        # print len(result)
        return result
    except Exception, e:
        print 'error:', str(e),'|',url
        return ''






def loaddict(num = 100000):
    dic_list = []
    with open('film.dic','r') as file:
        for i in xrange(num):
            dic_list.append(file.readline())
    return dic_list


def serial_query_parrel(words,t_num, wait=0.0):
    total = 0
    start = time.time()

    for word in words:
        result = filminfo_search3(word)
        # print str.strip(word),":",len(result)
        total += 1
        if(total % 100 == 0): print 'thread[',t_num,']avg speed:',(time.time() - start )/ total
        time.sleep(wait)


def serial_query_es(words,t_num, wait=0.0):
    total = 0
    start = time.time()

    for word in words:
        result = filminfo_search(word)
        # print str.strip(word),":",len(result)
        total += 1
        if(total % 100 == 0): print 'thread[',t_num,']avg speed:',(time.time() - start )/ total
        time.sleep(wait)

def serial_query_api(words,t_num, wait=0.0):
    total = 0
    start = time.time()

    for word in words:
        result = filminfo_search2(word)
        # print str.strip(word),":",len(result)
        total += 1
        if(total % 100 == 0): print 'thread[',t_num,']avg speed:',(time.time() - start )/ total
        time.sleep(wait)



def stress_test(thread=1,wait=0):
    pool = multiprocessing.Pool(processes = thread)
    dict_list = loaddict()


    piece_len = int(len(dict_list)/thread)
    print piece_len

    for i in xrange(thread):
        msg = "hello %d" %(i)
        pool.apply_async(serial_query, args = (dict_list[i*piece_len:(i+1)*piece_len],i,))   #维持执行的进程总数为processes，当一个进程执行完毕后会添加新的进程进去

    print "Mark~ Mark~ Mark~~~~~~~~~~~~~~~~~~~~~~"
    pool.close()
    pool.join()   #调用join之前，先调用close函数，否则会出错。执行完close后不会有新的进程加入到pool,join函数等待所有子进程结束
    print "Sub-process(es) done."
    #
    #
    # # for i in range(thread):
    # #     p.map(serial_query,dict_list[i*piece_len:(i+1)*piece_len],i)
    # p.map(serial_query,dict_list,0)
    #     # p.apply_async(serial_query, args=(dict_list[i*piece_len:(i+1)*piece_len],i))
    # print('Waiting for all subprocesses done...')
    # p.close()
    # p.join()

def parrel(func, thread = 20):
    # pool = multiprocessing.Pool(processes = thread)
    dict_list = loaddict()


    piece_len = int(len(dict_list)/thread)
    print piece_len

    for i in xrange(thread):
        p = multiprocessing.Process(target = func, args = (dict_list[i*piece_len:(i+1)*piece_len],i))
        p.start()
        # print "p.pid:", p.pid
        # print "p.name:", p.name
        # print "p.is_alive:", p.is_alive()

    print("The number of CPU is:" + str(multiprocessing.cpu_count()))
    for p in multiprocessing.active_children():
        print("child   p.name:" + p.name + "\tp.id" + str(p.pid))
    print "END!!!!!!!!!!!!!!!!!"

if __name__ == '__main__':
    #中文注释
    print 'ok'

    # print len(loaddict())
    # print filminfo_search("BBC时间")

    # serial_query(loaddict(),0)

    # stress_test(1)
    # parrel(serial_query_es,20)
    parrel(serial_query_api,20)
    # parrel(serial_query_parrel,5)