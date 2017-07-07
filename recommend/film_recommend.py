# -*- coding: utf-8 -*-
__author__ = 'Mx'

import pymongo
import MySQLdb
import datetime
import json
from elasticsearch import Elasticsearch


'''
    推荐的测试类,目前无接口输出
'''

class FilmSimilarity():
    def __init__(self, islocal=True):
        if islocal:
            self.settings = {
                'mongo_ip':'139.224.30.59',
                'mongo_port':27019,
                'mysql_ip':'139.224.30.59',
                'mysql_port':3300,
                'mysql_user':'apiread',
                'mysql_pwd':'70924d6fa4b2d745185fa4660703a5c0',
                'mysql_db':'yfapi',
                'target_mongo_ip':'172.17.6.150',
                'target_mongo_port':27017,
                'es_host_port':'172.17.6.150:9201',
            }
        else:
            pass
        pass

    def get_clients(self):
        settings = self.settings
        mongo_client = pymongo.MongoClient(settings['mongo_ip'],settings['mongo_port'])
        mysql_client = MySQLdb.connect(host=settings['mysql_ip'],port=settings['mysql_port'],user=settings['mysql_user']
                                       ,passwd=settings['mysql_pwd'],db=settings['mysql_db'],charset='utf8')
        store_client = pymongo.MongoClient(settings['target_mongo_ip'],settings['target_mongo_port'])
        return mongo_client, mysql_client, store_client

    def prepare_data(self):
        settings = self.settings
        #get all data into dna.film_all_dna
        mongo_client, mysql_client, store_client = self.get_clients()

        collection_dna = mongo_client['dna']['filmdna']
        collection_store = store_client['dna']['film_all_dna']

        all_dna = collection_dna.find({})
        # print self.get_filminfo(136056,client)
        # print self.get_filmstatus(12307,mysql_client)

        for item in all_dna:
            fid = item['film_id']
            item_for_insert = self.get_filmdna_by_id(fid,mongo_client,mysql_client,item)
            if item_for_insert:
                collection_store.insert_one(item_for_insert)
                print 'insert:', item['film_id']

    def get_filmdna_by_id(self,fid,mongo_client,mysql_client, filmtype=None):
        filmtype = self.get_filmtype(fid,mongo_client)
        if filmtype is None:
            filmtype = self.get_filmtype(fid,mongo_client)
            if filmtype is None:
                print 'No filmtype:', fid
                return None
        filmstatus = self.get_filmstatus(fid,mysql_client)
        if filmstatus is None:
            print 'No filmstatus:', fid
            return None
        filminfo = self.get_filminfo(fid,mongo_client)
        if filminfo is None:
            print 'No filminfo:', fid
            return None

        item_for_insert = self.assemble_data(filmtype,filminfo,filmstatus)
        return item_for_insert

    def es_search_similar_film_by_feature(self, all_film_dna, size=10, minimum_match=1):
        print all_film_dna
        es_client =  Elasticsearch(hosts=self.settings['es_host_port'])

        vec_int_feature = ['story_place', 'main_style', 'award_information', 'stroy_source', 'vision_effect', 'theme_type',
                           'movie_ending', 'traditional_type', 'related_subject', 'story_time']  #10, factor:0.3
        vec_str_feature = ['area', 'type']  #2, factor:0.4
        vec_cnt_feature = ['film_article_cnt', 'wemedia_video_cnt', 'comment_cnt', 'film_list_cnt', 'play_cnt', 'favorite_cnt']  #6, factor:0.1

        # score_vec_int_feature, score_vec_str_feature, score_vec_cnt_feature = 0.3, 0.4, 0.1
        score_vec_int_feature, score_vec_str_feature, score_vec_cnt_feature = 1,1,1

        #fid 1,         不用做计算
        # pubdate_m 1
        #'rec_level' int 1,  魔方后台, factor:0.1
        #pingfen float 1,  10分满分, factor:0.05
        #'video_status'==1  filter, done

        features = []
        for f in vec_int_feature:
            vec = all_film_dna[f]
            for i in vec:
                features.append({
                    "bool" :
                    {
                        "boost": score_vec_int_feature,
                        "should":[ {"match": {f: i}} ]
                    }
                })

        for f in vec_str_feature:
            vec = all_film_dna[f]
            for i in vec:
                features.append({
                    "bool" :
                    {
                        "boost": score_vec_str_feature,
                        "should":[ {"term": {f: i}} ]
                    }
                })



        body = {
            "size": size,
            "_source": ["story_place","feats_effect"],
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "should": features
                            ,"filter": [{ "term":{"video_status":1} }]          #'video_status'==1  filter
                            ,"minimum_should_match": 1
                        }
                    }
                    ,"script_score": {
                        "script": {
                            "lang": "painless",
                            "inline": "_score"
                                      "+Math.log(doc['comment_cnt'].value + doc['play_cnt'].value + doc['favorite_cnt'].value)"
                                      "+0.05*doc['pingfen'].value"
                                      "+0.1*doc['rec_level'].value"
                        }

                    }
                }
            }
        }


        # print body
        result = es_client.search('dna',doc_type=None,body=json.dumps(body,ensure_ascii=False))

        return result

    # def similar_film_filter(self, all_film_dna):

    def es_search_similar_film_by_id(self, fid, mongo_client, mysql_client):
        dna = self.get_filmdna_by_id(fid,mongo_client,mysql_client)
        return self.es_search_similar_film_by_feature(dna)


    def assemble_data(self,filmtype,filminfo,filmstatus):
        try:
            result = {
                'fid' : filmtype['film_id'],
                'stroy_source' : filmtype.get('story_source',[]),
                'story_time' : filmtype.get('story_time',[]),
                'story_place' : filmtype.get('story_place',[]),
                'traditional_type' : filmtype.get('traditional_type',[]),
                'theme_type' : filmtype.get('theme_type',[]),
                'vision_effect' : filmtype.get('vision_effect',[]),
                'main_style' : filmtype.get('main_style',[]),
                'related_subject' : filmtype.get('related_subject',[]),
                'movie_ending' : filmtype.get('movie_ending',[]),
                'award_information' : filmtype.get('award_information',[]),

                'type' : filminfo.get('type',[]),
                'pubdate_m' : filminfo.get('pubdate_m',datetime.datetime.strptime('1800-01-01 00:00:00','%Y-%m-%d %H:%M:%S')),
                'area' : filminfo.get('area',[]),
                'rec_level' : filminfo.get('rec_level',0),
                'video_status' : filminfo.get('video_status',0),
                'pingfen' : filminfo.get('pingfen',0),

                'comment_cnt' : filmstatus[3],
                'favorite_cnt' : filmstatus[4],
                'play_cnt' : filmstatus[7],
                'film_article_cnt' : filmstatus[8],
                'film_list_cnt' : filmstatus[9],
                'wemedia_video_cnt' : filmstatus[10]

            }
            return result
        except Exception, e:
            print 'assemble_data error:',str(e),'|',filmtype['film_id']
            return None


    def get_filmtype(self,fid,client):
        return client['dna']['filmdna'].find_one({'film_id':fid})

    def get_filminfo(self,fid,client):
        return client['filminfo']['filminfo'].find_one({'fid':fid})

    def get_filmstatus(self,fid,client):
        cursor = client.cursor()
        cursor.execute('select * from film where film_id = '+ str(fid))
        if cursor.rowcount > 0:
            return cursor.fetchone()
        return None







def test():
    fs = FilmSimilarity()
    # fs.prepare_data()


    mongo,mysql,store = fs.get_clients()
    print json.dumps(fs.es_search_similar_film_by_id(186144,mongo,mysql),ensure_ascii=False)


if __name__ == '__main__':
    test()