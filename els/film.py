# -*- coding:utf8 -*-

__author__ = 'Mx'

import json
import elasticsearch
import datetime
import pymongo
import utils
from bson.objectid import ObjectId

class Film_ELS():

    def __init__(self):
        self.local_env = {

        }
        self.env = {
            'active':'local',

            'local':{
                'els_host' : '172.17.6.150:9201',
                'mongo_port' : 27017,
                'mongo_host' : '172.17.6.150',
                'mongo_db' : 'filminfo',
                'mongo_collection' : 'filminfo',


                'index_batch_size' : 20,
            },
            'remote':{

            }
        }
        print 'done'


    def _get_mongo_film_connection(self, ss):
        # import sys
        # reload(sys)
        # sys.setdefaultencoding('utf8')

        pymongo.MongoClient()
        client = pymongo.MongoClient(ss['mongo_host'],ss['mongo_port'])
        db = client[ss['mongo_db']]
        collection = db[ss['mongo_collection']]
        return collection

    def _format_mongo_film(self, doc):

        return doc

    def format_pubdate(self,pdate):
        try:
            if not(pdate < 20180101 and pdate >= 18000101):
                pdate = 18000101
            return datetime.datetime.strptime(str(pdate),"%Y%m%d")
        except:
            print 'error pubdate:', pdate
            pdate = 18000101
            return datetime.datetime.strptime(str(pdate),"%Y%m%d")

    def _add_mongo_pubdate_date(self):
        settings = self.env[self.env['active']]

        collection = self._get_mongo_film_connection(settings)
        # all = collection.find({'_id':ObjectId('580c6f63a826a1238f3fbe89')},{'_id':0})
        all = collection.find({})
        print all.count()

        # collection.find_and_modify()
        #
        i = 0
        for doc in all:
            # tdoc = self._format_mongo_film(doc)

            tdate = self.format_pubdate(doc.get('pubdate',18000101))
            # print doc['_id']
            # print tdate
            collection.find_and_modify({"_id":doc['_id']},{"$set":{"pubdate_date":tdate}},True)
            i+=1

            if i % 1000 == 0:
                print i
            # break










    def index_by_mongo(self):
        #TODO: not done
        settings = self.env[self.env['active']]

        data = self._get_mongo_data(settings)

        print json.dumps(data.next(),ensure_ascii=False,cls=utils.CJsonEncoder)


        pass

    def update_mongo(self, settings):
        collection = self._get_mongo_film_connection(settings)
        all = collection.find({},{'_id':0})
        batch_size = settings['index_batch_size']
        result, n = [], 0
        for doc in all:
            tdoc = self._format_mongo_film(doc)

            if n < batch_size:
                result.append(tdoc)
                n += 1
            else:
                yield result
                result, n = [], 0

        if n > 0 :
            yield result






    pass




if __name__ == '__main__':
    #中文注释
    print 'ok'
    # Film_ELS().index_by_mongo()
    # print Film_ELS().format_pubdate(19920102)
    print Film_ELS()._add_mongo_pubdate_date()