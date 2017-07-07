# -*- coding:utf8 -*-

__author__ = 'Mx'


import datetime
import json
import os
import re


class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            return json.JSONEncoder.default(self, obj)
    def dumps(self,obj):
        if isinstance(obj, datetime.datetime):
            return obj.strptime('%Y-%m-%d %H:%M:%S.%f')
        else:
            return json.dumps(obj,ensure_ascii=False)
        # return json.dumps(obj,ensure_ascii=False)


def get_path_files(path = '.', patten=None, type = 0 ):
    '''

    :param path: 路径
    :param type: 0：返回所有文件, 1-n:返回最多type层目录的所有文件, -1, 返回所有目录
    :return:
    '''
    result = []
    for root,dirs,files in os.walk(path):
        if patten: files = filter(lambda f: re.compile(patten).match(f) ,files)
        if type == 0: result.extend([os.path.join(root,fn) for fn in files])
        elif type > 0:
            if len(root.split('/')) < type+1 :  result.extend([os.path.join(root,fn) for fn in files])
        elif type == -1:
            # if '.' != root: result.append(root)
            result.extend([os.path.join(root,dir) for dir in dirs])
        else:
            raise Exception('get_path_files invalid param:{type}')
        # print root,dirs,files

    return result
