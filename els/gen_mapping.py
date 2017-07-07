
#!/usr/bin/python
# -*- coding: UTF-8 -*-
import json as json


__author__ = 'Mx'

pinyin_analyzer={
    "index" : {
        "analysis" : {
            "tokenizer" : {
                "py" : {
                    "lowercase" : True,
                    "type" : "pinyin",
                    "keep_original" : False,
                    "keep_separate_first_letter" : False,
                    "keep_full_pinyin" : True,
                    "keep_first_letter": False,
                    "limit_first_letter_length" : 10,
                },
                "jp" : {
                    "lowercase" : True,
                    "type" : "pinyin",
                    "keep_original" : False,
                    "keep_separate_first_letter" : False,
                    "keep_full_pinyin" : False,
                    "keep_first_letter": True,
                }
            },
            "analyzer" : {
                "py" : {
                    "tokenizer" : "py"
                },
                "jp" : {
                    "tokenizer" : "jp"
                }
            }
        }
    }
}

mapping_all = {
    "analyzer": "ik_max_word",
    "search_analyzer": "ik_max_word",
    "term_vector": "yes",
    "store": "yes"
}
#"term_vector": "with_positions_offsets",
field_mapping = {
    "type": "text",
    "analyzer": "ik_max_word",
    "search_analyzer": "ik_max_word",
    "boost": 1.2,
    "fields": {
        "py": {
            "type": "text",
            "term_vector": "with_offsets",
            "analyzer": "py",
            "boost": 1.0
        },
        "jp": {
            "type": "text",
            "term_vector": "with_offsets",
            "analyzer": "jp",
            "boost": 1.0
        }
    }
}
def gen_create(idx_name):
    print("""
# Create a index with custom analyzer
curl -XPUT http://localhost:9200/%s/ -d'%s' """ % (idx_name, json.dumps(pinyin_analyzer, indent=2)))
def gen_alias(idx_name, raw_name, idx_name_pre):
    alias_update = {
        "actions": [
            { "remove": { "index": idx_name_pre, "alias": raw_name}},
            { "add": { "index":idx_name, "alias": raw_name}}
        ]
    }
    print("""
# create a alias
curl -XPUT http://localhost:9200/%s/_alias/%s
# update alias
curl -XPOST http://localhost:9200/_aliases -d'%s'
curl "http://localhost:9200/*/_alias/%s"
""" % (idx_name, raw_name, json.dumps(alias_update, indent=2), raw_name))
def gen_delete(idx_name):
    print("""
# Delete a index
curl -XDELETE http://localhost:9200/%s""" % (idx_name))
def gen_mapping(idx_name, type_name, properties):
    body = {type_name : {
            "_all": mapping_all,
            "properties": properties,
    	}
    }
    print("""
# Create mapping
curl -XPOST http://localhost:9200/%s/%s/_mapping -d'%s'""" %(idx_name, type_name, json.dumps(body, indent=2)))
iver = 5
pre_ver = "v" + str(iver-1)
ver = "v" + str(iver)
def gen_nfsearch():
    raw_name = "nfsearch"
    idx_name = raw_name + "_" + ver
    idx_name_pre = raw_name + "_" + pre_ver
    print("################# nfsearch #################")
    gen_delete(idx_name)
    gen_create(idx_name)
    props = {
        "title": field_mapping
    }
    gen_mapping(idx_name, "mediainfo", props)
    props = {
        "username": field_mapping
    }
    gen_mapping(idx_name, "userinfo", props)
    gen_alias(idx_name, raw_name, idx_name_pre)
    print("\n")
def gen_filminfo():
    raw_name = "filminfo"
    idx_name = raw_name + "_" + ver
    idx_name_pre = raw_name + "_" + pre_ver
    print("################# filminfo #################")
    gen_delete(idx_name)
    gen_create(idx_name)
    props = {
        "name": field_mapping,
        "alias": field_mapping,
        "director": field_mapping,
        "actor": field_mapping,
    }
    gen_mapping(idx_name, "filminfo", props)
    gen_alias(idx_name, raw_name, idx_name_pre)
    print("\n")
def gen_all():
    gen_nfsearch()
    gen_filminfo()
    print("""
# cat all indexes
curl 'http://localhost:9200/_cat/indices?v'
""")
gen_all()