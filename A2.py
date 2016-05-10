# -*- coding: utf-8 -*-
"""
Created on Thu May  5 12:44:53 2016

@author: xinruyue
"""

from pymongo import MongoClient


db = MongoClient('10.8.8.111:27017')['onions']
questionnaires = db['questionnaires']

def unpack(iterable):
    result = []
    for x in iterable:
        if hasattr(x, '__iter__'):
            result.extend(unpack(x))
        else:
            result.append(x)
    return result


tag = ["quitA","quitP"]

f = open("option.txt",'w+')

for each in tag:
    pipeline = [{"$match":{"tag":each}},
                {"$group":{"_id":"$tag","options":{"$push":"$body.options"}}}]
    qt_option = unpack(list(questionnaires.aggregate(pipeline))[0]['options'])
    
    sum_option = {}
    
    for i in qt_option:
        sum_option[i] =qt_option.count(i)
    print sum_option

        
        

