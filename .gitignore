# -*- coding: utf-8 -*-
"""
Created on Fri May  6 11:01:25 2016

@author: xinruyue
"""
from pymongo import MongoClient

db = MongoClient("10.8.8.111:27017")["eventsV35"]
events = db['eventV35']

event_db = MongoClient("10.8.8.111:27017")["ronfedb"]
video_events = event_db['n1a']

def video_ids(eventKey,num):
    pipeline = [
    {"$match":{"eventKey":eventKey}},
    {"$group": {"_id":None, "videoId": {"$push": "$eventValue.videoId"}}}]
    
    data = list(events.aggregate(pipeline))[0]
    videoId = data["videoId"]
    
    sum_video = {}
    for i in videoId:
        sum_video[i] = videoId.count(i)
    
    sortSV = sorted(sum_video.iteritems(), key=lambda d:d[1], reverse = True)
    videoIds = []
    for each in sortSV:
        if sortSV.index(each) < num:
            videoIds.append(each[0])
    return(videoIds)


EA_lists = ["enterVideo","startVideo",["finishVideo","clickVideoExit"],"enterTAVideoExitPoll",
           "clickTAVEExit"]
EP_lists = ["enterVideo","startVideo",["finishVideo","clickVideoExit"],"enterTPVideoExitPoll",
           "clickTPVEExit"]

#get video_ids
videoA_ids = video_ids("enterTAVideoExitPoll",10)
videoP_ids = video_ids("enterTPVideoExitPoll",5)



def group_users1(eventKey,input_var,step_users,device_mark):
    if device_mark:
        pipeline = [
        {"$match": {"eventKey": eventKey, "eventValue.videoId":{"$in": input_var['video_ids']},
        "device":{"$in":step_users},"initVideo":True}},
        {"$group": {"_id": None, "users": {"$addToSet": "$device"}}}]    
        if step_users == None:
            del pipeline[0]["$match"]["device"]
    else: 
        pipeline = [
        {"$match": {"eventKey": eventKey, "eventValue.videoId":{"$in": input_var['video_ids']},
        "user":{"$in":step_users},"initVideo":True}},
        {"$group": {"_id": None, "users": {"$addToSet": "$user"}}}]    
        if step_users == None:
            del pipeline[0]["$match"]["user"]
            
    full_users = list(video_events.aggregate(pipeline))
    if len(full_users) > 0:
        full_users=full_users[0]['users']
        return(full_users)
    else:
        return []

def group_users2(eventKey,input_var,step_users,device_mark):
    if device_mark:
        pipeline = [
        {"$match": {"eventKey": eventKey, "eventValue.videoId":{"$in": input_var['video_ids']},"device":{"$in":step_users},}},
        {"$group": {"_id": None, "users": {"$addToSet": "$device"}}}]    
    else: 
        pipeline = [
        {"$match": {"eventKey": eventKey, "eventValue.videoId":{"$in": input_var['video_ids']},"user":{"$in":step_users},}},
        {"$group": {"_id": None, "users": {"$addToSet": "$user"}}}]    
            
    full_users = list(events.aggregate(pipeline))
    
    if len(full_users) > 0:
        full_users=full_users[0]['users']
        return(full_users)
    else:
        return []

def caculate_data(input_var,device_mark):
    #第一个漏斗，进入视频
    first_step = input_var["event_lists"][0]
    full_users = group_users1(first_step,input_var,None,device_mark)     
    result = {first_step:len(full_users)}
    print result
    #其他漏斗
    for each_step in input_var['event_lists'][1:3]:
        if type(each_step) == list:
            for each in each_step:
                each_step = each
                this_step_users = group_users1(each_step,input_var,full_users,device_mark)           
                result[each_step]= len(this_step_users)  
                if each_step[-1]:
                    full_users = this_step_users 
                print result
        else:
            this_step_users = group_users1(each_step,input_var,full_users,device_mark)           
            result[each_step]= len(this_step_users)           
            full_users = this_step_users
            print result
    for each_step in input_var['event_lists'][3:]:
        this_step_users = group_users2(each_step,input_var,full_users,device_mark)           
        result[each_step]= len(this_step_users)
        full_users = this_step_users
    print result
    return(result)

def integrate_data(event_lists,videoX_ids):
    input_var = {"event_lists":event_lists,"video_ids":videoX_ids}
    caculate_data(input_var,device_mark)


device_mark = True   
result_A = integrate_data(EA_lists,videoA_ids)
print result_A
device_mark = False
result_P = integrate_data(EP_lists,videoP_ids)
print result_P
    
