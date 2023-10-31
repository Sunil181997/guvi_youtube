#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install pymongo')
get_ipython().system('pip install --upgrade google-api-python-client')

import pymongo
import pandas as pd
from googleapiclient.discovery import build
from pprint import pprint


# In[41]:


get_ipython().system('pip install mysql-connector-python')
#conda install -c anaconda mysql-connector-python


# In[44]:


pip install ipython-sql


# In[46]:


import mysql


# In[48]:


#%sql mysql://root:Sunil@1997@127.0.0.1/project1
#%sql mysql+mysqlconnector://root:Sunil@1997@127.0.0.1/project1
#mydb=mysql.connector.connect(host='local host',user='root',password="Sunil@1997",database='project1',use_pure=True)
mydb = mysql.connector.connect(host='127.0.0.1', user='root', password="Sunil@1997", database='project1', use_pure=True)


# In[49]:


my_cursor=mydb.cursor()


# In[72]:


my_cursor.execute('CREATE TABLE youtube1(channelname VARCHAR(250),channeldescription TEXT,totalsubscribers INT,totalvideoCount INT,totalviewCount INT,playlistid TEXT)')


# In[22]:


from pymongo import MongoClient


# In[23]:


client = MongoClient("mongodb://localhost:27017")


# In[24]:


db=client['youtube']


# In[55]:


Api_key='AIzaSyBW-2vLZ5D6HuBSLWDmW3q-olg6vE-QMDU'
youtube = build('youtube', 'v3', developerKey=Api_key)
from requests.models import Response
def channel_info(channel_id):
      request = youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=channel_id)
      response = request.execute()
      return(response)
def playlist_info(channel_id):
        request = youtube.playlists().list(
        part="snippet,contentDetails,status",
        channelId=channel_id,
        maxResults=10)
        response = request.execute()
        playlist={}
        i=0
        for c in response['items']:
           value=response['items'][i]["id"]
           playlist[str(i+1)]=value
           i=i+1
        return(playlist)
def video_id(pl_id):
        request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=pl_id,
        maxResults=10
        )
        response = request.execute()
        i=0
        vid={ }
        for c in response['items']:
            vid[str(i+1)]=response["items"][i]['contentDetails']['videoId']
            i=i+1
        return(vid)
def total_chvid(c_id):
      playlistid=playlist_info(c_id)
      video_Id={ }
      for key,val in playlistid.items():
       video_Id[str(playlistid[key])]=video_id(playlistid[key])

      return(video_Id)
def vdetail(vid):
      request = youtube.videos().list(part='contentDetails,statistics,snippet',id='fqfMXXjgtbA')
      response = request.execute()
      vdetails={ }
      vdetails['video_title']=response['items'][0]['snippet']['title']
      vdetails['publishedAt']=response['items'][0]['snippet']['publishedAt']
      vdetails['thumbnails']=response['items'][0]['snippet']['thumbnails']
      vdetails['statistics']=response['items'][0]['statistics']
      vdetails['comment']=comment_details(vid)
      return(vdetails)
def total_videodetails(c_id):
       video_details={ }
       tv=total_chvid(c_id)
       for key,val in tv.items():
          for k,v in val.items():
             video_details[v]=vdetail(v)
       return(video_details)
def comment_details(v_id):
      c_details={ }
      request = youtube.commentThreads().list(
         part='snippet,id',
         videoId=v_id,
         maxResults=100)
      response = request.execute()
      i=0
      for comment in response['items']:
                          c_details[response['items'][i]['snippet']['topLevelComment']['id']]=c_det(comment)
                          i=i+1
      return(c_details)
def c_det(comment):
  c={ }
  c['Comment_Text']=comment['snippet']['topLevelComment']['snippet']['textDisplay']
  c['Comment_Author']=comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
  c['Comment_PublishedAt']=comment['snippet']['topLevelComment']['snippet']['publishedAt']
  return(c)
def M_db(channel_id):
    response1=channel_info(channel_id)
    channeldetails={'channelname':response1['items'][0]['snippet']['title'],
                'channeldescription':response1['items'][0]['snippet']['description'],
                'totalsubscribers':response1['items'][0]['statistics']['subscriberCount'],
                'totalvideoCount':response1['items'][0]['statistics']['videoCount'],
                'totalviewCount':response1['items'][0]['statistics']['viewCount'],
                'playlistid':playlist_info(channel_id),
                'totalchannelvideo':total_chvid(channel_id),
                'all_video_detaila':total_videodetails(channel_id)
                 }
    return(channeldetails)


# In[84]:


channel_id=input()
data=M_db(channel_id)
data_c1=db['channel1']
data_c1.insert_one(data)
value1=data['channelname']
value2=data['channeldescription']
value3=data['totalsubscribers']
value4=data['totalvideoCount']
value5=data['totalviewCount']
value6=data['channelname']
insert_data_sql = """
INSERT INTO youtube1 (channelname, channeldescription, totalsubscribers, totalvideoCount, totalviewCount, playlistid)
VALUES (%s, %s, %s, %s, %s, %s);
"""
my_cursor.execute(insert_data_sql, (value1, value2, value3, value4, value5,value6))
mydb.commit()


# In[ ]:




