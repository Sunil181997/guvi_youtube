#!/usr/bin/env python
# coding: utf-8

# In[2]:


import streamlit as st
from googleapiclient.discovery import build
import pandas as pd
from streamlit_option_menu import option_menu
import mysql
import pymongo
import pandas as pd
import mysql.connector
from pymongo import MongoClient
mydb = mysql.connector.connect(host='127.0.0.1', user='root', password="Sunil@1997", database='project1', use_pure=True)
my_cursor=mydb.cursor()
client = MongoClient("mongodb://localhost:27017")
db=client['youtube']
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """this is guvi project"""})
with st.sidebar:
    selected = option_menu(None, ["Home","Scrap_and_Dump","Data_Analysis"], 
                           icons=["house-door-fill","tools","card-text"]
                           )
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
        maxResults=5
        )
        response = request.execute()
        i=0
        vid={ }
        for c in response['items']:
            vid[str(i+1)]=response["items"][i]['contentDetails']['videoId']
            i=i+1
        return(vid)
def channel_videos(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=5,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids
def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        if i>100:
            break
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats
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
      c_details=[ ]
      request = youtube.commentThreads().list(
         part='snippet,id',
         videoId=v_id,
         maxResults=50)
      response = request.execute()
      for comment in response['items']:
                          c_details.append(c_det(comment))
      return(c_details)
def c_det(comment):
  c={ }
  c['Comment_Text']=comment['snippet']['topLevelComment']['snippet']['textDisplay']
  c['Comment_Author']=comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
  c['Comment_PublishedAt']=comment['snippet']['topLevelComment']['snippet']['publishedAt']
  c['Comment_id'] = comment['id']
  c['Video_id'] = comment['snippet']['videoId']
  c['Like_count']= comment['snippet']['topLevelComment']['snippet']['likeCount']
  return(c)
def comments(v_ids):
  com_d = []
  j=0
  for i in v_ids:
        com_d+= comment_details(i)
        j=j+1
        if(j==10):
          break
  return com_d
                
def channel_db(channel_id):
    response1=channel_info(channel_id)
    channeldetail=[ ]
    channeldetails={'channelname':response1['items'][0]['snippet']['title'],
                'channeldescription':response1['items'][0]['snippet']['description'],
                'totalsubscribers':response1['items'][0]['statistics']['subscriberCount'],
                'totalvideoCount':response1['items'][0]['statistics']['videoCount'],
                'totalviewCount':response1['items'][0]['statistics']['viewCount']
                 }
    channeldetail.append(channeldetails)
    return(channeldetail)
def insert_into_channels(user_inp):
                collections = db.channel_details
                query = """INSERT INTO channel_details VALUES(%s,%s,%s,%s,%s)"""
                for i in  collections.find({"channelname" : user_inp},{'_id':0}):
                    viewcount = str(i['totalviewCount'])
                    channel_name = str(i['channelname'])
                    channel_description = str(i['channeldescription'])
                    subscribers = str(i['totalsubscribers'])
                    videos = str(i['totalvideoCount'])
                    my_cursor.execute(query, (channel_name, channel_description, subscribers, videos,viewcount))
                    mydb.commit()              
def insert_into_videos(user_inp):
            collections = db.video_details
            query1 = """INSERT INTO Video_details VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collections.find({"Channel_name" : user_inp},{'_id':0}):
                a1=str(i['Channel_name'])
                a2=str(i['Channel_id'])
                a3=str(i['Video_id'])
                a4=str(i['Title'])
                a5=str(i['Tags'])
                a6=str(i['Thumbnail'])
                a7=str(i['Description'])
                a8=str(i['Published_date'])
                a9=str(i['Duration'])
                a10=str(i['Views'])
                a11=str(i['Likes'])
                a12=str(i['Comments'])
                a13=str(i['Favorite_count'])
                a14=str(i['Definition'])
                a15=str(i['Caption_status'])
                my_cursor.execute(query1,(a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15))
                mydb.commit()

def insert_into_comments(user_inp):
            collections1 = db.video_details
            collections2 = db.comments_details
            query= """INSERT INTO comment_details VALUES(%s,%s,%s,%s,%s,%s)"""
            for vid in collections1.find({"Channel_name" : user_inp},{'_id' : 0}):
                for i in collections2.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    a1=str(i['Comment_Text'])
                    a2=str(i['Comment_Author'])
                    a3=str(i['Comment_PublishedAt'])
                    a4=str(i['Comment_id'])
                    a5=str(i['Video_id'])
                    a6=str(i['Like_count'])
                    my_cursor.execute(query,(a1,a2,a3,a4,a5,a6))
                    mydb.commit()
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['channelname'])
    return ch_name
if selected == "Home":
    st.write("[project-Domain] : Social Media")
    st.write("[Toolsused] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    st.write("[About] : guvi project of Retrieving the Youtube channels data from the youtube and storing it in a MongoDB as data lake and into a SQL databas then querying the data and displaying it in the Streamlit")
if selected == "Scrap_and_Dump":
    tab1,tab2 = st.tabs(["### :blue[Scrap]", "### :blue[Dump_to_sql]"])
    with tab1:
        st.write("### Enter Channel_ID :")
        ch_id = st.text_input("channel id of youtube")

        if ch_id and st.button("Extract Data"):
            ch_details =channel_db(ch_id)
            st.write('#### Extracted data from channel')
            st.table(ch_details)

        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = channel_db(ch_id)
                data_c1=db['channel_details']
                data_c1.insert_many(ch_details)
                v_ids = channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                data_c1=db['video_details']
                data_c1.insert_many(vid_details)
                comm_details = comments(v_ids)
                data_c1=db['comments_details']
                data_c1.insert_many(comm_details)
                st.success("Upload to MogoDB successful:)")

    with tab2: 
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel",options= ch_names)
        if st.button("Submit"):
                insert_into_channels(user_inp)
                insert_into_videos(user_inp)
                insert_into_comments(user_inp)
                st.success("Transformation to MySQL Successful!!!")
if selected == "Data_Analysis":
    st.write("## :orange[choose the query]")
    questions = st.selectbox('Questions',
    ['Choose the query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2023?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        my_cursor.execute("""SELECT Channel_name, Title FROM project1.video_details""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        my_cursor.execute("""SELECT Channelname, totalvideocount FROM channel_details ORDER BY totalvideocount DESC""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        my_cursor.execute("""SELECT Channel_name,Title,Video_id,views FROM video_details ORDER BY views DESC LIMIT 10""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        my_cursor.execute("""SELECT Title,comments FROM video_details ORDER BY comments DESC""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        my_cursor.execute("""SELECT Channel_name,likes FROM video_details ORDER BY likes DESC LIMIT 10""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        my_cursor.execute("""SELECT Title,likes FROM video_details ORDER BY likes DESC""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        my_cursor.execute("""SELECT Channelname, totalviewcount FROM channel_details ORDER BY totalviewcount DESC""")
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif questions == '8. What are the names of all the channels that have published videos in the year 2023?':
        my_cursor.execute("""SELECT channel_name 
                            FROM video_details
                            WHERE Published_date LIKE '2023%'
                            GROUP BY channel_name
                            """)
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)     
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        my_cursor.execute("""SELECT channel_name, AVG(Duration) 
                            FROM video_details GROUP BY channel_name
                            """)
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        my_cursor.execute("""SELECT channel_name,comments
                            FROM video_details ORDER BY comments DESC LIMIT 10
                            """)
        df = pd.DataFrame(my_cursor.fetchall(),columns=my_cursor.column_names)
        st.write(df)


# In[ ]:




