#---------------------------------# channels #------------------------------------------------------------

# channel lists:

# 1)aparupa dey-->"UCj6TI5TV04I4AZ8seDWjceA"

# 2)vidya vox-->"UCr-gTfI7au9UaEjNCbnp_Nw"

# 3)parithabangal higlights-->"UCWCVYwLvaTq5Q90c4874X5g"

# 4)Learnz Development Hub-->"UCgFKbA8YKZG4ODfumtTf0HQ"

# 5) Fries with potate-->"UCB_9w_j7jqEOqfhKejM7b-g"

#-------------------------------# Importing Libraries #--------------------------------------------------

from googleapiclient.discovery import build  
import googleapiclient.errors
import mysql.connector 
import streamlit as st
import pandas as pd 
from streamlit_option_menu import option_menu
import time


#------------------------------------# API Connection #------------------------------------------------------

api_service_name = "youtube"
api_version = "v3"

api_key = 'AIzaSyD175QTIBk6LI4VygZt8BWPk9_4y-TRJtA'
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)




with st.sidebar:
    opt = option_menu("Menu",
                    ['Home','ADD','Tables','Q/A'])
            
            
if opt=="Home":
        st.title(''':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]''')
        st.write("#")
        st.title(''':blue[Domain]: Social Media''')
        st.title('''# :blue[Skills take away from this project]: python scripting,data collection,streamlit,API integration,data management using SQL''')
        st.title(":blue[overview]: buliding a simple ui with streamlit, retrieving data from youtube API, storing the data SQL as a WH, querying the data warehouse with SQL,and displaying the data in the streamlit app.")
        st.title(":blue[developed by]: Arunachalam") 


if opt == "ADD":
    st.write("### ENTER THE TABLE NAME")
    channel_id = st.text_input("Enter the Channel ID")
    
    if st.button("Enter") and channel_id:
        
        # Get channel data
        def get_channel_data(channel_id):
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id
            )
            response = request.execute()
            if 'items' in response:
                i = response['items'][0]  # Assuming a single item for a unique channel_id
                data = {
                    "channel_name": i['snippet']['title'],
                    "channel_id": i["id"],
                    "channel_description": i['snippet']['description'],
                    "channel_playlistid": i['contentDetails']['relatedPlaylists']['uploads'],
                    "channel_subscriberCount": i['statistics']['subscriberCount'],
                    "channel_videoCount": i['statistics']['videoCount'],
                    "channel_viewCount": i['statistics']['viewCount']
                }
                return data
            else:
                st.write("Channel not found.")
                return None

        # Retrieve channel information
        channel_info = get_channel_data(channel_id)
        
        if channel_info:
            # Convert channel info to DataFrame
            channel_df = pd.DataFrame([channel_info])
         

            # Get video IDs
            def get_video_ids(channel_playlistid):
                video_ids = []
                next_page_token = None
                while True:
                    request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId=channel_playlistid,
                        maxResults=50,
                        pageToken=next_page_token
                    )
                    response = request.execute()
                    video_ids.extend([item['contentDetails']['videoId'] for item in response['items']])
                    next_page_token = response.get('nextPageToken')
                    if not next_page_token:
                        break
                return video_ids

            video_ids = get_video_ids(channel_info['channel_playlistid'])

            # Get video details
            def get_video_details(video_ids):
                video_datas = []
                for video_id in video_ids:
                    request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=video_id
                    )
                    response = request.execute()
                    for i in response['items']:
                        video_data = {
                            "channel_name": i['snippet']['channelTitle'],
                            "channel_id": i['snippet']['channelId'],
                            "video_id": i['id'],
                            "video_name": i['snippet']['title'],
                            "video_description": i['snippet'].get('description'),
                            "tags": i['etag'],
                            "published_at": i['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''),
                            "views": i['statistics'].get('viewCount'),
                            "likes": i['statistics'].get('likeCount'),
                            "favorite_count": i['statistics'].get('favoriteCount'),
                            "comment_count": i['statistics'].get('commentCount'),
                            "duration": i['contentDetails']['duration'].replace('PT', ' '),
                            "thumbnail": i['snippet']['thumbnails'].get('default', {}).get('url'),
                            "caption_status": i['contentDetails'].get('caption')
                        }
                        video_datas.append(video_data)
                return video_datas

            # Convert video details to DataFrame
            video_df = pd.DataFrame(get_video_details(video_ids))
         

            # Get comment details
            def get_comment_details(video_ids):
                comment_datas = []
                for vid in video_ids:
                    try:
                        request = youtube.commentThreads().list(
                            part="snippet",
                            videoId=vid,
                            maxResults=50
                        )
                        response = request.execute()
                        for i in response['items']:
                            comment_data = {
                                "Channel_Id": i['snippet']['topLevelComment']['snippet']['channelId'],
                                "Comment_Id": i['snippet']['topLevelComment']['id'],
                                "Comment_Text": i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                "Comment_Author": i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                "Comment_PublishedAt": i['snippet']['topLevelComment']['snippet']['publishedAt'].replace('T', ' ').replace('Z', '')
                            }
                            comment_datas.append(comment_data)
                    except Exception as e:
                        st.write(f"Error retrieving comments for video {vid}: {e}")
                return comment_datas

            # Convert comment details to DataFrame
            comment_df = pd.DataFrame(get_comment_details(video_ids))
           




#---------------------------------------------/ sql part \ -----------------------------------------

        # Database Configuration
        config = {
            "user": "root", "password": "arun2311",
            "host": "127.0.0.1", "database": "youtube", "port": "3306"
        }

        # Insert Channel Details
        try:
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()

            create_query = """CREATE TABLE IF NOT EXISTS channels(
                                channel_name VARCHAR(150) PRIMARY KEY,
                                channel_id VARCHAR(50),
                                channel_description TEXT,
                                channel_playlistid VARCHAR(200),
                                channel_subscriberCount INT,
                                channel_videoCount INT,
                                channel_viewCount INT
                            )"""
            cursor.execute(create_query)
            connection.commit()

            for _, row in channel_df.iterrows():
                insert_query = '''INSERT IGNORE INTO channels(
                                    channel_name, channel_id, channel_description,
                                    channel_playlistid, channel_subscriberCount,
                                    channel_videoCount, channel_viewCount
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s)'''
                values = (
                    row['channel_name'],
                    row['channel_id'],
                    row['channel_description'],
                    row['channel_playlistid'],
                    row['channel_subscriberCount'],
                    row['channel_videoCount'],
                    row['channel_viewCount']
                )
                cursor.execute(insert_query, values)
                connection.commit()
            st.success("Channel details inserted successfully!")
        except mysql.connector.Error as e:
            st.error(f"Error inserting channel details: {e}")
        finally:
            cursor.close()
            connection.close()

        # Insert Video Details
        try:
            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()

            create_query = """CREATE TABLE IF NOT EXISTS videos(
                                channel_name VARCHAR(200),
                                channel_id VARCHAR(50),
                                video_id VARCHAR(200) PRIMARY KEY,
                                video_name VARCHAR(200),
                                video_description TEXT,
                                tags VARCHAR(100),
                                published_at VARCHAR(200),
                                views INT,
                                likes INT,
                                favorite_count INT,
                                comment_count INT,
                                duration VARCHAR(200),
                                thumbnail VARCHAR(150),
                                caption_status VARCHAR(200)
                            )"""
            cursor.execute(create_query)
            connection.commit()

            for _, row in video_df.iterrows():
                insert_query = '''INSERT IGNORE INTO videos(
                                    channel_name, channel_id, video_id, video_name,
                                    video_description, tags, published_at, views,
                                    likes, favorite_count, comment_count, duration,
                                    thumbnail, caption_status
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
                values = (
                    row['channel_name'],
                    row['channel_id'],
                    row['video_id'],
                    row['video_name'],
                    row['video_description'],
                    row['tags'],
                    row['published_at'],
                    row['views'],
                    row['likes'],
                    row['favorite_count'],
                    row['comment_count'],
                    row['duration'],
                    row['thumbnail'],
                    row['caption_status']
                )
                cursor.execute(insert_query, values)
                connection.commit()
            st.success("Video details inserted successfully!")
        except mysql.connector.Error as e:
            st.error(f"Error inserting video details: {e}")
        finally:
            cursor.close()
            connection.close()

        # Insert Comment Details
        if not comment_df.empty:
            try:
                connection = mysql.connector.connect(**config)
                cursor = connection.cursor()

                create_query = """CREATE TABLE IF NOT EXISTS comments(
                                    Channel_Id TEXT,
                                    Comment_Id VARCHAR(100),
                                    Comment_Text TEXT,
                                    Comment_Author VARCHAR(100),
                                    Comment_PublishedAt DATETIME
                                )"""
                cursor.execute(create_query)
                connection.commit()

                for _, row in comment_df.iterrows():
                    insert_query = '''INSERT IGNORE INTO comments(
                                        Channel_Id, Comment_Id, Comment_Text,
                                        Comment_Author, Comment_PublishedAt
                                    )
                                    VALUES (%s, %s, %s, %s, %s)'''
                    values = (
                        row['Channel_Id'],
                        row['Comment_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_PublishedAt']
                    )
                    cursor.execute(insert_query, values)
                    connection.commit()
                st.success("Comment details inserted successfully!")
            except mysql.connector.Error as e:
                st.error(f"Error inserting comment details: {e}")
            finally:
                cursor.close()
                connection.close()

#----------------------------------- / streamlit \ -----------------------------------

if opt == "Tables":

    col = st.columns(1)

    with col[0]:
        visibility = st.radio(
            "Set the table name 👉",
            options=["channels", "videos", "comments"],
            key="visibility"
        )
        st.write("### ENTER THE TABLE NAME")

    channel_id = st.text_input("Enter the Channel ID")

    if st.button("Enter"):
        st.write("")


    config = {
        "user": "root","password": "arun2311",
        "host": "127.0.0.1","database": "youtube"
    }

 
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()


    if visibility == "channels":
        if channel_id:
            query = "SELECT * FROM channels WHERE channel_id = %s"
            cursor.execute(query, (channel_id,))
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=['channel_name', 'channel_id', 'channel_description', 'channel_playlistid', 'channel_subscriberCount', 'channel_videoCount', 'channel_viewCount'])
            st.write(df)


    elif st.session_state.visibility == "videos":
        if channel_id:
            query = "SELECT * FROM videos WHERE channel_id = %s"
            cursor.execute(query, (channel_id,))
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=[
                'channel_name','channel_id','video_id','video_name','video_description','tags','published_at','views','likes','favorite_count','comment_count','duration','thumbnail','caption_status'])
            st.write(df)

    elif st.session_state.visibility == "comments":
        if channel_id:
            query = "SELECT * FROM comments WHERE Channel_Id = %s"
            cursor.execute(query, (channel_id,))
            df = pd.DataFrame(cursor.fetchall(), columns=[
                'Channel_Id','Comment_Id','Comment_Text','Comment_Author','Comment_PublishedAt'])
            st.write(df)


if opt ==("Q/A"):
        questions = st.selectbox("shoot your Question",
                             ["Choose your Questions...",
                              '1. What are the names of all the videos and their corresponding channels?',
                              '2. Which channels have the most number of videos, and how many videos do they have?',
                              '3. What are the top 10 most viewed videos and their respective channels?',
                              '4. How many comments were made on each video, and what are their corresponding video names?',
                              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8. What are the names of all the channels that have published videos in the year 2022?',
                              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10.Which videos have the highest number of comments, and what are their corresponding channel names?'])
        

        config = {
        "user": "root","password": "arun2311",
        "host": "127.0.0.1","database": "youtube"
         }

        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()



        if questions =='1. What are the names of all the videos and their corresponding channels?':
            cursor.execute('SELECT video_name as video_name , channel_name as channel_name  FROM youtube.videos')
            df = pd.DataFrame(cursor.fetchall(), columns=['video_name', 'channel_name'])
            st.write(df)


        elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            cursor.execute("select channel_name, channel_videoCount from channels order by channel_videoCount desc limit 1;")
            df = pd.DataFrame(cursor.fetchall(),columns=['channel_name', 'channel_videoCount'])
            st.write(df)


        elif questions =='3. What are the top 10 most viewed videos and their respective channels?':
            cursor.execute("select video_name,channel_name,views from videos order by views desc limit 10;")
            data = cursor.fetchall()
            df = pd.DataFrame(data,columns=['video_name','channel_name','views'])
            st.write(df)


        elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            cursor.execute("select channel_name , video_name,video_id, comment_count from youtube.videos;")
            df = pd.DataFrame(cursor.fetchall(), columns =["channel_name", "video_name", "video_id", "comment_count"])
            st.write(df)

        elif questions =='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            cursor.execute("select videos.video_name, videos.channel_name,videos.likes from youtube.videos where likes =(select max(likes) from videos);")
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=["video_name", "channel_name", "likes"])
            st.write(df)

        elif questions =='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            cursor.execute("select video_name as video_name, sum(likes) as likes from youtube.videos group by video_name;")
            df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])
            st.write(df)

        elif questions =='7. What is the total number of views for each channel, and what are their corresponding channel names?':
            cursor.execute("select channel_name , sum(views) as total_views from videos group by channel_name;")
            df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
            st.write(df)

        elif questions =='8. What are the names of all the channels that have published videos in the year 2022?':
            cursor.execute("select * from videos where Published_at > '2022-01-01 00:00:00'and Published_at < '2022-12-31 00:00:00';")
            df = pd.DataFrame(cursor.fetchall(),columns=['channel_name', 'channel_id', 'video_id', 'video_name', 'video_description', 'tags', 'published_at', 'views', 'likes', ',favorite_count', 'comment_count', 'duration', 'thumbnail', 'caption_status'])
            st.write(df)


        elif questions =='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            cursor.execute("select channel_name,avg(duration) as average_duration from videos group by channel_name;")
            df = pd.DataFrame(cursor.fetchall(),columns =cursor.column_names)
            st.write(df)

        elif questions =='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
            cursor.execute("select channel_name,video_name,comment_count from youtube.videos v where comment_count =(select max(comment_count) from youtube.videos where channel_name = videos.channel_name);")
            data = cursor.fetchall()
            df = pd.DataFrame(data,columns=['channel_name',"video_name","comment_count"])
            st.write(df)




