# Import necessary libraries
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import isodate
import datetime
import pymongo
import mysql.connector
import pandas as pd

# Define global MongoDB variables
mongo_client = pymongo.MongoClient('mongodb://localhost:27017')
mongo_db = mongo_client.youtube
mongo_collection = mongo_db["channel_data_collection"]

# Define global MySQL variables
mysql_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mysql",
    database="youtube"
)
mysql_cursor = mysql_connection.cursor()

class IntegrityErrorOccurred(Exception):
    pass

# Function to build the YouTube service
def build_youtube_service(api_key):
    return build('youtube', 'v3', developerKey=api_key)

# Function to retrieve channel details
def channel_details(youtube, channel_id):
    # Make a request to the YouTube API to get channel details
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    return {
        'Channel_Name': response['items'][0]['snippet']['title'],
        'Channel_Id': response['items'][0]['id'],
        'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
        'Channel_Views': response['items'][0]['statistics']['viewCount'],
        'Channel_Description': response['items'][0]['snippet']['description'],
        'Playlist_Id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'Videos': {}  # Placeholder for videos
    }

# Function to retrieve playlist details
def playlist_details(youtube, playlist_id,max_results = 1000):

    video_ids = []
    next_page_token = None

    # Continue fetching videos until reaching the desired number or no more videos
    while len(video_ids) < max_results:
        # Make a request to the YouTube API to get playlist details
        request = youtube.playlistItems().list(
            part="snippet,contentDetails,id",
            playlistId=playlist_id,
            maxResults=min(50, max_results - len(video_ids)),
            pageToken = next_page_token
        )
        response = request.execute()

        # Extract video_ids into a list
        video_ids.extend(item['snippet']['resourceId']['videoId'] for item in response.get('items', []))
        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break
    return video_ids[:max_results]

# Function to retrieve video details
def video_details(youtube, video_id):
    # Make a request to the YouTube API to get video details
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response = request.execute()

    return {
        'Video_Id': video_id,
        'Video_Name': response['items'][0]['snippet']['title'],
        'Video_Description': response['items'][0]['snippet']['description'],
        'Tags': response['items'][0]['snippet']['tags'] if 'tags' in response['items'][0]['snippet'] else [],
        'PublishedAt': datetime.datetime.strptime(response['items'][0]['snippet']['publishedAt'][:-1], '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') if 'publishedAt' in response['items'][0]['snippet'] else None,
        'View_Count': response['items'][0]['statistics']['viewCount'],
        'Like_Count': response['items'][0]['statistics']['likeCount'],
        'Dislike_Count': response['items'][0]['statistics']['dislikeCount'] if 'dislikeCount' in response['items'][0]['statistics'] else 0,
        'Favorite_Count': response['items'][0]['statistics']['favoriteCount'] if 'favoriteCount' in response['items'][0]['statistics'] else 0,
        'Comment_Count': response['items'][0]['statistics'].get('commentCount',0),
        'Duration': str(datetime.timedelta(seconds=isodate.parse_duration(response['items'][0]['contentDetails']['duration']).seconds)),
        'Thumbnail': response['items'][0]['snippet']['thumbnails']['default']['url'],
        'Caption_Status': response['items'][0]['contentDetails']['caption'] if response['items'][0]['contentDetails']['caption'] != 'false' else 'Not Available',
        'Comments': comment_details(youtube, video_id)  # Placeholder for comments
    }

# Function to retrieve comment details
def comment_details(youtube, video_id):
    try:
        # Make a request to the YouTube API to get comment details
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            textFormat="plainText",
            maxResults=100  # Add this line to limit the number of comments
        )
        response = request.execute()

        # Extract relevant information and create a dictionary
        comment_details_dict = {}
        for item in response.get('items', [])[:100]:  # Limit to the first 100 comments
            comment_id = item['snippet']['topLevelComment']['id']
            comment_details_dict[comment_id] = {
                'Comment_Id': comment_id,
                'Comment_Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                'Comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment_PublishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt']
            }

        return comment_details_dict
    except HttpError as e:
        # Check if comments are disabled
        if e.resp.status == 403 and "commentsDisabled" in str(e):
            return {}
        else:
            # Re-raise the exception if it's not related to comments being disabled
            raise e
        
# Function to execute SQL query and commit changes
def execute_sql_query(query, values=None):
    mysql_cursor.execute(query, values)
    mysql_connection.commit()

# Function to insert channel data into MySQL
def insert_channel(channel_data):
    try:
        # Define SQL query to insert channel data
        channel_query = '''
            INSERT INTO Channel (channel_id, channel_name, channel_views, channel_description, subscription_count)
            VALUES (%s, %s, %s, %s, %s)
        '''
        values = (
            channel_data['Channel_Id'],
            channel_data['Channel_Name'],
            int(channel_data['Channel_Views']),
            channel_data['Channel_Description'],
            int(channel_data['Subscription_Count']))
        
        # Execute SQL query and commit changes
        execute_sql_query(channel_query, values)

    except mysql.connector.errors.IntegrityError as e:
        # Handle integrity error (e.g., channel already exists)
        st.warning(f"Channel with ID {channel_data['Channel_Id']} already exists in the database. Skipping insertion.")
        raise IntegrityErrorOccurred("Integrity error occurred. Stopping execution.")

# Function to insert playlist data into MySQL  
def insert_playlist(playlist_data):
        
    # Define SQL query to insert playlist data
    playlist_query = '''
        INSERT INTO Playlist (playlist_id, channel_id)
        VALUES (%s, %s)
    '''
    values = (
        playlist_data['Playlist_Id'],
        playlist_data['Channel_Id'])

    # Execute SQL query and commit changes
    execute_sql_query(playlist_query, values)
  
# Function to insert video data into MySQL
def insert_video(video_data):
    
    # Define SQL query to insert video data
    video_query = '''
        INSERT INTO Video (video_id, playlist_id, video_name, video_description, video_published_date,
                            view_count, like_count, dislike_count, favorite_count, comment_count, duration,
                            thumbnail, caption_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    values = (
        video_data['Video_Id'],
        video_data['Playlist_Id'],
        video_data['Video_Name'],
        video_data['Video_Description'],
        str(datetime.datetime.strptime(video_data['PublishedAt'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')),
        int(video_data['View_Count']),
        int(video_data['Like_Count']),
        int(video_data['Dislike_Count']),
        int(video_data['Favorite_Count']),
        int(video_data['Comment_Count']),
        video_data['Duration'], 
        video_data['Thumbnail'],
        video_data['Caption_Status'])

    # Execute SQL query and commit changes
    execute_sql_query(video_query, values)
    
# Function to insert comment data into MySQL
def insert_comment(comment_data):
        
    # Define SQL query to insert comment data
    comment_query = '''
        INSERT INTO Comment (comment_id, video_id, comment_text, comment_author, comment_published_date)
        VALUES (%s, %s, %s, %s, %s)
    '''
    values = (
        comment_data['Comment_Id'],
        comment_data['Video_Id'],
        comment_data['Comment_Text'],
        comment_data['Comment_Author'],
        str(datetime.datetime.strptime(comment_data['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')))

    # Execute SQL query and commit changes
    execute_sql_query(comment_query, values)
        


def get_channel_data(youtube, channel_id):
    channel_list = [channel_id]
    channel_details_list = {}

    for channel_id in channel_list:
        channel_info = channel_details(youtube, channel_id)

    # Retrieve video IDs as a list
        video_ids = playlist_details(youtube, channel_info['Playlist_Id'])

        for video_id in video_ids:
            video_info = video_details(youtube, video_id)
            channel_info['Videos'][video_id] = video_info

        channel_details_list[channel_info['Channel_Name']] = channel_info
    return channel_details_list

# Function to retrieve channel data, insert into MongoDB
def insert_data_to_mongodb(channel_details_list):
    
    # Insert channel details into MongoDB
    mongo_collection.insert_one(channel_details_list)

# Function to display SQL query results in the Streamlit app
def display_query_results():

    # Dictionary mapping queries to SQL statements
    query_mapping = {
        "Names of all videos and their corresponding channels": "SELECT video_name, channel_name FROM Video JOIN Playlist ON Video.playlist_id = Playlist.playlist_id JOIN Channel ON Playlist.channel_id = Channel.channel_id",
        "Channels with the most number of videos": "SELECT channel_name, COUNT(*) AS video_count FROM Channel INNER JOIN Playlist ON Channel.channel_id = Playlist.channel_id INNER JOIN Video ON Playlist.playlist_id = Video.playlist_id GROUP BY channel_name ORDER BY video_count DESC LIMIT 5",
        "Top 10 most viewed videos": "SELECT video_name, channel_name, view_count FROM Video JOIN Playlist ON Video.playlist_id = Playlist.playlist_id JOIN Channel ON Playlist.channel_id = Channel.channel_id ORDER BY view_count DESC LIMIT 10",
        "Number of comments on each video": "SELECT Video.video_name, COUNT(*) AS comment_count FROM Video INNER JOIN Comment ON Video.video_id = Comment.video_id GROUP BY Video.video_name",
        "Videos with the highest number of likes": "SELECT video_name, channel_name, like_count FROM Video JOIN Playlist ON Video.playlist_id = Playlist.playlist_id JOIN Channel ON Playlist.channel_id = Channel.channel_id ORDER BY like_count DESC LIMIT 10",
        "Total likes and dislikes for each video": "SELECT Video.video_name, Video.like_count, Video.dislike_count FROM Video",
        "Total views for each channel": "SELECT Channel.channel_name, SUM(Video.view_count) AS total_views FROM Channel INNER JOIN Playlist ON Channel.channel_id = Playlist.channel_id INNER JOIN Video ON Playlist.playlist_id = Video.playlist_id GROUP BY Channel.channel_name",
        "Channels that published videos in 2022": "SELECT DISTINCT Channel.channel_name FROM Channel INNER JOIN Playlist ON Channel.channel_id = Playlist.channel_id INNER JOIN Video ON Playlist.playlist_id = Video.playlist_id WHERE YEAR(Video.video_published_date) = 2022",
        "Average duration of videos in each channel": "SELECT Channel.channel_name, AVG(Video.duration) AS average_duration FROM Channel INNER JOIN Playlist ON Channel.channel_id = Playlist.channel_id INNER JOIN Video ON Playlist.playlist_id = Video.playlist_id GROUP BY Channel.channel_name",
        "Videos with the highest number of comments": "SELECT video_name, comment_count FROM (SELECT V.video_name,COUNT(C.comment_id) as comment_count FROM Video V JOIN Comment C ON V.video_id = C.video_id GROUP BY V.video_id) AS comment_counts ORDER BY comment_count DESC LIMIT 10",
    }

    # Dropdown for selecting queries
    selected_query = st.selectbox("Select a query:", list(query_mapping.keys()))

    # Get the corresponding query
    query = query_mapping.get(selected_query, "")

    if not query:
        st.warning("Invalid query selected.")
        return

    # Execute the selected query
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchall()

    # Display the result as a table
    if result:
        df = pd.DataFrame(result,columns=[desc[0] for desc in mysql_cursor.description])
        st.subheader("Query Result:")
        st.table(df)
    else:
        st.info("No data available for the selected query.")

# Function to get channel names from MongoDB
def get_mongodb_channel_names():
    # Get the cursor for all documents in MongoDB
    cursor = mongo_collection.find()

    # Initialize an empty list to store channel names
    channel_names = []

    # Iterate through each document and extract channel names
    for document in cursor:
        if document:
            # Exclude "_id" field if present
            channel_names.extend([channel_name for channel_name in document.keys() if channel_name != "_id"])

    return channel_names
    
# Function to close database connections
def close_database_connections():
    mysql_cursor.close()
    mysql_connection.close()
    mongo_client.close()

# Streamlit app
def main():
    st.title("YouTube Data Harvesting and Warehousing App")

    api_key = 'Enter your Youtube API Key here!!!'

    # Get channel ID from user input
    channel_id = st.text_input("Enter YouTube Channel ID:")

    # Button to start data harvesting and save to MongoDB
    if st.button("Harvest Data and Save to MongoDB"):
        with st.spinner('Harvesting Data...'):
            # Build YouTube service
            youtube = build_youtube_service(api_key)
            result = get_channel_data(youtube, channel_id)

            insert_data_to_mongodb(result)

        st.success("Data harvesting and storage to MongoDB completed successfully!")

    # Dropdown to select channels from MongoDB for moving to MySQL
    selected_channel = st.selectbox("Select Channel to Move to MySQL", get_mongodb_channel_names())

    # Button to move data from MongoDB to MySQL for the selected channel
    if st.button("Move Channel from MongoDB to MySQL"):
        with st.spinner('Moving Data...'):

            # Move selected data from MongoDB to MySQL
            channel_data = mongo_collection.find_one({selected_channel: {"$exists": True}})
            if channel_data:
                # Insert channel data into MySQL
                insert_channel(channel_data[selected_channel])

                # Insert playlist data into MySQL
                insert_playlist(channel_data[selected_channel])

                playlist_id = channel_data[selected_channel]['Playlist_Id']

                # Iterate over videos in the channel
                for video_id, video_data in channel_data[selected_channel]['Videos'].items():
                    video_data['Playlist_Id'] = playlist_id

                    # Insert video data into MySQL
                    insert_video(video_data)

                    # Iterate over comments in the video
                    for comment_id, comment_data in video_data['Comments'].items():
                        comment_data['Video_Id'] = video_id

                        # Insert comment data into MySQL
                        insert_comment(comment_data)

                
        st.success(f"Selected channel '{selected_channel}' moved from MongoDB to MySQL successfully!")

    st.write('---')

    st.subheader('SQL Queries: ')
    display_query_results()

    # Closing MongoDB and MySQL connection
    close_database_connections()

# Run the Streamlit app
if __name__ == "__main__":
    main()
