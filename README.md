# Youtube_Data_Harvesting_and_Warehousing
## Overview

This Python application, built with Streamlit, harvests data from the YouTube API, stores it in MongoDB, and warehouses it in a MySQL database. The app retrieves channel details, playlists, videos, and comments, providing a comprehensive view of YouTube content.

![image](https://github.com/AkashKumar305/Youtube_Data_Harvesting_and_Warehousing/blob/main/Streamlit.png)


## Key Features

- **YouTube Data Harvesting:** Retrieve detailed information about channels, playlists, videos, and comments using the YouTube API.
- **Data Storage:** Store harvested data in both MongoDB and MySQL databases for flexibility and analytical capabilities.
- **Streamlit App:** User-friendly interface powered by Streamlit for data harvesting and SQL query execution.
- **Data Analysis:** Perform various SQL queries to analyze YouTube data, such as video views, likes, comments, and more.

## Getting Started

1. **API Key:** Obtain a YouTube API key and insert it in the python file.
2. **Channel ID:** Input the target YouTube channel ID in the app.
3. **Harvest Data:** Click the "Harvest Data and Save to MongoDB" button to initiate data retrieval and storage.
4. **Migrate Data:** Select a channel from the dropdown list and click the "Move Channel from MongoDB to MySQL" to migrate the data.
5. **SQL Queries:** Explore and execute predefined SQL queries using the Streamlit app.

## Workflow Execution

1. Get the Input such as Youtube API key and Channel ID from the user to harvest data using Streamlit app.
2. The Streamlit app utilizes the provided API key to build a connection to the YouTube API and retrieves the channel details such as video details, comment details etc.
3. The harvested data is inserted into a MongoDB collection ('channel_data_collection'). The MongoDB connection details are specified in the insert_data_to_mongodb function.
4. The data is migrated from the MongoDB to MySQL database. Data is inserted into four tables such as 'Channel', 'Playlist', 'Video', 'Comment'.
5. The Streamlit app provides a set of predefined SQL queries that users can execute to analyze the harvested data. These queries include retrieving video details, channel statistics, top-viewed videos, comment counts, and more.
6. The results of the SQL queries are displayed within the Streamlit app as tables. Users can choose from a selection of queries, and the corresponding results are presented for analysis.
7. Once the data harvesting, insertion, and analysis processes are complete, a success message is displayed in the Streamlit app.

![video]()

## Prerequisites

- Python
- MongoDB
- MySQL
- Streamlit
- Google API Client
- pandas
