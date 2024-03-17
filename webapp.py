import streamlit as st
import pandas as pd
import mysql.connector
st.title("Youtube Data Harvesting")
conn= mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="youtube_data"
)


# Function to execute MySQL query and display result as table
def display_query_result_as_table(mysql_conn, query, table_title):
    # Number of records per page
    records_per_page = 10

    cursor = mysql_conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    if result:
        df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
        st.write(f"### {table_title}")
        st.dataframe(df)

        
    else:
        st.write(f"No data found for {table_title}")

if st.button("Show All Videos and Channels"):
# Query 1: Names of all videos and their corresponding channels

    query1 = """
        SELECT videos.videoName, channels.title AS channelName
        FROM videos
        INNER JOIN channels ON videos.channelId = channels.channelId
        """
    display_query_result_as_table(conn, query1, "Names of all videos and their corresponding channels")
    
if st.button("Show Channel with high number of Videos"):
    query2="""select videoCount,channelId from channels order by videoCount desc limit 1"""
    display_query_result_as_table(conn, query2, "Channel with top video count")
    
if st.button("Show Videos with High View Count"):   
    query3="""select c.title,v.videoName,v.viewCount from channels as c
            inner join videos as v
            on c.channelId=v.channelId
            order by v.viewCount desc
            limit 10"""
    display_query_result_as_table(conn, query3, "Videos that are highly viewed")

if st.button("Show the number of Comments accross each Video"):   
    query4="""select v.videoName,count(c.commentId) from videos as v
            inner join comments as c
            on c.videoId=v.videoId
            group by v.videoName"""
    display_query_result_as_table(conn, query4, "Comment Count of each Video")
    
if st.button("Show the Top liked VideoNames and their Channels "):   
    query5="""select b.ChannelName,a.videoName,a.Highest_Likes from
            (select videoName,likeCount as Highest_Likes,channelId from videos where (likeCount,channelId) 
            in 
            (select max(v.likeCount),c.channelId from Videos as v
                inner join channels as c
                on c.channelId=v.channelId
                group by c.title,c.channelId)) a,
            (select title as ChannelName ,channelId from channels) b
            where a.channelId=b.channelId
"""
    display_query_result_as_table(conn, query5, "Top liked videos of a channel")

if st.button("Show number of likes and dislikes of each video"):   
    query6="""select videoName,likeCount,dislikeCount 
                from videos 
                order by likeCount desc"""
    display_query_result_as_table(conn, query6, "Likes and Dislikes count of each Video")

if st.button("Show total views of a channel"):   
    query7="""select c.title as ChannelName , sum(v.viewCount) from videos v
                inner join channels c
                on v.channelId=c.channelId
                group by ChannelName"""
    display_query_result_as_table(conn, query7, "Total number of Views of a Channel")

if st.button("Show channels who published videos in the year 2022"):   
    query8="""select distinct c.title as channelName from videos v
                inner join channels c
                on c.channelId=v.channelId
                where year(v.publishedAt)='2022'"""
    display_query_result_as_table(conn, query8, "Channels who published videos in the year 2022")
    
if st.button("Show Average Duration of videos accross each channel"):   
    query9="""select b.ChannelName,a.Average_duration_in_seconds from
                (select avg(duration)  as Average_duration_in_seconds,channelId from videos 
                    group by channelId) a,
                (select title as ChannelName,channelId  from channels) b
            where a.channelId=b.channelId"""
    display_query_result_as_table(conn, query9, "Average duration of videos accross each channel")
    
if st.button("Show videos with most Comments along with channel name"):   
    query10="""select b.ChannelName,a.videoName,a.commentCount from
                (select videoName,commentCount,channelId from videos where (commentCount,channelId) in (select max(commentCount),channelId from videos group by channelId))a,
                (select channelId,title as ChannelName from channels) b
            where a.channelId=b.channelId"""
    display_query_result_as_table(conn, query10, "Max comment count of videos accross each channel")