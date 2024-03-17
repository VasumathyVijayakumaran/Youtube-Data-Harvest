Read the "config.json" for required Youtube API, MONGO and MYSQL connection information.

1)Get the google youtube data into local mongo db with the list of channel Ids in Python


2) Get the Mongo information and YouTube information from config file like API KEY, List of channelIds, Mongo Connection and Collection information
and mySql connection info

3)Get the following Channel information into Mongo channel collection by ChannelId
	ChannelId
	videoCount
	subscriberCount
	title
	description
	playlistId (uploads)
	channelType (kind)
	

4) Get the following Channel Video information into Mongo video collection by ChannelId

	ChannelId
	videoId
	videoName
	videoDescription
	thumbnailUrl (default/URL)
	publishedAt
	viewCount
	likeCount
	dislikeCount
	favoriteCount
	commentCount
	captionStatus
	duration
	durationInSeconds 
	
	Note Duration needs to be converted to HH:MM:SS

5)Get the list of comments for video into Mongo comment collection by videoId

	ChannelId
	videoId
	commentId
	commentText
	commentAuthorName
	commentPublishedDate
	
6)Verify the data written to MONGO DB.

7)Migrate the Data from Mongo DB into MYSQL using Python script.

8)Create a Streamlit python code to display the queries in the UI.
