- Creating a streamlit application to visualize the data extracted using Youtube API and storing them into the Mongo DB .
- Install the necessary packages Streamlit, GoogleAPIClient, PYMONGO, MYSQL.Connector. 
- Ensure the local MongoDB and MYSQL Workbench are running.
- Read and update the config file for API KEY , connection information for both Mongo DB and MYSQL.

**Working :-**

- When the User inputs a channelID in the application, the Youtube API is called and the responses of that particular Channel, Videos pertaining to that Channel and the Comments for each video are gathered. All these information are stored in the form of collections in Mongo DB.
- Migrating the data from MongoDB to MYSQL when the user inputs a channelId given in the dropdown list.
*Flow diagram*
![Capstone1](https://github.com/VasumathyVijayakumaran/Youtube-Data-Harvest/assets/162603223/4bd6dc65-d401-4de1-8760-083bae0430f8)
- Some sample SQL data manipulation queries are embedded in the Streamlit to display the Top liked Channel/Video,Most Commented Videos,Average duration of each video, etc., from the MYSQL database.
