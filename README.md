# Customer Behavior and Content

Analyze customer behavior and content by using the datasets started_streams.csv and whatson.csv files

Download the datasets to your local folder.


# Overview

script customer_behavior_content.py holds 3 functions

# Function 1 : sales_rental_broadcast_rights(file_start_stream, file_whatson)

1.  To find the broadcast rights for a title to be able to expose it to the analytics team for further analysis. This should be for Product Types: TVOD and EST. Matching on most recent date for whatson data and joining based on the house_number and country.

2.  sales_rental_broadcast_rights uses two datasets started_streams.csv ans whatson.csv files as arguments.

3.  df_country is a new table/dataframe created, to map country with country code in both the input files. 

# Function 2 : product_user_count(file_start_stream)

1.  Know how many watches a product is getting and how many unique users are watching the content, in what device, country and what product_type.

2.  product_user_count uses started_streams.csv dataset as one argument.

3.  product_watches dataframe contains number of watches a prouct is getting based on date,program title,device_name,country_code.

4.  unique_user_count dataframe contains number of unique user watching based on date,program title,device_name,country_code.

# Function 3 : genre_time_day(file_start_stream)

1.  List the most popular Genre and what hours people watch.

2.  Added new column in start_stream dataframe containing at what hour(Time) of the day people watch.

# Executing the script

When executing the function, the script will prompt to enter the path to the folder. Provide the path until the file name. 
Ex : c:\\assignmnets\\

Note:Do not include the filenames, as it is part of the code.


