<h1 style="text-align: center;"> Spark Individual Assignment - Maximilian Pichler </h1>


![](pexels-matheus-bertelli-573241.jpg)

> **A spark is a little thing, yet it may kindle the world.** 

\- _Martin Farquhar Tupper_

## Introduction
For this assignment we had to choose a dataset which we then analyse and process using Spark's Python API PySpark. The goal was to implement all the preprocessing steps covered during the first part of the class of the Master in Big Data and Business Analytics at IE - HST.

In addition to the class-material, we where allowed to use the official documentation and suggested readings provided by the professor. For this reason I have acquired the book "Spark - The Definitive Guide"by Bill Chambers and Matei Zaharia to further make myself comfortable with the capabilities of Spark and how to use them efficienlty.

Additionally I have set myself the goals 
- to use a [github-repository](https://github.com/Maximilian-Pichler/Spark) for version-management, 
- to write the report in markdown
- to create UDFs for modularity and reduced redundancy
- to use advanced methods introduced in Spark - Definitive Guide

---

Classes, Methods & Functions covered in this assignment
```py
SparkContext()
SparkSession()
createDataFrame()
printSchema()/schema()
StructType()
StructField()
withColumnRenamed()
select()
groupBy()
orderBy()
show()
first()
withColumn()
when()/otherwise() 
drop()
isNull()
isNotNull()
countDistinct()
col()
alias()
count()
sum()
avg()
agg()
join()
pivot()
expr()
sort()
lit()
summary()
```

---

## What: Background / Scenario
The first step of the process was choosing a dataset that would fit the requirements of having at least 100.000 rows. Fortunately, there are many data sources available nowadays. After a few hours of research I ended up with a handfull of datasets of high quality that would fit these requirements

Having a background in tourism, I ended up choosing the "Hotel Booking Demand" dataset from [Kaggle](https://www.kaggle.com/jessemostipak/hotel-booking-demand). Before moving to Vienna I attended a School of Higher Education for Turism and Hotel Management and spent quite some time working at hotels. It has always fascinated me how many different guests and caracters one can encounter at the same place, and how different and yet similar their booking behaviours are. So I found it interesting to see what insights the data of these two hotels might offer. 

The dataset is the result of a research project, where data was gathered from two different hotels over a period of two years. This data was consolidated into the same structure / schema, anonimized, and then published for research purpuses. 
 
## Why: Goal of Analysis
the goal of the analysis was to help the hotel-managers identify various factors that might help them to improve their revenues. There are many different ways to do that, however based on the data we will focus on the guest-, booking-, and time dimensions of this challenge. 

### Business Questions

#### What does the customer-spending mix look like?
Knowing how much your guests spend daily enables managers to better plan the amount of activities they can offer to their guests

#### How does the booking date weekday correlate with the average spending per person
This information can be used for financial forecasting and cashflow optimization

#### High spender statistics by country
With this informaiton, managers can make decisions on which countries they should focus their marketing-efforts


## How: Analysis Deep Dive
For the analysis several steps need to be performed:
After loading the required methods, functions and the data into the session, the deepdive into the data begins.

### EDA & Preprocessing:
In order to get an overview of the data we have multiple possibilities. The first one beeing the documentation on kaggle which gives us an idea of what the data is representing. For quality of life I'll paste the table here:
**Data description** [Source](https://www.sciencedirect.com/science/article/pii/S2352340918315191)

| variable                       | type    | description                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------------------ | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| hotel                          | string  | Hotel (H1 = Resort Hotel or H2 = City Hotel)                                                                                                                                                                                                                                                                                                                                                                                 |
| is_canceled                    | boolean | Value indicating if the booking was canceled (1) or not (0)                                                                                                                                                                                                                                                                                                                                                                  |
| lead_time                      | integer  | Number of days that elapsed between the entering date of the booking into the PMS and the arrival date                                                                                                                                                                                                                                                                                                                       |
| arrival_date_year              | integer  | Year of arrival date                                                                                                                                                                                                                                                                                                                                                                                                         |
| arrival_date_month             | string  | Month of arrival date                                                                                                                                                                                                                                                                                                                                                                                                        |
| arrival_date_week_number       | integer  | Week number of year for arrival date                                                                                                                                                                                                                                                                                                                                                                                         |
| arrival_date_day_of_month      | integer  | Day of arrival date                                                                                                                                                                                                                                                                                                                                                                                                          |
| stays_in_weekend_nights        | integer  | Number of weekend nights (Saturday or Sunday) the guest stayed or booked to stay at the hotel                                                                                                                                                                                                                                                                                                                                |
| stays_in_week_nights           | integer  | Number of week nights (Monday to Friday) the guest stayed or booked to stay at the hotel                                                                                                                                                                                                                                                                                                                                     |
| adults                         | integer  | Number of adults                                                                                                                                                                                                                                                                                                                                                                                                             |
| children                       | integer  | Number of children                                                                                                                                                                                                                                                                                                                                                                                                           |
| babies                         | integer  | Number of babies                                                                                                                                                                                                                                                                                                                                                                                                             |
| meal                           | string  | Type of meal booked. Categories are presented in standard hospitality meal packages: Undefined/SC – no meal package;  BB – Bed & Breakfast;  HB – Half board (breakfast and one other meal – usually dinner);  FB – Full board (breakfast, lunch and dinner)                                                                                                                                                                 |
| country                        | string  | Country of origin. Categories are represented in the ISO 3155–3:2013 format                                                                                                                                                                                                                                                                                                                                                  |
| market_segment                 | string  | Market segment designation. In categories, the term “TA” means “Travel Agents” and “TO” means “Tour Operators”                                                                                                                                                                                                                                                                                                               |
| distribution_channel           | string  | Booking distribution channel. The term “TA” means “Travel Agents” and “TO” means “Tour Operators”                                                                                                                                                                                                                                                                                                                            |
| is_repeated_guest              | boolean | Value indicating if the booking name was from a repeated guest (1) or not (0)                                                                                                                                                                                                                                                                                                                                                |
| previous_cancellations         | integer  | Number of previous bookings that were cancelled by the customer prior to the current booking                                                                                                                                                                                                                                                                                                                                 |
| previous_bookings_not_canceled | integer  | Number of previous bookings not cancelled by the customer prior to the current booking                                                                                                                                                                                                                                                                                                                                       |
| reserved_room_type             | string  | Code of room type reserved. Code is presented instead of designation for anonymity reasons                                                                                                                                                                                                                                                                                                                                   |
| assigned_room_type             | string  | Code for the type of room assigned to the booking. Sometimes the assigned room type differs from the reserved room type due to hotel operation reasons (e.g. overbooking) or by customer request. Code is presented instead of designation for anonymity reasons                                                                                                                                                             |
| booking_changes                | integer  | Number of changes/amendments made to the booking from the moment the booking was entered on the PMS until the moment of check-in or cancellation                                                                                                                                                                                                                                                                             |
| deposit_type                   | string  | Indication on if the customer made a deposit to guarantee the booking. This variable can assume three categories:   No Deposit – no deposit was made;   Non Refund – a deposit was made in the value of the total stay cost;   Refundable – a deposit was made with a value under the total cost of stay.                                                                                                                    |
| agent                          | string  | ID of the travel agency that made the booking                                                                                                                                                                                                                                                                                                                                                                                |
| company                        | string  | ID of the company/entity that made the booking or responsible for paying the booking. ID is presented instead of designation for anonymity reasons                                                                                                                                                                                                                                                                           |
| days_in_waiting_list           | integer  | Number of days the booking was in the waiting list before it was confirmed to the customer                                                                                                                                                                                                                                                                                                                                   |
| customer_type                  | string  | Type of booking, assuming one of four categories:   Contract - when the booking has an allotment or other type of contract associated to it;  Group – when the booking is associated to a group;  Transient – when the booking is not part of a group or contract, and is not associated to other transient booking;  Transient-party – when the booking is transient, but is associated to at least other transient booking |
| adr                            | double  | Average Daily Rate as defined by dividing the sum of all lodging transactions by the total number of staying nights                                                                                                                                                                                                                                                                                                          |
| required_car_parking_spaces    | integer  | Number of car parking spaces required by the customer                                                                                                                                                                                                                                                                                                                                                                        |
| total_of_special_requests      | integer  | Number of special requests made by the customer (e.g. twin bed or high floor)                                                                                                                                                                                                                                                                                                                                                |
| reservation_status             | string  | Reservation last status, assuming one of three categories:  Canceled – booking was canceled by the customer; Check-Out – customer has checked in but already departed;  No-Show – customer did not check-in and did inform the hotel of the reason why                                                                                                                                                                       |
| reservation_status_date        | integer  | Date at which the last status was set. This variable can be used in conjunction with the ReservationStatus to understand when was the booking canceled or when did the customer checked-out of the hotel                                                                                                                                                                                                                     |
 
 The second tool to better understand the data is pyspark. WIth it we can easily print the schema of the dataset, get the shape of the data and inform ourselfes about the datatypes of the dataset.
 
 Pyspark guessed most of the datatypes right, and we only need to cast a few datatypes. Furthermore I use a helper dataset for weekdays, for which the schema is created manually. Columns that are not essential for answering the business questions will be dropped to improve the processing performance.

### define entities, metrics and dimensions
Having an overview of the data we can now start labeling Entities Metrics and Dimensions of the dataset.

Entities
- Bookings
- Guests (dimension)
- Time (dimension)

Metrics
- average_daily_rate
- cancellations
- distribution channels
- ...

Dimensions
- time
- agents
- guests
- ...

### define groups
For this reason we will get a few random samples that give us an idea about how the data is distributed. with this information we can roughly group the columns into 3 groups

- booking related
- guest related
- timing related


### Basic Group Profiling
Having layed out a solid groundwork, we can now get our hands dirty. We start of with some summary statistics and aggregations that  help us to get a grasp about what the data looks like.

In order to do so, we will perform similar operations on numerical and categorical data, and to make our lifes easier, I created functions that help with identifying these two kinds of datatypes and another function that helps with plotting and formatting the output as markdown tables

- get summary statistics from each group

### Answer Business Qustions
- BQ1: # what does our customer-spending mix look like?
	- transformations, aggregations, groupings
- BQ2
	- 
- BQ3
	- 

	


## Insights: Conlcusion



---

<h1 style="text-align: center;"> Bonus</h1>


## Course Environment
Setting up the VM to allow login via token
change the config files to token

Setting up a samba share, so i do not have to backup the image 

## PiSpark
Setting up the Networking,
setting up CockroachDB and Jupyter on kubernetes
reognizing the hubris 
reverting to hadoop & spark (baremetal, rather than containers or even kubernetes)

Question: Raul & Jorge: how can i get jupyter to run on multiple nodes after having installed hadoop/spark and jupyter

Picture of the cluster