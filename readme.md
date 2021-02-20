<h1 style="text-align: center;"> Spark Individual Assignment - Maximilian Pichler </h1>


![](pexels-matheus-bertelli-573241.jpg)

> *A spark is a little thing, yet it may kindle the world.*
\-  _Martin Farquhar Tupper_

## Introduction
For this assignment we had to choose a dataset which we then analyse and process using Spark's Python API PySpark. The goal was to implement all the preprocessing steps covered during the first part of the class of the Master in Big Data and Business Analytics at IE - HST.

In addition to the class-material, we where allowed to use the official documentation and suggested readings provided by the professor. For this reason I have acquired the book "Spark - The Definitive Guide" by Bill Chambers and Matei Zaharia to further make myself comfortable with the capabilities of Spark and how to use them efficienlty.

### Goals
The goal of the assignment is to use all the classes, methods & fucntoins for preprocessing that were covered in class, write a report of about ten pages and film a short video presenting the work.


```py
# Classes, Methods & Functions we need to use for this assignment:
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

#### Additional Goals
**additionally to the requirements above, I have set myself the following goals:**
- to use a github for version-management, see [github repository](https://github.com/Maximilian-Pichler/Spark)
- to write the assignment-report in markdown
- to create UDFs for modularity and reduced redundancy
- to use advanced methods introduced in Spark - Definitive Guide
- to use [user-stories](https://www.atlassian.com/agile/project-management/user-stories) to formulate the business questions

## The Dataset
The first step of the process was choosing a dataset that would fit the requirements of having at least 100.000 rows. Fortunately, there are many data sources available nowadays. After a few hours of research I ended up with a handfull of datasets of high quality that had all the prequisites needed for the assignment.

Having a background in Tourism and Hotel Management I have spent quite some time working at hotels in various positions. It has always fascinated me how many different guests and caracters one can encounter in these places, and how different and yet similar their booking and spending behaviours are. This is the reason I ended up choosing the "Hotel Booking Demand" dataset from [Kaggle](https://www.kaggle.com/jessemostipak/hotel-booking-demand), and i was curious to see what insights the data of these two hotels might offer. 


The **"Hotel Booking Demand"** dataset is the result of a research project, where data was gathered from two different hotels over a period of two years and contains booking information for two hotels with 31 variables describing 119,390 observations  between 1st of July of 2015 and the 31st of August 2017.

This data was consolidated into the same structure / schema, anonimized, and then published for research purpuses.^[Source: (https://www.sciencedirect.com/science/article/pii/S2352340918315191)]

## The Story

In this scenario, the two hotels from the dataset belong to a Hedgefond that mainly invests in hotels and bars, called "Hotels and Drinks on our Premise", short HADOOP. We just started our internship in the Advanced Analytics department, and in order to work on big projects we first need to proove our Spark skills on three simple business questions that otherwise no one had the time to work on.

As HADOOP also started a hughe transformation programm to make their business more "agile", the lead analysts and managers use **[user-stories](https://www.agilealliance.org/glossary/user-stories/#q=~(infinite~false~filters~(postType~(~'page~'post~'aa_book~'aa_event_session~'aa_experience_report~'aa_glossary~'aa_research_paper~'aa_video)~tags~(~'user*20stories))~searchTerm~'~sort~false~sortDirection~'asc~page~1)])** to formulate their business questions. 

To proove our worthyness we are only allowed to use basic Python and the PySpark Library...nothing else. Luckily this is exactly what we have studied during our Master at the Institute of Human Sciences and Technology at IE University. 

One of the Senior Data Analysts sent us the credentials needed to connect to the Remote Jupyter Server running on a Hadoop cluster and the modules and functions that we need to use Spark. 

Usually the data of HADOOP is stored in databases, but this is our first day at work the Database Admins yet have to configure the access-permissions for our user. Luckily one of the Senior Analysts exports two tables for us as a CSV that we can use until then. He also hands us a very used looking book with the title "Spark - The Definitive Guide"...

---
---

<h1 style="text-align: center;"> Customer Spending through the Lens of Sales, Marketing, and Finance - a Report </h1>

![](pexels-hosein-ashrafosadat-243204.jpg)


## What: Background
Due to the lack of hotel guests during the 2020 pandemic, it is a good time to decide on strategic actions for HADOOP's hotels. Forecasts suspect, that hotels in this area won't be able to accept guests for at least another year, so the time can be spent on plannin, evaluating and executing major projects without affecting current operation. 

The Hedgefond's recently launched Big Data Architecture, enables their analysts to gather and process the information needed and set up a baseline for datadriven decisions quickly. This data can then also be used to evaluate the results of these projects in near real-time, and allow them to adapt the scope, time and budget dimensions accordingly.
 
## Why: Goal of Analysis
The goal of the analysis is to help decision makers identify various factors that help them to improve revenues and reduce costs. The CEO has given instructions to his strategic analysts, to interview the sales-, marketing and finance-managers and work out the most pressing business questions that need to be answered. 

### Business Questions

HADOOP's analysts have concluded that decisions on how to increase the company's ROI should be based on customer spending. This variable gives managers insights to decide which areas they need to put their focus on.
The three business questions with the highest priority are presented below.

#### Question 1 - Sales
**As sales analyst of HADOOP's hotels I would like to know what the overall customer-spending mix looks like in order to evaluate additional offers for our hotels.**

Customer spending per person should therefore be assigned to categories according to the following criteria:

| category name          | customer spending per person |
| ---------------------- | ---------------------------- |
| 1 very high            | > 85€                        |
| 2 high                 | between 62€ and 85€          |
| 3 average              | between 45€ and 62€          |
| 4 low                  | between 28€ and 45€          |
| 5 very low             | between 0€ and 28€           |
| 6 something went wrong | < 0€                         |

#### Question 2 - Finance
**As financial analyst of HADOOP's hotels I would like to know how the weekdays of bookings correlate with the customer spending per person, in order to optimize my cashflow prediction models.**

#### Question 3 - Marketing
**As marketing analyst of HADOOP's hotels I would like to know from what countries the most spending customers come from to optimize the ROI on our marketing campaigns.**

## How: Analysis Deep Dive
### EDA
In order to get an overview of the data we have multiple possibilities. The first one is to read the the documentation, which gives us an idea of what the data is representing.
[**Source**](https://www.sciencedirect.com/science/article/pii/S2352340918315191)

| variable                       | type    | description                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------------------ | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| is_canceled                    | boolean | Value indicating if the booking was canceled (1) or not (0)                                                                                                                                                                                                                                                                                                                                                                  |
| is_repeated_guest              | boolean | Value indicating if the booking name was from a repeated guest (1) or not (0)                                                                                                                                                                                                                                                                                                                                                |
| adr                            | double  | Average Daily Rate as defined by dividing the sum of all lodging transactions by the total number of staying nights                                                                                                                                                                                                                                                                                                          |
| adults                         | integer | Number of adults                                                                                                                                                                                                                                                                                                                                                                                                             |
| arrival_date_day_of_month      | integer | Day of arrival date                                                                                                                                                                                                                                                                                                                                                                                                          |
| arrival_date_week_number       | integer | Week number of year for arrival date                                                                                                                                                                                                                                                                                                                                                                                         |
| arrival_date_year              | integer | Year of arrival date                                                                                                                                                                                                                                                                                                                                                                                                         |
| babies                         | integer | Number of babies                                                                                                                                                                                                                                                                                                                                                                                                             |
| booking_changes                | integer | Number of changes/amendments made to the booking from the moment the booking was entered on the PMS until the moment of check-in or cancellation                                                                                                                                                                                                                                                                             |
| children                       | integer | Number of children                                                                                                                                                                                                                                                                                                                                                                                                           |
| days_in_waiting_list           | integer | Number of days the booking was in the waiting list before it was confirmed to the customer                                                                                                                                                                                                                                                                                                                                   |
| lead_time                      | integer | Number of days that elapsed between the entering date of the booking into the PMS and the arrival date                                                                                                                                                                                                                                                                                                                       |
| previous_bookings_not_canceled | integer | Number of previous bookings not cancelled by the customer prior to the current booking                                                                                                                                                                                                                                                                                                                                       |
| previous_cancellations         | integer | Number of previous bookings that were cancelled by the customer prior to the current booking                                                                                                                                                                                                                                                                                                                                 |
| required_car_parking_spaces    | integer | Number of car parking spaces required by the customer                                                                                                                                                                                                                                                                                                                                                                        |
| reservation_status_date        | integer | Date at which the last status was set. This variable can be used in conjunction with the ReservationStatus to understand when was the booking canceled or when did the customer checked-out of the hotel                                                                                                                                                                                                                     |
| stays_in_week_nights           | integer | Number of week nights (Monday to Friday) the guest stayed or booked to stay at the hotel                                                                                                                                                                                                                                                                                                                                     |
| stays_in_weekend_nights        | integer | Number of weekend nights (Saturday or Sunday) the guest stayed or booked to stay at the hotel                                                                                                                                                                                                                                                                                                                                |
| total_of_special_requests      | integer | Number of special requests made by the customer (e.g. twin bed or high floor)                                                                                                                                                                                                                                                                                                                                                |
| agent                          | string  | ID of the travel agency that made the booking                                                                                                                                                                                                                                                                                                                                                                                |
| arrival_date_month             | string  | Month of arrival date                                                                                                                                                                                                                                                                                                                                                                                                        |
| assigned_room_type             | string  | Code for the type of room assigned to the booking. Sometimes the assigned room type differs from the reserved room type due to hotel operation reasons (e.g. overbooking) or by customer request. Code is presented instead of designation for anonymity reasons                                                                                                                                                             |
| company                        | string  | ID of the company/entity that made the booking or responsible for paying the booking. ID is presented instead of designation for anonymity reasons                                                                                                                                                                                                                                                                           |
| country                        | string  | Country of origin. Categories are represented in the ISO 3155–3:2013 format                                                                                                                                                                                                                                                                                                                                                  |
| customer_type                  | string  | Type of booking, assuming one of four categories:   Contract - when the booking has an allotment or other type of contract associated to it;  Group – when the booking is associated to a group;  Transient – when the booking is not part of a group or contract, and is not associated to other transient booking;  Transient-party – when the booking is transient, but is associated to at least other transient booking |
| deposit_type                   | string  | Indication on if the customer made a deposit to guarantee the booking. This variable can assume three categories:   No Deposit – no deposit was made;   Non Refund – a deposit was made in the value of the total stay cost;   Refundable – a deposit was made with a value under the total cost of stay.                                                                                                                    |
| distribution_channel           | string  | Booking distribution channel. The term “TA” means “Travel Agents” and “TO” means “Tour Operators”                                                                                                                                                                                                                                                                                                                            |
| hotel                          | string  | Hotel (H1 = Resort Hotel or H2 = City Hotel)                                                                                                                                                                                                                                                                                                                                                                                 |
| market_segment                 | string  | Market segment designation. In categories, the term “TA” means “Travel Agents” and “TO” means “Tour Operators”                                                                                                                                                                                                                                                                                                               |
| meal                           | string  | Type of meal booked. Categories are presented in standard hospitality meal packages: Undefined/SC – no meal package;  BB – Bed & Breakfast;  HB – Half board (breakfast and one other meal – usually dinner);  FB – Full board (breakfast, lunch and dinner)                                                                                                                                                                 |
| reservation_status             | string  | Reservation last status, assuming one of three categories:  Canceled – booking was canceled by the customer; Check-Out – customer has checked in but already departed;  No-Show – customer did not check-in and did inform the hotel of the reason why                                                                                                                                                                       |
| reserved_room_type             | string  | Code of room type reserved. Code is presented instead of designation for anonymity reasons                                                                                                                                                                                                                                                                                                                                   |
 
 The second tool to better understand the data is to use python and pyspark capabilities for Exploratory Data Analysis (EDA). This way we can easily print the schema, get the shape, and inform ourselfes about the datatypes of the dataset.
 
#### Preprocessing
**Schema**
Loading the dataset with `option("inferSchema", "true")` we can see that PySpark already guessed most of the datatypes correctly. Only a few datatypes need to be recasted, 
For the second csv - which only contains two columns - the schema is created manually.

**Renaming Columns**


**NULL Values**
Last but not least, we will check the quality of the data, by checking how many missing values it contains: 

**Performance Tuning - droppping & caching**
and some columns - that are not essential for answering the business questions - will be dropped to improve the processing performance. 
Spark offers multiple ways of persisting data on DISK_ONLY, MEMORY_ONLY, DISK & MEMORY and OFF_HEAP. We make use of the `cache()` function, which basically calls `persist(StorageLevel.MEMORY_AND_DISK)`, thus making best use of the RAM available to the Virtual Machine.^[Source: https://sparkbyexamples.com/spark/spark-difference-between-cache-and-persist]

#### Entities, Metrics and Dimensions
Having an overview of the data we now start labeling Entities, Metrics and Dimensions of the dataset. This step helps us to understand how we can use variables to answer business questions.

| Entities           | Metrics                 | Dimensions |
| ------------------ | ----------------------- | ---------- |
| Bookings           | average_daily_rate      | time       |
| Guests (dimension) | number of cancellations | agents     |
| Time (dimension)   | stays in nights         | countries  |
| hotel (dimension)  | lead_time               | hotel      |
| ...                | ...                     | ...        |

#### Groups Definition
Group definitions help us to prefilter the possible Entities, Metrics and dimension according to the information they contain. This further helps with streamlining the processing steps for answering the business questions.

To make this easier, we will print a few random samples that give us an idea about how the content of the data and how it is distributed. 

Having a look at the data, we can define **three groups**:
- booking-related
- guest-related
- timing-related

#### Basic Group Profiling
Having established a good understanding of the meta-perspective, we can now get our hands dirty. 
We start of with some summary statistics and aggregations that help us to understand the data on a macro-level.

For this we will perform different operations on numerical and categorical data. And, to make our lifes easier, we create functions that help with identifying these two kinds of categories. Furthermore we use another User Defined Function (UDF) to plot and format the output in markdown tables.

Next, we plot an aggregated overview of all columns for each group, count the most and least frequently occuring values and print summary statistics, using pysparks `summary()` method.

After finishing the EDA, we are comfortable with the data on the micro-, macro-, and meta-level and can tackle the business questions.

**Findings** 

### Answer Business Qustions
#### Business Question 1
In order to answer this business question, we first create categories for average daily spendings. In order to assign them properly we also need to calculate the average daily spending per PAX, wich takes into account that couples with childre don't spend as much on their children as on themselfes.

with this information we then categorize each booking into one of 6 categories of customer-spending (very high, high, average, low, very low, something went wrong).

After assingning a category to each booking we can then aggregate by it, and count them. This table gives us a nice overview of the spending-behaviour of these guests
	
#### Business Question 2
This was a rather tricky question to answer, because we do only have fractured information about dates. 
The first move here, was to create a proper column that contains the date of arrival. In order to achieve this, we needed to transform the arrival_date_month colum, containing strings of months, and concatenate it with "year_of_arrival"and "day_of_month"resulting in a yyyy-mm-dd format. 

The next step, was do subtract the lead_time value and so get the date of booking. The booking date was then used to join the helper table containing the day of week, which then in turn was aggregated with the average spending rate per person to get answer this question

#### Business Question 3
For this question the dataframe from the previous question came in handy. It contained all the data needed. In order to answer the business question the customer_spending column needed to be pivoted and aggregated.
	
Last but not least we also printed summary statistics for countries and the spending rates 

## Insights: Conlcusion
It seems that the overall customer spending mix is already well distributed, however, some performance optimization can still be done. The difference in the two hotels suggests, that the management of them could learn from each other, Good customer mix
weekday seems to correlate. 
One of the hotels needs to work on its customer mix
The country of origin matters

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