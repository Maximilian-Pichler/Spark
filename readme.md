<h1 style="text-align: center;"> Spark Individual Assignment - Maximilian Pichler </h1>


![](assets/pexels-matheus-bertelli-573241.jpg)

> *A spark is a little thing, yet it may kindle the world.*
\-  _Martin Farquhar Tupper_

## Introduction
For this assignment, we had to choose a dataset which we then analyze and process using Spark's Python API PySpark. Spark is an open-source application supported by data bricks. Its features like cluster-computing, in-memory processing, and lazy evaluation make it a potent tool for processing and analyzing Big Data. 

The goal was to implement all the preprocessing steps covered during the first part of the class of the Master in Big Data and Business Analytics at IE - HST.

In addition to the class-material, we were allowed to use the professor's official documentation and suggested readings. For this assignment, "Spark - The Definitive Guide" by Bill Chambers and Matei Zaharia was used to get comfortable with Spark's capabilities and learn how to use them efficiently.

### Goals
The assignment's goal is to use all the classes, methods & functions for preprocessing that were covered in class, write a report of about ten pages, and film a short video presenting the work.


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
**additionally to the requirements above, the following personal goals were set:**
- to use GitHub for version-management, see [GitHub Repository here](https://github.com/Maximilian-Pichler/Spark)
- to write the assignment-report in markdown
- to create UDFs for modularity and reduced redundancy
- to use advanced methods introduced in Spark - Definitive Guide
- to use [user-stories](https://www.atlassian.com/agile/project-management/user-stories) to formulate the business questions

## The Dataset
The first step of the process was choosing a dataset that would fit the requirements of having at least 100.000 rows. Fortunately, there are many data sources available nowadays. After a few hours of research, I have found a handful of datasets that had the prerequisites needed for this assignment.
Having a background in Tourism and Hotel Management, I have spent quite some time working at hotels in various positions. It has always fascinated me how many different guests and characters one can encounter in these places and how different and yet similar their booking and spending behaviors are. The aforementioned is the reason I ended up choosing the "Hotel Booking Demand" dataset from [Kaggle](https://www.kaggle.com/jessemostipak/hotel-booking-demand), and I was curious to see what insights the data of these two hotels might offer.

The  **"Hotel Booking Demand"** dataset is a result of a research project where data was gathered from two different hotels. This was done for two years, and the data contains booking information for two hotels with 31 variables describing 119,390 observations between the 1st of July of 2015 and the 31st of August 2017.
This data was consolidated into the same structure/schema, anonymized, and then published for research purposes.^[Source: (https://www.sciencedirect.com/science/article/pii/S2352340918315191)]

## The Story

In this scenario, the two hotels from the dataset belong to a Hedgefond that mainly invests in hotels and bars, called "Hotels and Drinks on our Premise," short HADOOP. We just started our internship in the Advanced Analytics department, and in order to work on big projects, we first need to prove our Spark skills on three simple business questions.

As HADOOP also started a huge transformation program to make their business more "agile", the lead analysts and managers use  **[user-stories](https://www.agilealliance.org/glossary/user-stories/#q=~(infinite~false~filters~(postType~(~'page~'post~'aa_book~'aa_event_session~'aa_experience_report~'aa_glossary~'aa_research_paper~'aa_video)~tags~(~'user*20stories))~searchTerm~'~sort~false~sortDirection~'asc~page~1)])** to formulate their business questions. 

To prove our worthiness, we are only allowed to use basic Python and the PySpark Library...nothing else. Luckily this is exactly what we have studied during our Master's at the Institute of Human Sciences and Technology at IE University. 

One of the Senior Data Analysts sent us the credentials needed to connect to the remote Jupyter Server running on a Hadoop cluster and the modules and functions to use Spark in the environment. 

Usually, HADOOP's data is stored in databases, but this is our first day at work, and the Database Admins yet have to configure the access-permissions for our user. Luckily one of the Senior Analysts exports two tables for us as a CSV that we can use until then. He also hands us a very used-looking book with the title "Spark - The Definitive Guide"...

---
---

<h1 style="text-align: center;"> Customer Spending through the Lens of Sales, Marketing, and Finance - a Report </h1>

![](assets/pexels-hosein-ashrafosadat-243204.jpg)


## What: Background
Due to the lack of hotel guests during the 2020 pandemic, it is an excellent time to decide on HADOOP's hotels' strategic actions. Forecasts suspect that hotels in this area will not accept guests for at least another year, so the time can be spent planning, evaluating, and executing major projects without affecting current operations. 

The Hedgefond's recently launched Big Data Architecture enables their analysts to quickly gather and process the information needed and set up a baseline for data-driven decisions. This data can then also be used to evaluate these projects' results in near real-time and allow them to adapt the scope, time, and budget dimensions accordingly.
 
## Why: Goal of Analysis
The goal of the analysis is to help decision-makers identify various factors that help them improve revenues and reduce costs. The CEO has given his strategic analysts instructions to interview the sales-, marketing and finance-managers and work out the most pressing business questions that need to be answered. 

### Business Questions
HADOOP's analysts have concluded that decisions on how to increase the company's ROI should be based on customer spending. This variable gives managers insights to decide which areas they need to put their focus on.
The three business questions with the highest priority are presented below.

#### Question 1 - Sales
**As a sales analyst of HADOOP's hotels, I would like to know what the overall customer-spending mix looks like in order to evaluate additional offers for our hotels.**

Customer spending per person should therefore be assigned to categories according to the following criteria:

| category name          | customer spending per person |
| ---------------------- | ---------------------------- |
| 1 very high            | > 85€                        |
| 2 high                 | between 62€ and 85€          |
| 3 average              | between 45€ and 62€          |
| 4 low                  | between 28€ and 45€          |
| 5 very low             | between 0€ and 28€           |
| 6 something went wrong | < 0€                         |

**additional information**
*Adults count as one person; children as 0.5;  babies as 0.2.*

#### Question 2 - Finance
**As a financial analyst of HADOOP's hotels, I would like to know how the booking-months correlate with the customer spending per person to optimize my cashflow prediction models.**

#### Question 3 - Marketing
**As a marketing analyst of HADOOP's hotels, I would like to know from what countries the most spending customers come from to optimize the ROI on our marketing campaigns.**

#### Question 3.1 - Marketing
**As a marketing analyst of HADOOP's hotels, I would like to know what the customer-spending-mix looks like for both hotels to estimate the budget needed for our marketing campaigns. **

#### Question 4 - IT
**As an IT analyst of HADOOP's booking-systems, I would like to know the distribution of bookings per weekday to estimate the workload on our systems**

## How: Analysis Deep Dive
### EDA
In order to get an overview of the data, we have multiple possibilities. The first one is to read the documentation, which gives us an idea of what the data represents.
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
 
The second tool to better understand the data is to use python and PySpark capabilities for Exploratory Data Analysis (EDA). This way, we can easily print the schema, get the shape, and inform ourselves about the datatypes of the dataset.
 
#### Preprocessing
##### Schema
Loading the dataset with `option("inferSchema", "true")` we can see that PySpark already guessed most of the datatypes correctly and only recast the data types of a few columns.
For the second CSV - which only contains two columns - the schema is created manually.

##### Renaming Columns
We rename the column "adr" to "average_daily_rate" to make it immediately recognizable what it represents.

##### NULL Values
Finally, we will check the quality of the data by counting how many missing values it contains. Here we can see that NULL values might come in a different format, as the first query shows zero NULL values and the second - with `(col(c) \== "NULL")` show at least a few.
- country: 488 NULL Values
- agent: 16340^[later in the report we see that there is also a "undefined" value in this colum. One could also decide to reassign the null values to the "undefined" or vice versa]

##### Performance Tuning - dropping & caching
To work efficiently, some columns - that are not essential for answering the business questions - will be dropped to improve the processing performance. 
Spark offers multiple ways of persisting data on DISK_ONLY, MEMORY_ONLY, DISK & MEMORY, and OFF_HEAP. We make use of the `cache()` function, which calls `persist(StorageLevel.MEMORY_AND_DISK)`, making the best use of the RAM available to the Virtual Machine.^[Source: https://sparkbyexamples.com/spark/spark-difference-between-cache-and-persist]

#### Entities, Metrics and Dimensions
Having an overview of the data, we now start labeling Entities, Metrics, and Dimensions. This step helps us to understand how we can use variables to answer business questions.

| Entities           | Metrics                 | Dimensions |
| ------------------ | ----------------------- | ---------- |
| Bookings           | average_daily_rate      | time       |
| Guests (dimension) | number of cancellations | agent      |
| Time (dimension)   | stays in nights         | countries  |
| hotel (dimension)  | lead_time               | hotel      |
| ...                | ...                     | ...        |

#### Groups Definition
Group definitions help us to prefilter the possible Entities, Metrics, and dimensions according to the information they contain. This step helps with streamlining the processing steps for answering the business questions.

To make this easier, we will print a few random samples that give us an idea about how the data's content and how it is distributed. We do this in two different ways, once with a PySpark data frame and once we first transform the data frame to a pandas data frame with `toPandas()`. They both show the same data; it is up to personal preference which one looks nicer.

Having a look at the data, we can define **three groups**:
- booking-related
- guest-related
- timing-related

### Basic Profiling per Group
Having established a good understanding of the meta-perspective, we can now get our hands dirty. 
We start with some summary statistics and aggregations that should help us understand the macro-level data.

For this, we will perform various operations for numerical and categorical data in each group. To make our lives easier, we create lists containing the column-names of each group and functions that help identify the numerical and categorical columns of each group.
Furthermore, we use another User Defined Function (UDF) to plot and format the output in markdown tables.

These preparations allow us to execute the same code and only change the group that we want to analyze^[In another version of this script, we could create another function, that would automatically loop over this code and change the group atuomatically, and thus further improve code reuse]
- print the ten most occurring entries per group
- print the highest and lowest counts of categorical data per group
- print the unique number of values of categorical data per group
- print summary statistics of numerical data per group using `summary()`

After finishing the EDA, we are comfortable with the micro-, macro-, and meta-level data and can tackle the business questions.

#### Interesting Findings
##### booking-related
- someone waited 391 days to get the booking confirmed
- someone had an average daily rate of 5400€ (the average is 94€)
- one agent is responsible for 1/3 of the bookings for both hotels

##### timing-related
- someone booked a room more than two years in advance
- January is the least popular month for staying in these hotels

##### guest-related
- a fifth of the bookings happen by couples from Portugal, that travel with children or babies, order Bed&Breakfast, room type A and stay at the hotel for the first time
- Roomtype "L" has only been booked six out of 119390 times

### Answer Business Qustions
#### Business Question 1
To answer this business question, we want to create categories for average daily spendings per person. Before doing so, we need to calculate the number of persons per booking. For this, we implement the formula provided with the business question. After calculating this, we can then divide the average spending rate of the booking by the number of persons and get the average daily spending per person. 

- pax = persons per booking
- a = adults
- c = children
- b = babies
- adspp = average daily spending per pax
- ads = average daily spending*

$$
pax = a + c * 0.5 + b * 0.2 \
$$
$$
adspp = ads / pax 
$$

With this information, we then categorize each booking into one of six categories of customer-spending (very high, high, average, low, very low, something went wrong).

After assigning a category to each booking, we can aggregate by it and count the occurrences of each of them. 

| customer_spending      | count |
| ---------------------- | ----- |
| 4 low                  | 37655 |
| 3 average              | 32325 |
| 2 high                 | 23224 |
| 1 very high            | 15347 |
| 5 very low             | 9028  |
| 6 something went wrong | 1811  |

With categories 5. and 6. at the bottom, it seems to be a good mix. However, with low-spenders on the top, we found something to act on.

#### Business Question 2
This was a rather tricky question to answer because we get the booking dates and thus their weekday through a slight detour. 

The first move here is to create a column that contains the date of arrival. To achieve this, we needed to transform the arrival_date_month column, containing strings of months, and concatenate it with "year_of_arrival"and "day_of_month" resulting in a "yyyy-mm-dd" format. 

Next subtract the lead_time value with `expr("date_add(to_date(arrival_date,'yyyy-MM-dd'),-cast(lead_time as int))")`, because this is the only way we can pass a column as argument for `date_add()`. 

Finally, we create dummy-variables which we then use to calculate the ratio of very_high spenders per booking weekday.


#### Business Question 3
For this question we print the summary statistics for customer-spendings per country by using the `avg(), stddev(), min(), max()` methods.
As we can see, a guest coming from Serbia is a high-spender with a probability of 64%. This could mean that if we increase our marketing efforts in Serbia, we might expect a higher ROI than compared to other countries.

#### Business Question 3.2
For this question, the data frame from BQ 2 comes in handy again as it contains all the data needed. 
We need to pivot and aggregate "customer_spending" and group the data by "hotel".
As we can see, the average customer spending mix is quite diverse for both hotels.

#### Business Question 4
For this question, we finally come to use `join()` to create a weekday column. After doing so, we experiment with `spark.sql()` and query the data differently.  Below we can see the differences in booking counts per weekday.

The counts do not seem to differ that much in sum. In real life, we would call the IT analyst and consult with him if this is this data answers his question, or if we dig a little bit deeper and formulate another business question.

| day_of_week | count(day_of_week) |
| ----------- | ------------------ |
| Friday      | 19631              |
| Thursday    | 19254              |
| Monday      | 18171              |
| Saturday    | 18055              |
| Wednesday   | 16139              |
| Sunday      | 14141              |
| Tuesday     | 13999              |

## Insights: Conclusion
The data processed and analyzed show a few promising results for the HADOOP Hedgefund. Sales and marketing questions have yielded exciting insights and show that the customer-spending-mix is a promising KPI to measure improvements.

The data also shows a clear difference between our two hotels, which might be due to their difference in location, management, quality, or target group.

It might also be due to the country of origin mix, as we have seen significant differences in average spending in this regard.

Concerning the Question from IT, we might go back to the analyst and discuss the business question and our analysis results. While we have found an answer to the question, further analysis might be proper.

For future analysis or processing, we should change the data source to something like a database to guarantee that the data is up-to-date. We might also implement a pipeline to ingest, process, and finally serve the data to a dashboard that the CEO can access. This way, it is possible to track the KPIs in near-real-time.

---