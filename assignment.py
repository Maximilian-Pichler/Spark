# %%
import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from IPython.display import display, Markdown
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import when, count, col, countDistinct, \
                                    desc, asc, round, date_format, \
                                    concat_ws, expr, month, \
                                    first, lit, max, min, stddev, avg

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# %% [markdown]
# # Import Data

# %%
data_df = \
    spark.read.option("inferSchema", "true")\
        .option("header", "true")\
        .csv('Data/Spark Lab/Individual Assignment/hotel_bookings.csv')
data_df.cache()

dow_schema = StructType(\
    [StructField("date",DateType(),True),\
     StructField("day_of_week",StringType(),True)])

days_of_week = \
    spark.read.schema(dow_schema)\
        .option("header", "true")\
        .option("sep", ";")\
        .csv('Data/Spark Lab/Individual Assignment/day_of_week3.csv')

days_of_week.first()

# %%
# assigning the schema and column names to variables
columns = data_df.schema.names
total_bookings = data_df.count()


display(Markdown('printing the schema of the dataset'))
data_df.printSchema()

display(Markdown(f'the dataset consists of **{total_bookings}** rows'))

# %% preprocessing
# perform typecasts where needed
# change column names
# drop columns that are not needed

data_df = \
    data_df.withColumn("is_canceled",col("is_canceled").cast("boolean"))\
        .withColumn("is_repeated_guest",col("is_repeated_guest").cast("boolean"))\
        .withColumn("adr",col("adr").cast("double"))\
        .withColumnRenamed("adr", "average_daily_rate")\
        .drop('required_car_parking_spaces')\
        .drop('previous_cancellations')\
        .drop('previous_bookings_not_canceled')\
        .drop('assigned_room_type')\
        .drop('booking_changes')\
        .drop('deposit_type')\
        .drop('company')\
        .drop('reservation_status_date')\


# update columns
columns = data_df.schema.names

#%%
display(Markdown('get a random sample from the dataset with spark'))
print(data_df.sample(False, 0.1).take(2))

display(Markdown('get a random sample from the dataset with pandas'))
pandas_sample_df = data_df.toPandas()
pandas_sample_df.sample(n=2)


# %%
display(Markdown('printing null values per column'))
# thank you Raúl for this line of code!
# it took a while to understand it, it is genius!
data_df.select([count(when(col(c).isNull(), c)).alias(c) for c in columns[:10]]).show()             
data_df.select([count(when(col(c).isNull(), c)).alias(c) for c in columns[10:19]]).show()
data_df.select([count(when(col(c).isNull(), c)).alias(c) for c in columns[19:29]]).show()
data_df.select([count(when(col(c).isNull(), c)).alias(c) for c in columns[29:]]).show()

# %% [markdown]
# In this data set, consisting of 119.390 rows we can see a mix of integer-, double- and string-type data. The initial check for NULL values (with .isNull()) suggested that there are no missing values. However, running the code again with == 'NULL' we can see that the columns "company", "agent", and "country" have some missing values.
# %%
display(Markdown('printing null values per column'))
# thank you Raúl for this line of code!
# it took a while to understand it, it is genius!
data_df.select([count(when(col(c).isNull() | (col(c) == "NULL"), c)).alias(c) for c in columns[:10]]).show()             
data_df.select([count(when(col(c).isNull() | (col(c) == "NULL"), c)).alias(c) for c in columns[10:19]]).show()
data_df.select([count(when(col(c).isNull() | (col(c) == "NULL"), c)).alias(c) for c in columns[19:29]]).show()
data_df.select([count(when(col(c).isNull() | (col(c) == "NULL"), c)).alias(c) for c in columns[29:]]).show()

# %% [markdown]
# Next, lets try to define some groups of columns. For this reason, we will first check [the datasource](https://www.kaggle.com/jessemostipak/hotel-booking-demand) and get ourselves familiar with the contents of each column.
# With this information at hand, we can further categorize the data, helping us to better understand what the data can tell us.
# 
# # Entries, Metrics & Dimensions
# 
# ## Entities: 
# - Bookings, 
# - Guests (dimension)
# - Hotels (dimension)
# - Distribution Channels (dimension)
# - Time
# 
# ## Metrics: 
# - average_daily_rate
# - cancellations
# - distribution channels
# - ...
# 
# ## Dimensions: 
# - guests 
# - agents 
# - timing
# - ...
# 
# # Column Categorization
# - Booking related
# - Timing related
# - Guest/Targetgroup related

# %%
# define group variable "booking"
booking = ['hotel', 'is_canceled', 'market_segment', 'agent', 
            'days_in_waiting_list', 'reservation_status', 
             'distribution_channel', 'average_daily_rate']

# define group variable "time"
time = ['lead_time', 'arrival_date_year', 'arrival_date_month', 
        'arrival_date_week_number', 'arrival_date_day_of_month', 
        'stays_in_weekend_nights', 'stays_in_week_nights']

# define group variable "guests"
guest = ['adults', 'children', 'babies', 'country', 'is_repeated_guest',  
        'meal', 'reserved_room_type'] 

# %% [markdown]
# With these groups, we can now create some basic insights about the data.
# We will query for dinsinct values, counts and summary statistics of numerical and categorical columns.

# %%
# Since we are going to use different metrics depending on the datatype, functions to get these appropriate columnnames can be handy.
# The basic idea here was to spend more time with one column-group, think about how to modularize functions so they can be used in multiple ways, to create a recipe that can be used regardless of the data at hand. This recipe can then be applied to the other column-groups.

def get_categoricals(data_df):
    """This function takes as input a spark dataframe and returns a list of its StringType columnames"""
    categoricals = [column.name for column in data_df.schema.fields if isinstance(column.dataType, StringType)]
    return categoricals


def get_numericals(data_df):
    """This function takes as input a spark dataframe and returns a list of its IntegerType and DoubleType columnames"""
    numericals =  [column.name for column in data_df.schema.fields if isinstance(column.dataType, (IntegerType, DoubleType))]
    return numericals

def get_min_max(data_df):
    for category in get_categoricals(data_df):
        first = data_df.groupBy(category).count().sort(desc('count')).first()
        last = data_df.groupBy(category).count().sort(asc('count')).first()
        total = data_df.groupBy(category).agg(count(lit(1)))
        display(Markdown("""
| %s | %s | %s |
|----|----|----|
| %s | %s | %s |
""" % (f"least_{category}", f"most_{category}",
    "%s (%d occurrences)" % (first[category], first["count"]), 
    "%s (%d occurrences)" % (last[category], last["count"]),
    "%s (%d occurrences)" % (total[category], last["count"]))))

# %% [markdown]
# # Basic profiling of booking-related data
# 

# %%
display(Markdown('\n print the most occuring entries of the whole booking group in descending order'))
data_df.groupBy(booking).count().sort(desc('count')).show(10)

display(Markdown('\n print the highest and lowest counts of categorical columns belonging to the booking-related group'))
get_min_max(data_df[booking])

display(Markdown('\n show number of unique categorical-values per column'))
data_df.select([countDistinct(c).alias(c) for c in get_categoricals(data_df[booking])]).show()

display(Markdown('\n print summary statistics of booking-numricals'))
data_df.select(get_numericals(data_df[booking])).summary().show()

# %% [markdown]
# # Basic profiling of timing-related data

# %%
display(Markdown('\n print the most occuring entries of the whole timing group in descending order'))
data_df.groupBy(time).count().sort(desc('count')).show(10)

display(Markdown('\n print the highest and lowest counts of categorical columns belonging to the timing-related group'))
get_min_max(data_df[time])

display(Markdown('\n show number of unique categorical-values per column'))
data_df.select([countDistinct(c).alias(c) for c in get_categoricals(data_df[time])]).show()

display(Markdown('\n print summary statistics of time-numricals'))
data_df.select(get_numericals(data_df[time])).summary().show()

# %% [markdown]
# # Basic profiling of guest-related data

# %%
display(Markdown('\n print the most occuring entries of the whole guest group in descending order'))
data_df.groupBy(guest).count().sort(desc('count')).limit(10).show()

display(Markdown('\n print the highest and lowest counts of categorical columns belonging to the guest-related group'))
get_min_max(data_df[guest])

display(Markdown('\n show number of unique categorical-values per column'))
data_df.select([countDistinct(c).alias(c) for c in get_categoricals(data_df[guest])]).show()

display(Markdown('\n print summary statistics of guest-numricals'))
data_df.select(get_numericals(data_df[guest])).summary().show()

# %% [markdown]
# # Business Question 1: what does the customer-spending mix look like?
# 
# customer_spending is going to be categorized by the colum "average-daily-rate" as follows:
# 
# - "6 something went wrong"               -> adr_pp = (-infinity,0)
# - "5 very low"            -> adr_pp = (0, 28)
# - "4 low"                -> adr_pp = (28,45) 
# - "3 average"                -> adr_pp = (45,62) 
# - "2 high"                   -> adr_pp = (62,85)
# - "1 very high"                       -> adr_pp = (85,+infinity)
# 
# 

# %%
# 1 Let's calculate the avere-daily-rate per guest, (currently per booking).
# for this we need to create a total guests column, that takes into consideration, that children do not count as a "full guest" (also called PAX)
# then we devide the average_daily_rate per PAX and categorize accordingly
# guest = guest + ["customer_spending"]

bq1_df = \
    data_df.withColumn("PAX", (col("adults") + 0.5 * col("children") + 0.2 * col("babies")))\
        .withColumn("adr_pp", (col("average_daily_rate") / col("PAX")))\
        .withColumn("customer_spending", 
            when(col("adr_pp")<=0,
                "6 something went wrong")
            .when((col("adr_pp")>0) & (col("adr_pp")<=28),
                "5 very low")
            .when((col("adr_pp")>28) & (col("adr_pp")<=45),
                "4 low")
            .when((col("adr_pp")>45) & (col("adr_pp")<=62),
                "3 average")
            .when((col("adr_pp")>62) & (col("adr_pp")<=85),
                "2 high")
            .otherwise(
                "1 very high"))

display(Markdown('Print the customer mix according to the new categorization'))
bq1_df.groupBy("customer_spending").count().sort(desc('count')).show(10)

# %% [markdown]
# # Business Question 2: during which month do we get the highest ratio of 3 average bookings

# %%
# we can reuse the df from business question 1 - however there are a few things to do:
# 1 cast dateTypes on the according columns, to create a date column
# 2 Calculate the booking date by subtracting the lead time from the 
# Group the data so it answers the Business Question

# first create a proper month column
bq2_df = \
    bq1_df.\
        withColumn("month",
            when(col("arrival_date_month") == "January", 1)
            .when(col("arrival_date_month") == "February", 2)
            .when(col("arrival_date_month") == "March", 3)
            .when(col("arrival_date_month") == "April", 4)
            .when(col("arrival_date_month") == "May", 5)
            .when(col("arrival_date_month") == "June", 6)
            .when(col("arrival_date_month") == "July", 7)
            .when(col("arrival_date_month") == "August", 8)
            .when(col("arrival_date_month") == "September", 9)
            .when(col("arrival_date_month") == "October", 10)
            .when(col("arrival_date_month") == "November", 11)
            .when(col("arrival_date_month") == "December", 12))

# next we can create a Date-colum "booking_date".
# finally, we can calculate the booking date, by subtracting the lead_time from the booking_date 
    # expr() is needed in order to pass the column to date_add    
# %%
bq2_df = \
    bq2_df.withColumn("arrival_date", 
            date_format(concat_ws('-', bq2_df.arrival_date_year, bq2_df.month , bq2_df.arrival_date_day_of_month), 'yyyy-MM-dd'))\

# %%       
bq2_df = \
    bq2_df.withColumn("booking_date", 
        expr("date_add(to_date(arrival_date,'yyyy-MM-dd'),-cast(lead_time as int))"))                

#%%
# join dataframe with day_of_week dataframe
bq2_df = \
    bq2_df.join(days_of_week, 
        bq2_df["arrival_date"] == days_of_week["date"], 
        how = 'left')

# %%
# add dummy variables for customer_spending
bq2_df = \
    bq2_df.withColumn("very_high", 
            when(col("customer_spending") == "1 very high", 1).otherwise(0))\
        .withColumn("high", 
            when(col("customer_spending") == "2 high", 1).otherwise(0))\
        .withColumn("average", 
            when(col("customer_spending") == "3 average", 1).otherwise(0))\
        .withColumn("low", 
            when(col("customer_spending") == "4 low", 1).otherwise(0))\
        .withColumn("very_low", 
            when(col("customer_spending") == "5 very low", 1).otherwise(0))\
        .withColumn("something_went_wrong", 
            when(col("customer_spending") == "6 something went wrong", 1).otherwise(0))\


# %%
# with the dataframe created above, we can finally go about answering the business question
# calculate ratio of "1 very high" per month
ba2_df = \
    bq2_df.groupBy(month("booking_date"))\
    .sum("very_high", "high", "average", "low", "very_low", "something_went_wrong")

display(Markdown("printing the ratio of customer-spending 'very high'"))
ba2_df = \
    ba2_df.withColumn(
            "ratio", round((
                col("sum(very_high)") / 
                (col("sum(very_high)") + col("sum(high)") + col("sum(average)") + col("sum(low)") + col("sum(very_low)") + col("sum(something_went_wrong)")))
                ,2))\
        .sort(desc("ratio")).show()

# %% [markdown]
# # Business Question 3: what are the customer_spending ratios per weekday, for each of the two hotels

# %%
display(Markdown("printing the customer-spending mix per country"))
bq3_df = \
    bq2_df.groupBy("hotel", "day_of_week")\
        .pivot("customer_spending")\
        .agg(count("customer_spending"))\
        .orderBy(
            col("1 very high").desc(),
            col("2 high").desc(), 
            col("3 average").desc(), 
            col("4 low").desc(), 
            col("5 very low").desc())\
        .show()


# %%
#%%

display(Markdown("**customer-spending mix per country"))
bq2_df.groupBy("country")\
    .agg(round(avg("very_high"),2).alias("average_high"),
        round(min("adr_pp"),2).alias("adr_pp_min"),
        round(max("adr_pp"),2).alias("adr_pp_max"),
        round(stddev("very_high"),2).alias("stddev_high"))\
    .orderBy(
        col("average_high").desc())\
    .where((col("stddev_high") != 0) & (col("stddev_high").isNotNull()) & (col("stddev_high") != "NaN"))\
    .show()
#%%

"""select *, count()
from table
groupby day_of_week"""
# %%
bq2_df.groupBy("day_of_week").count().show()

