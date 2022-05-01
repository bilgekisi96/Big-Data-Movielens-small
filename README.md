# Movielens-small Data analysis with Python 


Data analysis for Movielens-Small data was carried out using Apache Spark via Jupyter Notebook.

## Summary

This dataset (ml-latest-small) describes 5-star rating and free-text tagging activity from [MovieLens](http://movielens.org), a movie recommendation service. It contains 100023 ratings table and 2488 tag table across 8570 movies and 8570 links tables. These data were created by 610 users between March 29, 1996 and September 24, 2018. This dataset was generated on September 26, 2018.

## Setup
 
 ### Pyspark setup
 
 https://spark.apache.org/downloads.html Hadoop version must be 2.7 
  1. Opening C:\spark file in PC
  2. Then we delete the .template extension of the log4j.properties.template file in the conf folder and open it with any text editor, change the log4j.rootCategory=INFO line to log4j.rootCategory=ERROR and save it.
 3. Go to the environment variables of Windows (Control Panel -> System and Security -> System -> Advanced System Settings -> Environment Variables) and create a new variable. Define variable name: “SPARK_HOME” value: “C:\spark”.
Again, Select Path in the environment variables and say edit and add %SPARK_HOME%\bin to Path.
  
  ### Hadoop Setup
  
  1. Download "winutils.exe" for Hadoop
  2. Opening C:\hadoop file in PC Copy winutils.exe to Hadoop File
  3. Go to the C disk and create the C:\tmp\hive directory.
  It opens with the option to run command line as administrator. In the directory where winutils.exe is located, write the command: `chmod -R 777 C:\tmp\hive`
  Finally, write the `spark-shell` command on the command line and check if the installation is complete.
 
 ### Anaconda Download
  1. If it is not installed on your PC, anaconda is installed via the https://www.anaconda.com/products/individual site.

After all the above installations are completed, it will be possible to start working.

# Task-1 Exploratory Data Analysis

1.  First of all, a new page is opened by saying File>New Notebook on jupyter notebook. 

2. To import the Python libraries to be used, write the `!pip install` command for downloading. Example: `!pip install pyspark ` Example: `!pip install pyspark `

### 3.Libraries are imported. 

```
import sqlite3,sys                                   
import os.path
import matplotlib.pyplot as plt
import seaborn as sns
```
### 4.The functions requirements for connection to database and execute to queries

```
def create_db_connection(dbfile):                     #this function is creating to connection with database 
    connection = None
    try:
        connection = sqlite3.connect(dbfile)
        print("Database Connection Successfully!!")
        return connection
    except Error as err:
        print(f"Error Database connection failed !!!!: '{err}'")
```
```

def execute_query(connection,query):                 #this function is execution for queries
    cursor = connection.cursor()
    try:
        quer=cursor.execute(query)
        print("query was succesfully!!")
        connection.commit()
        return quer
    except Error as err:
        print(f"Error query does not work:{err}")
```
```
connect=create_db_connection("movielens-small.db")

Database Connection Successfully!!

```
### 5.The command take to table names from Database 
```
query = "SELECT name FROM sqlite_master WHERE type = 'table'"  #Table names in database There are 4 table in database 
result=execute_query(connect,query)
print("Table Names:")
print(tabulate([i for i in result],tablefmt="grid"))

query was succesfully!!

Table Names:
+---------+
| movies  |
+---------+
| ratings |
+---------+
| links   |
+---------+
| tags    |
+---------+
```
### 6.Investigating tables row numbers in Database  
```
query = "SELECT name FROM sqlite_master WHERE type = 'table'"   #Table names in database There are 4 table in database 
result=execute_query(connect,query)
table_names=[i[0] for i in result]
nums,labels=[],[]
for i in table_names:                                           #Tables row numbers
    query="SELECT * FROM {cha}".format(cha=i)
    result=execute_query(connect,query) 
    rownum=len([j for j in result])
    nums.append(rownum)
    labels.append(i)
print()
print("Tables Row Numbers")
print(tabulate([[i for i in labels],[j for j in nums]],tablefmt='grid'))
query was succesfully!!
query was succesfully!!
query was succesfully!!
query was succesfully!!
query was succesfully!!

Tables Row Numbers
+--------+---------+-------+------+
| movies | ratings | links | tags |
+--------+---------+-------+------+
| 8570   | 100023  | 8570  | 2488 |
+--------+---------+-------+------+
    
plt.figure(figsize=(8,8))
#define Seaborn color palette to use
colors = sns.color_palette('pastel')[0:5]
plt.title("TABLES ROW NUMBERS")
plt.pie(nums, labels = labels, colors = colors, autopct='%.0f%%')
plt.show()

```
![alt text](https://github.com/bilgekisi96/Big-Data-Movielens-small/blob/main/indir.png)

```

```
### 7.Checking null values in tables using list comprehansion method for check to rows and values
```
for i in table_names:                                              #Check to Null values in Tables 
    query="SELECT * FROM {cha}".format(cha=i)
    result=execute_query(connect,query) 
    print(tabulate([["There are Null" if False in [False if (j=="NULL" or j==None) else True for j in result for k in j] else "There are not NULL value in table"]],tablefmt='grid'))
    
query was succesfully!!
+-----------------------------------+
| There are not NULL value in table |
+-----------------------------------+
query was succesfully!!
+-----------------------------------+
| There are not NULL value in table |
+-----------------------------------+
query was succesfully!!
+-----------------------------------+
| There are not NULL value in table |
+-----------------------------------+
query was succesfully!!
+-----------------------------------+
| There are not NULL value in table |
+-----------------------------------+
```

### 8.Looking unique values in tables 
```
keys,values = [],[]
for i in table_names:                                                                             
    query="SELECT COUNT(DISTINCT movieId) FROM {cha}".format(cha=i)
    result=execute_query(connect,query)                                                     #Checking unique movieIDs in tables
    result=tuple(result)[0][0]
    values.append(i)
    keys.append(result)
print()
print("Number of unique values for each table")
print(tabulate([values,keys],tablefmt="grid"))                                                                                                
query was succesfully!!
query was succesfully!!
query was succesfully!!
query was succesfully!!

Number of unique values for each table
+--------+---------+-------+------+
| movies | ratings | links | tags |
+--------+---------+-------+------+
| 8570   | 8552    | 8570  | 672  |
+--------+---------+-------+------+
```

### 9.First join process on movieIds for relational movielens database joined 2 table movies and ratings Also check to row number
```
query = "SELECT COUNT(*) FROM movies b INNER JOIN ratings a ON a.movieId = b.movieId"
result=execute_query(connect,query)                                                    #row number after inner join 
print(tabulate([list(result)[0]],tablefmt="grid"))                                     #I setup relation to tables on movieId's

query was succesfully!!
+--------+
| 100023 |
+--------+
```
### 10.After inner join process checked to column names
```
query = "SELECT * FROM movies b INNER JOIN ratings a ON a.movieId = b.movieId"    #I made INNER JOIN for arrive to genres colunm
result=execute_query(connect,query)                                               #columns names between two tables  
print("Column names after join process")
col_nam=[]
for i in result.description:col_nam.append(i[0])
print(tabulate([col_nam],tablefmt="grid"))
query was succesfully!!

Column names after join process
+---------+-------+------+--------+--------+---------+--------+-----------+
| movieId | title | year | genres | userId | movieId | rating | timestamp |
+---------+-------+------+--------+--------+---------+--------+-----------+

```
```

query = "SELECT a.userId,a.movieId,a.rating,b.genres FROM movies b INNER JOIN ratings a ON a.movieId = b.movieId LIMIT 5"
result=execute_query(connect,query)                                               # userıd movieıd rating and genres dataframe 

for i in result:print(i)

query was succesfully!!
(7, 1, 5.0, 'Adventure|Animation|Children|Comedy|Fantasy')
(10, 1, 4.0, 'Adventure|Animation|Children|Comedy|Fantasy')
(13, 1, 4.5, 'Adventure|Animation|Children|Comedy|Fantasy')
(16, 1, 5.0, 'Adventure|Animation|Children|Comedy|Fantasy')
(21, 1, 5.0, 'Adventure|Animation|Children|Comedy|Fantasy')

```
### Missing values between two table movies and ratings
```
query = "SELECT a.movieId,b.movieId,b.userId,b.rating,b.timestamp FROM movies a LEFT JOIN ratings b ON b.movieId = a.movieId WHERE b.rating IS NULL ; "
result=execute_query(connect,query)                   #There are 18 NULL values between movies and ratings 
print()                                               #This values there are in movies table but there arent in ratings table 
print("Null values between 2 table")
print(tabulate([i for i in result],missingval="None",tablefmt="grid"))

query was succesfully!!

Null values between 2 table
+-------+------+------+------+------+
|   133 | None | None | None | None |
+-------+------+------+------+------+
|   654 | None | None | None | None |
+-------+------+------+------+------+
|  2499 | None | None | None | None |
+-------+------+------+------+------+
|  6328 | None | None | None | None |
+-------+------+------+------+------+
|  7541 | None | None | None | None |
+-------+------+------+------+------+
|  8574 | None | None | None | None |
+-------+------+------+------+------+
|  8765 | None | None | None | None |
+-------+------+------+------+------+
| 31408 | None | None | None | None |
+-------+------+------+------+------+
| 31687 | None | None | None | None |
+-------+------+------+------+------+
| 33340 | None | None | None | None |
+-------+------+------+------+------+
| 49422 | None | None | None | None |
+-------+------+------+------+------+
| 54248 | None | None | None | None |
+-------+------+------+------+------+
| 59549 | None | None | None | None |
+-------+------+------+------+------+
| 60020 | None | None | None | None |
+-------+------+------+------+------+
| 60382 | None | None | None | None |
+-------+------+------+------+------+
| 68486 | None | None | None | None |
+-------+------+------+------+------+
| 70695 | None | None | None | None |
+-------+------+------+------+------+
| 86487 | None | None | None | None |
+-------+------+------+------+------+

```
### 11. Playling with subquaries cleaning and prepared genres column 
```
query = "SELECT movieId,title,year,imdbId,tmdbId,userId,rating,timestamp,SUBSTR(genre,1,INSTR(genre,'|') - 1) as genres FROM (SELECT movieId,title,year,SUBSTR(genres, 1 , 10 ) as genre,imdbId,tmdbId,userId,rating,timestamp FROM (SELECT a.movieId,a.title,a.year,a.genres,b.imdbId,b.tmdbId,c.userId,c.rating,c.timestamp FROM movies a INNER JOIN links b ON a.movieId = b.movieId INNER JOIN ratings c ON b.movieId = c.movieId)) LIMIT 5;"
result=execute_query(connect,query)                                                                 # I look and prepare values

for i in result:print(i) 

query was succesfully!!
(1, 'Toy Story', 1995, '0114709', '862', 7, 5.0, 835583333, 'Adventure')
(1, 'Toy Story', 1995, '0114709', '862', 10, 4.0, 1113487849, 'Adventure')
(1, 'Toy Story', 1995, '0114709', '862', 13, 4.5, 1275864236, 'Adventure')
(1, 'Toy Story', 1995, '0114709', '862', 16, 5.0, 855198755, 'Adventure')
(1, 'Toy Story', 1995, '0114709', '862', 21, 5.0, 865106839, 'Adventure')

```

## QUESTIONS PARTS:


### 1.Question : Write a SQL query to create a dataframe with including userid, movieid, genre and rating
```
query = "SELECT a.userId,a.movieId,a.rating,b.genres FROM movies b INNER JOIN ratings a ON a.movieId = b.movieId INNER JOIN links c ON a.movieId = c.movieId LIMIT 10"
result=execute_query(connect,query)   # userıd movieıd rating and genres dataframe 
res = [list(i[:5]) for i in result]
values =[["userId","movieId","rating","genres"]]+res
print(tabulate(values,tablefmt="grid",headers="firstrow"))

query was succesfully!!
+----------+-----------+----------+---------------------------------------------+
|   userId |   movieId |   rating | genres                                      |
+==========+===========+==========+=============================================+
|        7 |         1 |      5   | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+
|       10 |         1 |      4   | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+
|       13 |         1 |      4.5 | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+
|       16 |         1 |      5   | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+
|       21 |         1 |      5   | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+
|       22 |         1 |      4   | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+
|       27 |         1 |      4   | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+
|       30 |         1 |      5   | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+
|       31 |         1 |      3   | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+
|       36 |         1 |      2.5 | Adventure|Animation|Children|Comedy|Fantasy |
+----------+-----------+----------+---------------------------------------------+

```
### 2.Question : Count ratings for each movie, and list top 5 movies with the highest value
```
query = "SELECT COUNT(rating) as terra,title FROM (SELECT * FROM movies b INNER JOIN ratings a ON a.movieId = b.movieId) GROUP BY title ORDER BY terra DESC LIMIT 5;"
result=execute_query(connect,query)
res = [list(i[:5]) for i in result]
values =[["Count ratings","title"]]+res
print(tabulate(values,tablefmt="grid",headers="firstrow"))

query was succesfully!!
+-----------------+---------------------------+
|   Count ratings | title                     |
+=================+===========================+
|             337 | Silence of the Lambs, The |
+-----------------+---------------------------+
|             328 | Shawshank Redemption, The |
+-----------------+---------------------------+
|             327 | Pulp Fiction              |
+-----------------+---------------------------+
|             324 | Jurassic Park             |
+-----------------+---------------------------+
|             318 | Forrest Gump              |
+-----------------+---------------------------+
```
### 3.Question: Find and list top 5 most rated genres
```
query = "SELECT COUNT(rating) as terra,genres FROM (SELECT * FROM movies b INNER JOIN ratings a ON a.movieId = b.movieId INNER JOIN links c ON a.movieId = c.movieId) GROUP BY genres ORDER BY terra DESC LIMIT 5"
result=execute_query(connect,query)
keys,values=[],[]
res = [list(i[:5]) for i in result]
values =[["rating","genres"]]+res
print(tabulate(values,tablefmt="grid",headers="firstrow"))

query was succesfully!!
+----------+----------------+
|   rating | genres         |
+==========+================+
|     7008 | Drama          |
+----------+----------------+
|     6396 | Comedy         |
+----------+----------------+
|     3877 | Comedy|Romance |
+----------+----------------+
|     3121 | Drama|Romance  |
+----------+----------------+
|     3000 | Comedy|Drama   |
+----------+----------------+
```
```
fig, ax = plt.subplots(figsize=(12,6))

bars = ax.bar(values, keys)
ax.bar_label(bars)
plt.xlabel("Genres")
plt.ylabel("Number of genres")
plt.title(" 5 most rated genres")
plt.show()

```

![alt text](https://github.com/bilgekisi96/Big-Data-Movielens-small/blob/main/indir%20(1).png)

```

```
# Modelling with Pyspark

```
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext                                                  #import to libraries
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import udf,col,when
import numpy as np
from IPython.display import Image
from IPython.display import display
from IPython.display import clear_output
```

```
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("moive analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()                                                                #Evaluating to Spark session
``` 

### DATA ANALYSIS AND EXPLORATION

```
movies_df = spark.read.load("movies.txt", format='csv', header = True)
ratings_df = spark.read.load("ratings.txt", format='csv', header = True)          #Taking tables 
links_df = spark.read.load("links.txt", format='csv', header = True)
tags_df = spark.read.load("tags.txt", format='csv', header = True)
```

```
print(type(movies_df))                                                            #movies table row count
movies_df.count()
<class 'pyspark.sql.dataframe.DataFrame'>

8570

```

```
movies_df.show(5)                                                                  #movies table first 5 rows
movies_df.createOrReplaceTempView("movies_df")
display (spark.sql("SELECT * FROM movies_df limit 5"))

+-------+--------------------+----+--------------------+
|movieId|               title|year|              genres|
+-------+--------------------+----+--------------------+
|      1|           Toy Story|1995|Adventure|Animati...|
|      2|             Jumanji|1995|Adventure|Childre...|
|      3|    Grumpier Old Men|1995|      Comedy|Romance|
|      4|   Waiting to Exhale|1995|Comedy|Drama|Romance|
|      5|Father of the Bri...|1995|              Comedy|
+-------+--------------------+----+--------------------+
only showing top 5 rows

DataFrame[movieId: string, title: string, year: string, genres: string]
```

```
ratings_df.show(5)                                                                 #ratings table for 5 rows
ratings_df.createOrReplaceTempView("ratings_df")
display (spark.sql("SELECT * FROM ratings_df limit 5"))

+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|     1|      6|   2.0|980730861|
|     1|     22|   3.0|980731380|
|     1|     32|   2.0|980731926|
|     1|     50|   5.0|980732037|
|     1|    110|   4.0|980730408|
+------+-------+------+---------+
only showing top 5 rows

DataFrame[userId: string, movieId: string, rating: string, timestamp: string]

```

```
links_df.show(5)
links_df.createOrReplaceTempView("links_df")                                        #links table first 5 rows
display (spark.sql("SELECT * FROM links_df limit 5"))

+-------+-------+------+
|movieId| imdbId|tmdbId|
+-------+-------+------+
|      1|0114709|   862|
|      2|0113497|  8844|
|      3|0113228| 15602|
|      4|0114885| 31357|
|      5|0113041| 11862|
+-------+-------+------+
only showing top 5 rows

DataFrame[movieId: string, imdbId: string, tmdbId: string]

```
```
tmp1 = ratings_df.groupBy("userID").count().toPandas()['count'].min()             
tmp2 = ratings_df.groupBy("movieId").count().toPandas()['count'].min()                 #How many movies rated by count of person

print('For the users that rated movies and the movies that were rated:')
print('Minimum number of ratings per user is {}'.format(tmp1))
print('Minimum number of ratings per movie is {}'.format(tmp2))

For the users that rated movies and the movies that were rated:

Minimum number of ratings per user is 20
Minimum number of ratings per movie is 1

```

```
tmp1 = sum(ratings_df.groupBy("movieId").count().toPandas()['count'] == 1)           #2808 out of 8552 movies are rated only one user
tmp2 = ratings_df.select('movieId').distinct().count()
print('{} out of {} movies are rated by only one user'.format(tmp1, tmp2))

2808 out of 8552 movies are rated by only one user

```

```
sc = spark.sparkContext                                                              #Sql spark Context conn.
sqlContext=SQLContext(sc)
```

```
ratings_df=spark.read.csv("ratings.txt",inferSchema=True,header=True)                #columns value types in ratings table
ratings_df.printSchema()

root
 |-- userId: integer (nullable = true)                                                                     
 |-- movieId: integer (nullable = true)
 |-- rating: double (nullable = true)
 |-- timestamp: integer (nullable = true)

```

```
ratings_df.show()                                                                   #taking ratings table

+------+-------+------+---------+
|userId|movieId|rating|timestamp|
+------+-------+------+---------+
|     1|      6|   2.0|980730861|
|     1|     22|   3.0|980731380|
|     1|     32|   2.0|980731926|
|     1|     50|   5.0|980732037|
|     1|    110|   4.0|980730408|
|     1|    164|   3.0|980731766|
|     1|    198|   3.0|980731282|
|     1|    260|   5.0|980730769|
|     1|    296|   4.0|980731208|
|     1|    303|   3.0|980732235|
|     1|    318|   3.0|980731417|
|     1|    350|   3.0|980731745|
|     1|    366|   2.0|980731621|
|     1|    367|   4.0|980731380|
|     1|    431|   2.0|980731312|
|     1|    432|   2.0|980732235|
|     1|    451|   1.0|980731789|
|     1|    457|   4.0|980730816|
|     1|    474|   3.0|980730816|
|     1|    480|   4.0|980731903|
+------+-------+------+---------+
only showing top 20 rows

```

```
movies_df=spark.read.csv('movies.txt',inferSchema=True,header=True)                    # movies table value types 
movies_df.printSchema

root
 |-- movieId: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- year: string (nullable = true)
 |-- genres: string (nullable = true)

```

```
movies_df.show()

+-------+--------------------+----+--------------------+
|movieId|               title|year|              genres|
+-------+--------------------+----+--------------------+
|      1|           Toy Story|1995|Adventure|Animati...|
|      2|             Jumanji|1995|Adventure|Childre...|
|      3|    Grumpier Old Men|1995|      Comedy|Romance|
|      4|   Waiting to Exhale|1995|Comedy|Drama|Romance|
|      5|Father of the Bri...|1995|              Comedy|
|      6|                Heat|1995|Action|Crime|Thri...|
|      7|             Sabrina|1995|      Comedy|Romance|
|      8|        Tom and Huck|1995|  Adventure|Children|
|      9|        Sudden Death|1995|              Action|
|     10|           GoldenEye|1995|Action|Adventure|...|
|     11|American Presiden...|1995|Comedy|Drama|Romance|
|     12|Dracula: Dead and...|1995|       Comedy|Horror|
|     13|               Balto|1995|Adventure|Animati...|
|     14|               Nixon|1995|               Drama|
|     15|    Cutthroat Island|1995|Action|Adventure|...|
|     16|              Casino|1995|         Crime|Drama|
|     17|Sense and Sensibi...|1995|       Drama|Romance|
|     18|          Four Rooms|1995|              Comedy|
|     19|Ace Ventura: When...|1995|              Comedy|
|     20|         Money Train|1995|Action|Comedy|Cri...|
+-------+--------------------+----+--------------------+
only showing top 20 rows

```
```
links_df=spark.read.csv('links.txt',inferSchema=True,header=True)                    # Links table value types 
links_df.printSchema()
root
 |-- movieId: integer (nullable = true)
 |-- imdbId: integer (nullable = true)
 |-- tmdbId: integer (nullable = true)

```

## DATA TRAIN TEST SPLIT

```
training_df,validation_df = ratings_df.randomSplit([0.8,0.2])                        #split to data for train and test

```

```
iterations=10                                                                        #model parameters choosed best parameters for taking best solutions
regularization_parameter=0.1
rank=4
error=[]
err=0

```

### ALTERNATING LEAST SQUARES RECOMMENDED MODEL


#### Using ALS(Alternating Least squares) recommended regression model for data 
#### Firstly model trained with train data and then model tested any see before with test data After that Take to rmse score by using evaluator

```
als = ALS(maxIter=iterations,regParam=regularization_parameter,rank=5,userCol="userId",itemCol="movieId",ratingCol="rating")
  
model = als.fit(training_df)                                                           #training data trained by als model

predictions = model.transform(validation_df)                                           #and tested model with test data

new_predictions = predictions.filter(col('prediction')!=np.nan)

evaluator = RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction") 
                                                                                       #we look root mean square error for regression model 
rmse = evaluator.evaluate(new_predictions)

print("Root Mean Square Error= "+str(rmse))                                            #taking  RMSE scores 

Root Mean Square Error= 0.9390473765159149
```

#### Taking new scores from mixing data data 
```
for rank in range(4,10):
    als = ALS(maxIter=iterations,regParam=regularization_parameter,rank=rank,userCol="userId",itemCol="movieId",ratingCol="rating")
    model = als.fit(training_df)
    predictions = model.transform(validation_df)
    new_predictions = predictions.filter(col('prediction')!=np.nan)
    evaluator = RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction")
    rmse = evaluator.evaluate(new_predictions)
    print("Root Mean Square Error= "+str(rmse))    
    
Root Mean Square Error= 0.9372678988760661
Root Mean Square Error= 0.9390473765159146
Root Mean Square Error= 0.9388196042742588
Root Mean Square Error= 0.9439639521307659
Root Mean Square Error= 0.9401075667945087
Root Mean Square Error= 0.9419780113792817 

```
#### Taking Predictions by trained model
```
predictions.show(10)
+------+-------+------+---------+----------+
|userId|movieId|rating|timestamp|prediction|
+------+-------+------+---------+----------+
|     1|    858|   5.0|980730742|  3.747875|
|     1|    451|   1.0|980731789|       NaN|
|     1|   1201|   4.0|980730742|  3.290426|
|     1|   1034|   3.0|980731208| 2.7763813|
|     1|     22|   3.0|980731380| 3.1554518|
|     1|   1197|   4.0|980730769| 3.8877766|
|     1|    590|   2.0|980732165|   3.03294|
|     1|    457|   4.0|980730816| 3.7875729|
|     1|    608|   5.0|980732037|  3.138443|
|     1|    996|   2.0|980732235| 2.7168202|
+------+-------+------+---------+----------+
only showing top 10 rows

```
#### Implemented grid search to model with grid search parameters and this model used for every part of cross validation it is great idea for scores 
#### but the results are not changed for data 

```

from pyspark.ml.tuning import *

from pyspark.ml.tuning import ParamGridBuilder
#Grid Search and cross validation for our model  
als1 = ALS(maxIter=iterations,regParam=regularization_parameter,rank=rank,userCol="userId",itemCol="movieId",ratingCol="rating")
paramGrid = ParamGridBuilder()\
.addGrid(als1.regParam,[0.1,0.01,0.18])\
.addGrid(als1.rank,range(4,10))\
.build()

evaluator=RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction")
crossval=CrossValidator(estimator=als1,estimatorParamMaps=paramGrid,evaluator=evaluator,numFolds=5)
cvModel=crossval.fit(training_df)

```
#### Implemented with cv model for taking scores for every part of data 
```

CV_Pred = cvModel.transform(validation_df)
new_prediction = CV_Pred.filter(col('prediction')!=np.nan)

evaluator = RegressionEvaluator(metricName="r2",labelCol="rating",predictionCol="prediction")
evaluator_rmse = RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction")

rsquare = evaluator_rmse.evaluate(new_prediction)
rmse = evaluator.evaluate(new_prediction)
print("r2= "+str(rsquare))             #I took r2 and rmse scores   
print("rmse= "+str(rmse))              #rmse score is perfect but r2 is explane:This data not 
                                       #suitable for prediction but we are working on recommendation staff so it is not important
r2= 0.9372678988760661
rmse= 0.21520010750095808
```

```
CV_Pred.show(10)

+------+-------+------+---------+----------+
|userId|movieId|rating|timestamp|prediction|
+------+-------+------+---------+----------+
|     1|    858|   5.0|980730742|  3.779253|
|     1|    451|   1.0|980731789|       NaN|
|     1|   1201|   4.0|980730742| 3.2785628|
|     1|   1034|   3.0|980731208|  3.267778|
|     1|     22|   3.0|980731380| 3.0015154|
|     1|   1197|   4.0|980730769|  3.917275|
|     1|    590|   2.0|980732165| 3.2255154|
|     1|    457|   4.0|980730816| 3.8312576|
|     1|    608|   5.0|980732037| 3.2846289|
|     1|    996|   2.0|980732235| 2.8194199|
+------+-------+------+---------+----------+
only showing top 10 rows
```
#### Filtered same movieId and taking predictins
```
predictions.join(movies_df,"movieId").select("userId","title","genres","prediction").show(15)

+------+--------------------+--------------------+----------+
|userId|               title|              genres|prediction|
+------+--------------------+--------------------+----------+
|    31|American Tail: Fi...|Adventure|Animati...| 2.1850998|
|    31|Land Before Time,...|Adventure|Animati...| 2.4595604|
|   516|Devil's Advocate,...|Drama|Mystery|Thr...| 2.8890162|
|   516|                  10|      Comedy|Romance| 2.1996143|
|    85|   American Splendor|        Comedy|Drama|  4.577711|
|    53|        Galaxy Quest|Adventure|Comedy|...| 3.1859865|
|   481|Men in Black (a.k...|Action|Comedy|Sci-Fi|  3.894961|
|   633|Men in Black (a.k...|Action|Comedy|Sci-Fi| 2.4802399|
|   597|       Out of Africa|       Drama|Romance|   4.02375|
|   155|Hellbound: Hellra...|              Horror|  3.100603|
|   193|Hudsucker Proxy, The|              Comedy|   4.20068|
|   126|       Dirty Dancing|Drama|Musical|Rom...| 3.7039447|
|   183|Men in Black (a.k...|Action|Comedy|Sci-Fi| 3.3027043|
|   210|               Spawn|Action|Adventure|...|  2.426955|
|   210|Children of the Corn|     Horror|Thriller| 3.1096544|
+------+--------------------+--------------------+----------+
only showing top 15 rows
```
#### Filtered to userId 599 we want to make predict for giving ratings different kind of movie by same user 

```
for_one_user = predictions.filter(col("userId")==599).join(movies_df,"movieId").join(links_df,"movieId").select("userId","title","genres","tmdbId","prediction")
for_one_user.show(5)

+------+--------------------+--------------------+------+----------+
|userId|               title|              genres|tmdbId|prediction|
+------+--------------------+--------------------+------+----------+
|   599|        Strange Days|Action|Crime|Dram...|   281| 3.7138426|
|   599|Clear and Present...|Action|Crime|Dram...|  9331|  3.656493|
|   599|        True Romance|      Crime|Thriller|   319| 4.0647445|
|   599|              Eraser|Action|Drama|Thri...|  9268| 2.8951352|
|   599|    Army of Darkness|Action|Adventure|...|   766|  4.067372|
+------+--------------------+--------------------+------+----------+
only showing top 5 rows
```

```
import webbrowser 
link="https://www.themoviedb.org/movie/"           #I get movie titles
for movie in for_one_user.take(5):
    movieURL=link+str(movie.tmdbId)
    print(movie.title)
    webbrowser.open(movieURL)
Replacement Killers, The
Army of Darkness
Ronin
True Romance
Eraser

```
#### Taking 5 recommendations on model for Users and Items

```
userRecommends=model.recommendForAllUsers(5)
movieRecommends=model.recommendForAllItems(5)

```

```
userRecommends.printSchema()                           #inside of user recommends

root
 |-- userId: integer (nullable = false)
 |-- recommendations: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- movieId: integer (nullable = true)
 |    |    |-- rating: float (nullable = true)

```

```
userRecommends.show(5)
+------+--------------------+
|userId|     recommendations|
+------+--------------------+
|     1|[{2660, 4.57362},...|
|     3|[{31116, 5.746894...|
|     5|[{72605, 5.328123...|
|     6|[{115170, 4.89595...|
|     9|[{1216, 4.986879}...|
+------+--------------------+
only showing top 5 rows

```

#### Selecting movieIds and make recommendations according to userIds
```
userRecommends.select("userId","recommendations.movieId").show(10,False)  
+------+------------------------------------+
|userId|movieId                             |
+------+------------------------------------+
|1     |[5294, 2649, 7088, 260, 1428]       |
|3     |[2649, 1428, 5792, 1283, 1404]      |
|5     |[71379, 3262, 3055, 493, 5613]      |
|6     |[115170, 98243, 106841, 1238, 85056]|
|9     |[8675, 7338, 2649, 83, 678]         |
|12    |[2966, 123, 3161, 745, 53894]       |
|13    |[8675, 7338, 7088, 7070, 106841]    |
|15    |[7940, 1306, 1238, 2295, 3204]      |
|16    |[27873, 3466, 7088, 4703, 4187]     |
|17    |[963, 4675, 2398, 5264, 3784]       |
+------+------------------------------------+
only showing top 10 rows

```

```
movieRecommends.printSchema()
root
 |-- movieId: integer (nullable = false)
 |-- recommendations: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- userId: integer (nullable = true)
 |    |    |-- rating: float (nullable = true)
 
```

#### Make user recommendations for films 
```
movieRecommends.select("movieId","recommendations.userId").show(10,False)

+-------+-------------------------+
|movieId|userId                   |
+-------+-------------------------+
|1      |[241, 268, 530, 444, 691]|
|3      |[95, 452, 163, 124, 612] |
|5      |[241, 530, 133, 479, 118]|
|6      |[201, 530, 82, 306, 112] |
|9      |[452, 675, 265, 612, 133]|
|12     |[504, 675, 220, 480, 452]|
|13     |[241, 268, 530, 133, 691]|
|15     |[480, 504, 366, 249, 452]|
|16     |[241, 530, 181, 334, 201]|
|17     |[241, 334, 530, 181, 268]|
+-------+-------------------------+
only showing top 10 rows

```
#### Taking distinct 5 userId for recommendation
```
users=ratings_df.select("userId").distinct().limit(5)
users.show()
+------+
|userId|
+------+
|   148|
|   463|
|   471|
|   496|
|   243|
+------+
```
####  10 Sample recommended for userId
```
userSubsetRecs = model.recommendForUserSubset(users,10)   
userSubsetRecs.show()

+------+--------------------+
|userId|     recommendations|
+------+--------------------+
|   471|[{27873, 5.133694...|
|   463|[{115170, 5.28641...|
|   243|[{6669, 5.170791}...|
|   496|[{37857, 5.92129}...|
|   148|[{8675, 4.008882}...|
+------+--------------------+
```
#### Taking their movieIds
```
userSubsetRecs.select("userId","recommendations.movieId").show(10,False) #recomendations movieIds for userIDs
                                                                         #end it is done !!!!
+------+-------------------------------------------------------------------+
|userId|movieId                                                            |
+------+-------------------------------------------------------------------+
|471   |[27873, 3466, 2810, 31193, 4187, 7088, 2083, 4703, 4873, 8477]     |
|463   |[115170, 98243, 3598, 1414, 93721, 106696, 2810, 74754, 6645, 8477]|
|243   |[6669, 27846, 2920, 26840, 6783, 6643, 8477, 53906, 5607, 26171]   |
|496   |[37857, 31193, 4275, 4228, 1713, 1934, 2184, 5942, 4777, 531]      |
|148   |[8675, 7338, 26840, 7070, 6669, 26171, 5607, 1104, 3550, 5899]     |
+------+-------------------------------------------------------------------+

```

```
movies=ratings_df.select("movieId").distinct().limit(5)  #movieIds for recommeds Users
movies.show()
+-------+
|movieId|
+-------+
|   1580|
|   1645|
|    471|
|   1088|
|   2142|
+-------+
```

```
movieSubsetRecs = model.recommendForItemSubset(movies,10)                     #Same process repeated for movieId
movieSubsetRecs.select("movieId","recommendations.userId").show(10,False)

+-------+--------------------------------------------------+
|movieId|userId                                            |
+-------+--------------------------------------------------+
|1580   |[241, 530, 265, 133, 452, 444, 268, 289, 124, 217]|
|471    |[164, 185, 691, 289, 201, 111, 158, 538, 625, 458]|
|2142   |[241, 452, 25, 594, 698, 265, 95, 124, 429, 41]   |
|1645   |[452, 280, 124, 612, 241, 95, 265, 133, 217, 479] |
|1088   |[241, 268, 472, 265, 530, 133, 3, 112, 283, 82]   |
+-------+--------------------------------------------------+
```

```
movie_ids=[1580,3175,2366,1590]               #model tried for new users
user_ids=[543,543,543,543]
new_user_preds=sqlContext.createDataFrame(zip(movie_ids,user_ids),schema=['movieId','userId'])
new_predictions=model.transform(new_user_preds)
new_predictions.show()

+-------+------+----------+
|movieId|userId|prediction|
+-------+------+----------+
|   1580|   543|  3.381722|
|   3175|   543| 3.2734551|
|   2366|   543| 2.8578584|
|   1590|   543| 2.5245814|
+-------+------+----------+

```

