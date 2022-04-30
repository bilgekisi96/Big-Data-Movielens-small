# Movielens-small-analysis-with-pythonsql-Modelling-Pyspark
Data analysis for Movielens-Small data was carried out using Apache Spark via Jupyter Notebook.
## Summary
=======

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
  2. To use Apache Spark on Jupyter Notebook, go to (Control Panel -> System and Security -> System -> Advanced System Settings -> Environment Variables). By pressing Edit Path C:\Users\EnesA\anaconda3 The directory where Anaconda is installed is added. C:\Users\EnesA\anaconda3\Scripts The directory of the Scripts folder in the area where anaconda is installed is added.

After all the above installations are completed, it will be possible to start working.

# Task-1 Exploratory Data Analysis

1.  First of all, a new page is opened by saying File>New Notebook on jupyter notebook. 

2. To import the Python libraries to be used, write the `!pip install` command for downloading. Example: `!pip install pyspark ` Example: `!pip install pyspark `

3.Libraries are imported. 

```
import sqlite3,sys                                   
import os.path
import matplotlib.pyplot as plt
import seaborn as sns
```
4.The functions requirement for connection to database and execute to queries

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
5.the command take to table names from Database 
```
query = "SELECT name FROM sqlite_master WHERE type = 'table'"        #Table names in database There are 4 table in database 
result=execute_query(connect,query)
table_names=[]
for i in result:
    print("Table Name:",i)
    table_names.append(i[0])
    
query was succesfully!!
Table Name: ('movies',)
Table Name: ('ratings',)
Table Name: ('links',)
Table Name: ('tags',)
```
```
plt.figure(figsize=(8,8))
nums,labels=[],[]
for i in table_names:                                            #Tables row numbers
    query="SELECT * FROM {cha}".format(cha=i)
    result=execute_query(connect,query) 
    rownum=len([j for j in result])
    print("{table} Table Row Number --> {num} ".format(table=i,num=rownum))
    nums.append(rownum)
    labels.append(i)
#define Seaborn color palette to use
colors = sns.color_palette('pastel')[0:5]
plt.title("TABLES ROW NUMBERS")
plt.pie(nums, labels = labels, colors = colors, autopct='%.0f%%')
plt.show()
query was succesfully!!
movies Table Row Number --> 8570 
query was succesfully!!
ratings Table Row Number --> 100023 
query was succesfully!!
links Table Row Number --> 8570 
query was succesfully!!
tags Table Row Number --> 2488 

```
data:image/png;base64,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

```
4. The command is run to start a session over Spark. To use the data, 
```
spark = SparkSession.builder.appName('MovieLens').getOrCreate()

ratings = spark.read.option("header", "true").csv("ml-25m/ratings.csv")
ratings.show(5)
+------+-------+------+----------+
|userId|movieId|rating| timestamp|
+------+-------+------+----------+
|     1|    296|   5.0|1147880044|
|     1|    306|   3.5|1147868817|
|     1|    307|   5.0|1147868828|
|     1|    665|   5.0|1147878820|
|     1|    899|   3.5|1147868510|
+------+-------+------+----------+
only showing top 5 rows
movies = spark.read.option("header", "true").csv("ml-25m/movies.csv")
movies.show(5)
+-------+--------------------+--------------------+
|movieId|               title|              genres|
+-------+--------------------+--------------------+
|      1|    Toy Story (1995)|Adventure|Animati...|
|      2|      Jumanji (1995)|Adventure|Childre...|
|      3|Grumpier Old Men ...|      Comedy|Romance|
|      4|Waiting to Exhale...|Comedy|Drama|Romance|
|      5|Father of the Bri...|              Comedy|
+-------+--------------------+--------------------+
only showing top 5 rows

```
6. When using sql directly over the session, the "movies" "ratings" variables defined to pull the data are converted into a sql table. Now you can start working on the data.
```
movies.createOrReplaceTempView("Movies")
ratings.createOrReplaceTempView("Ratings")
```
## 1.Question - Write a SQL query to create a dataframe with including userid, movieid, genre and rating

1. In this question, data can be retrieved in 2 different ways. Since data is requested from 2 separate tables, a join is required. First, **Movies** and **Ratings** tables can be linked to each other using inner join. Or tables can be linked to each other in the **where** field as in the 2nd example.
```
dataframe=spark.sql("select r.userId,m.movieId,m.genres,r.rating from Movies m inner join Ratings r on r.movieId=m.movieId").show()
+------+-------+--------------------+------+
|userId|movieId|              genres|rating|
+------+-------+--------------------+------+
|     1|    296|Comedy|Crime|Dram...|   5.0|
|     1|    306|               Drama|   3.5|
|     1|    307|               Drama|   5.0|
|     1|    665|    Comedy|Drama|War|   5.0|
|     1|    899|Comedy|Musical|Ro...|   3.5|
|     1|   1088|Drama|Musical|Rom...|   4.0|
|     1|   1175|Comedy|Drama|Romance|   3.5|
|     1|   1217|           Drama|War|   3.5|
|     1|   1237|               Drama|   5.0|
|     1|   1250| Adventure|Drama|War|   4.0|
|     1|   1260|Crime|Film-Noir|T...|   3.5|
|     1|   1653|Drama|Sci-Fi|Thri...|   4.0|
|     1|   2011|Adventure|Comedy|...|   2.5|
|     1|   2012|Adventure|Comedy|...|   2.5|
|     1|   2068|Drama|Fantasy|Mys...|   2.5|
|     1|   2161|Adventure|Childre...|   3.5|
|     1|   2351|               Drama|   4.5|
|     1|   2573|       Drama|Musical|   4.0|
|     1|   2632|Adventure|Drama|M...|   5.0|
|     1|   2692|        Action|Crime|   5.0|
+------+-------+--------------------+------+
only showing top 20 rows

dataframe=spark.sql("select r.userId,m.movieId,m.genres,r.rating from Movies m,Ratings r where r.movieId=m.movieId").show()
+------+-------+--------------------+------+
|userId|movieId|              genres|rating|
+------+-------+--------------------+------+
|     1|    296|Comedy|Crime|Dram...|   5.0|
|     1|    306|               Drama|   3.5|
|     1|    307|               Drama|   5.0|
|     1|    665|    Comedy|Drama|War|   5.0|
|     1|    899|Comedy|Musical|Ro...|   3.5|
|     1|   1088|Drama|Musical|Rom...|   4.0|
|     1|   1175|Comedy|Drama|Romance|   3.5|
|     1|   1217|           Drama|War|   3.5|
|     1|   1237|               Drama|   5.0|
|     1|   1250| Adventure|Drama|War|   4.0|
|     1|   1260|Crime|Film-Noir|T...|   3.5|
|     1|   1653|Drama|Sci-Fi|Thri...|   4.0|
|     1|   2011|Adventure|Comedy|...|   2.5|
|     1|   2012|Adventure|Comedy|...|   2.5|
|     1|   2068|Drama|Fantasy|Mys...|   2.5|
|     1|   2161|Adventure|Childre...|   3.5|
|     1|   2351|               Drama|   4.5|
|     1|   2573|       Drama|Musical|   4.0|
|     1|   2632|Adventure|Drama|M...|   5.0|
|     1|   2692|        Action|Crime|   5.0|
+------+-------+--------------------+------+
only showing top 20 rows
```

```
dataframe=spark.sql("select r.userId,m.movieId,m.genres,r.rating from Movies m,Ratings r where r.movieId=m.movieId").show()
```

## 2.Question - Count ratings for each movie, and list top 5 movies with the highest value

```
spark.sql("""select count(r.rating) CountRatings,m.title as MovieName from Movies m 
inner join Ratings r on r.movieId=m.movieId 
group by m.title order by count(r.rating) desc limit 5""").show()
+------------+--------------------+
|CountRatings|           MovieName|
+------------+--------------------+
|       81491| Forrest Gump (1994)|
|       81482|Shawshank Redempt...|
|       79672| Pulp Fiction (1994)|
|       74127|Silence of the La...|
|       72674|  Matrix, The (1999)|
+------------+--------------------+
```
## 3.Question - Find and list top 5 most rated genres
```
sspark.sql("""select r.rating MostRated,m.genres from Movies m 
inner join Ratings r on r.movieId=m.movieId  
order by r.rating desc limit 5""").show()

+---------+--------------------+
|MostRated|              genres|
+---------+--------------------+
|      5.0|Adventure|Drama|S...|
|      5.0|      Comedy|Romance|
|      5.0|      Horror|Mystery|
|      5.0|              Horror|
|      5.0|Drama|Horror|Thri...|
+---------+--------------------+

```

## 4.Question - By using timestamp from ratings table, provide top 5 most frequent users within a week
In this question, we convert timestamp to date for use.
```
spark.sql("""select count(*) Frequentusers,userId from Movies m 
inner join Ratings r on r.movieId=m.movieId 
where CAST(TO_DATE(FROM_UNIXTIME(r.timestamp))as date)>CAST(TO_DATE(FROM_UNIXTIME(r.timestamp))as date) - interval '1' week 
group by userId 
order by count(*) desc   limit 5""").show()
```
## 5.Question -Calculate average ratings for each genre, and plot average ratings of top 10 genres with descending order

If anyone uses the seaborn library with this data, has to convert to Pandas.
```
table= spark.sql("""

select m.genres,avg(r.rating) as AvgOfRatings  from movies m
inner join Ratings r on r.movieId=m.movieId
group by m.genres order by AvgOfRatings desc limit 10""")

table.show()
plot=table.toPandas()
+--------------------+------------+
|              genres|AvgOfRatings|
+--------------------+------------+
|Comedy|Crime|Dram...|         5.0|
|Action|Drama|Myst...|         5.0|
|Adventure|Drama|F...|         5.0|
|Children|Comedy|D...|         5.0|
|Fantasy|Horror|Ro...|         5.0|
|Adventure|Fantasy...|         5.0|
|Action|Comedy|Mys...|         5.0|
|Adventure|Drama|R...|         5.0|
|Animation|Crime|M...|       4.625|
|Action|Children|D...|         4.5|
+--------------------+------------+

```
```
sns.barplot(x="AvgOfRatings",y = plot.genres,data=plot)
```

# Task-2 Recommender Design Model

Here, the previously developed Lightfm library will be used. Since there is ready-made movielens-small database information in the Lightfm library, data can be drawn directly thanks to the library.


```
import numpy as np

from lightfm.datasets import fetch_movielens

movielens = fetch_movielens()

```
This gives us a dictionary with the following fields:
```
for key, value in movielens.items():
    print(key, type(value), value.shape)
    
('test', <class 'scipy.sparse.coo.coo_matrix'>, (943, 1682))
('item_features', <class 'scipy.sparse.csr.csr_matrix'>, (1682, 1682))
('train', <class 'scipy.sparse.coo.coo_matrix'>, (943, 1682))
('item_labels', <type 'numpy.ndarray'>, (1682,))
('item_feature_labels', <type 'numpy.ndarray'>, (1682,))

train = movielens['train']
test = movielens['test']
```
The train and test elements are the most important: they contain the raw rating data, split into a train and a test set. Each row represents a user, and each column an item. Entries are ratings from 1 to 5.

## Fitting models
Now let's train a BPR model and look at its accuracy.

We'll use two metrics of accuracy: precision@k and ROC AUC. Both are ranking metrics: to compute them, we'll be constructing recommendation lists for all of our users, and checking the ranking of known positive movies. For precision at k we'll be looking at whether they are within the first k results on the list; for AUC, we'll be calculating the probability that any known positive is higher on the list than a random negative example.
```
from lightfm import LightFM
from lightfm.evaluation import precision_at_k
from lightfm.evaluation import auc_score

model = LightFM(learning_rate=0.05, loss='bpr')
model.fit(train, epochs=10)

train_precision = precision_at_k(model, train, k=10).mean()
test_precision = precision_at_k(model, test, k=10).mean()

train_auc = auc_score(model, train).mean()
test_auc = auc_score(model, test).mean()

print('Precision: train %.2f, test %.2f.' % (train_precision, test_precision))
print('AUC: train %.2f, test %.2f.' % (train_auc, test_auc))
Precision: train 0.59, test 0.10.
AUC: train 0.90, test 0.86.

```

The WARP model, on the other hand, optimises for precision@k---we should expect its performance to be better on precision.
```
model = LightFM(learning_rate=0.05, loss='warp')

model.fit_partial(train, epochs=10)

train_precision = precision_at_k(model, train, k=10).mean()
test_precision = precision_at_k(model, test, k=10).mean()

train_auc = auc_score(model, train).mean()
test_auc = auc_score(model, test).mean()

print('Precision: train %.2f, test %.2f.' % (train_precision, test_precision))
print('AUC: train %.2f, test %.2f.' % (train_auc, test_auc))
```
Modelling result is :
```
Precision: train 0.61, test 0.11.
AUC: train 0.93, test 0.90.

```
# Task – 3 Text Analysis:

It also means sentiment analysis.
In order to perform this analysis, the following libraries must be imported.
If there are no libraries, the libraries are installed with the `!pip install` command as shown earlier. It is then imported. If the latest version of Visual Studio Code ++ is not installed on your PC, you will receive an error in the installation of some libraries.
The path to be followed for installation can be followed from the page https://docs.microsoft.com/tr-tr/cpp/build/vscpp-step-0-installation?view=msvc-170.

## Importing libraries

```
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import nltk
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelBinarizer
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from wordcloud import WordCloud,STOPWORDS
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize,sent_tokenize
from bs4 import BeautifulSoup
import spacy
import re,string,unicodedata
from nltk.tokenize.toktok import ToktokTokenizer
from nltk.stem import LancasterStemmer,WordNetLemmatizer
from sklearn.linear_model import LogisticRegression,SGDClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.svm import SVC
from textblob import TextBlob
from textblob import Word
from sklearn.metrics import classification_report,confusion_matrix,accuracy_score
import os
import warnings
```
The nltk library is an important tool in this analysis.
nltk: Natural Language Toolkit; It is an open-source library developed with the Python programming language to work with human language data and built with over 50 corpus and lexical resources under development.
The following command is run for this kit that needs to be installed extra on the PC.
```
import nltk
nltk.download() # it will open pop-up for installing nltk
showing info https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/index.xml 
```
The process is completed by installing the setup that falls on the desktop.

## Importing Data and Data Analysis
The IMDB Dataset file is placed in the folder where Jupyter Notebook receives data. Then the following command is run and the data is retrieved.
```
imdb_data=pd.read_csv('IMDB Dataset.csv')
print(imdb_data.shape)
imdb_data.head(10)
review	sentiment
0	One of the other reviewers has mentioned that ...	positive
1	A wonderful little production. <br /><br />The...	positive
2	I thought this was a wonderful way to spend ti...	positive
3	Basically there's a family where a little boy ...	negative
4	Petter Mattei's "Love in the Time of Money" is...	positive
5	Probably my all-time favorite movie, a story o...	positive
6	I sure would like to see a resurrection of a u...	positive
7	This show was an amazing, fresh & innovative i...	negative
8	Encouraged by the positive comments about this...	negative
9	If you like original gut wrenching laughter yo...	positive
```
## Data Describe and Sentiment Count (Control) 
```
imdb_data.describe()
imdb_data['sentiment'].value.counts()
```
## Text normalization
Words are tokenized. To separate a statement into words, we utilise the word tokenize () method.
```
tokenizer=ToktokTokenizer()
stopword_list=nltk.corpus.stopwords.words('english')
```

## Removing the html strips
```
def strip_html(text):
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text()
#Removing the square brackets
def remove_between_square_brackets(text):
    return re.sub('\[[^]]*\]', '', text)
#Removing the noisy text
def denoise_text(text):
    text = strip_html(text)
    text = remove_between_square_brackets(text)
    return text
#Apply function on review column
imdb_data['review']=imdb_data['review'].apply(denoise_text)
```
## Removing special characters
Since English evaluations are used in the dataset, it is necessary to ensure that special characters are deleted.
```
#Define function for removing special characters
def remove_special_characters(text, remove_digits=True):
    pattern=r'[^a-zA-z0-9\s]'
    text=re.sub(pattern,'',text)
    return text
#Apply function on review column
imdb_data['review']=imdb_data['review'].apply(remove_special_characters)
```

## Removing stopwords and normalization
Stop words are words that have little or no meaning, especially when synthesising meaningful aspects from the text.
Stop words are words that are filtered out of natural language data (text) before or after it is processed in computers. While “stop words” usually refers to a language’s most common terms, all-natural language processing algorithms don’t employ a single universal list.
Stopwords include words such as a, an, the, and others.
```
#set stopwords to english
stop=set(stopwords.words('english'))
print(stop)

#removing the stopwords
def remove_stopwords(text, is_lower_case=False):
    tokens = tokenizer.tokenize(text)
    tokens = [token.strip() for token in tokens]
    if is_lower_case:
        filtered_tokens = [token for token in tokens if token not in stopword_list]
    else:
        filtered_tokens = [token for token in tokens if token.lower() not in stopword_list]
    filtered_text = ' '.join(filtered_tokens)    
    return filtered_text
#Apply function on review column
imdb_data['review']=imdb_data['review'].apply(remove_stopwords)
{'ours', 'he', 'our', 'am', 'aren', 'each', 'yourselves', 'o', 'all', 'any', 'what', 'during', 'as', 'those', "you've", 'on', "shan't", "that'll", 'these', 'yours', 'hasn', 'who', 'of', 'doing', 'did', 'for', 'against', 'why', 'few', 'its', 's', 'will', 'weren', 'very', 'whom', 'can', 'further', 'she', 'because', 'me', 'be', "couldn't", 'than', 'being', 'down', 'should', 'isn', 'needn', 'over', "didn't", 'ain', 'had', 'd', 'again', 'some', 'been', 'where', 'm', "hadn't", "won't", 'more', "mustn't", 'once', 'does', 'and', 'under', 'myself', 'so', "it's", 'hadn', 'mustn', 'the', "shouldn't", 'their', 'while', 'were', 'himself', 'out', 'both', 'own', 'll', 'there', 'don', 'in', 'with', 'into', 'but', "aren't", 'just', 'this', 'yourself', 'here', 'nor', 'too', 'no', 'how', 'hers', 'below', 'when', 'before', 'are', 'after', "wasn't", 'couldn', 'do', 'such', 'ourselves', "wouldn't", 've', 'now', 'shan', 'itself', 'theirs', 'most', 'her', "you'd", 't', 'other', "you'll", 'y', 'won', 'your', 'having', "weren't", 'we', 'was', 'an', 'above', 'that', 'from', 'up', 'about', 'ma', 'same', "don't", "needn't", 'to', 'haven', 'not', 're', 'they', "isn't", 'off', 'themselves', 'at', 'didn', 'my', 'have', 'herself', 'a', 'by', 'or', "mightn't", 'has', 'it', 'you', "she's", 'wouldn', 'then', 'between', "you're", 'his', "haven't", 'until', "hasn't", 'through', 'mightn', 'shouldn', 'is', "should've", 'wasn', 'i', "doesn't", 'doesn', 'him', 'which', 'if', 'only', 'them'}
```
