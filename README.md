# Twitter-Analysis

PART A. MESSAGE LENGTH ANALYSIS
Create a Histogram plot that depicts the distribution of tweet sizes (measured in number of characters) among the Twitter dataset. To make the data more readable, the histogram must aggregate bars in groups of 5 (that is, first bar counts tweets of length 1-5, second bar counts tweets 6-10, and so on) as part of your MapReduce job. Your MapReduce program must compute the histogram bins for a correct solution. Aggregating bins outside MapReduce will deduct marks from the complete grade.


PART B. TIME ANALYSIS (45%)
1.Create a bar plot showing the number of Tweets that were posted each hour of the event. You should aggregate together all the messages emitted at the same hour, regardless of the day the messages were sent (hence, you will have 24 different groups). When checking the correctness of your results, keep in mind the timezone of the 2016 Olympic games, as that should give you base expectations about the prime time when the main activities occurred.

2.For the most popular hour of the games, compute the top 10 hashtags that were emitted during that hour. Hashtags are words contained inside the tweet, starting with the hashcode (#) character. Does that information provide you any hint on the main events/activities that took place at peak time?


PART C. SUPPORT ANALYSIS (20%)
The goal of the final part of the coursework is to compute the 30 athletes that received the highest support, according to the Twitter messages of our dataset. Please note that there might be a bias towards English speaking countries/athletes, because of the methodology used when collecting the dataset.  

In this final section, you will have to use an additional dataset, containing the list of Rio 2016 athletes that obtained a medal, and their discipline. The dataset can be found at:

/data/medalistsrio.csv

The data is saved in Comma(,)-separated-values format, with the first row providing header names. Dataset has been downloaded from https://www.kaggle.com/rio2016/olympic-games/data . The dataset has been cropped to only include medalists in an effort to reduce the computation time it will impose to the cluster. 

You need to match full names, exactly as they are written in the athletes dataset. Do not try to split names and surnames, or look for additional information sources, such as Twitter handles for athletes. 

1.Draw a table with the top 30 athletes in number of mentions across the dataset. For each athlete, include the number of mentions retrieved.  For this question you can sort results and compute the top X outside your MapReduce code.

2.Draw a table with the top 20 sports according to the mentions of olympic athletes captured. For resolving athletes into sports use the medalistsrio secondary dataset. For this question you can sort results and compute the top X outside your MapReduce code.
