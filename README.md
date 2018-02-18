# Multi-aspect Entity-centric Analysis of Big Social Media Archives

Social media archives serve as important historical information sources, and thus meaningful analysis and exploration methods are of immense value for historians, sociologists and other interesting parties. This **Apache Spark** library contains functions for computing several entity-related measures given an annotated (with *time*, *entities* and *sentiments*) collection of short texts.  
Currently, the library supports the distributed computation of the following measures: 
- **entity popularity** (how much discussion it generates)
- **entity attitude** (predominant sentiment)
- **entity sentimentality** (magnituted of sentiment)
- **entity controversiality** (whether there is a consensus about the sentiment of the entity)
- **entity-to-entity connectedness** (how connected - in terms of co-occurrences - is the entity to another entity)
- **entity k-network** (top-k strongly connected entities)
- **entity k-positive-network** (top-k positive connected entities)
- **entity k-negative-network** (top-k negative connected entities)

- **entity high attitude** (top-n periods of highest predominant sentiment for a specified interval)
- **entity low attitude**  (top-n periods of lowest predominant sentiment for a specified interval)
- **entity high popularity** (top-n periods of highest score of discussion for a specified interval)
- **entity low popularity**  (top-n periods of lowest  score of discussion for a specified interval)
- **entity high controversiality** (top-n periods of highest score of controversiality for a specified interval)
- **entity low controversiality**  (top-n periods of lowest  score of controversiality for a specified interval)



Dataset must be in the following tab-seperated ("\t") format (each line describes information about a short text, e.g., tweet):

- ID(long)
- USER(string)
- POST_DATE (format "EEE MMM dd HH:mm:ss Z yyyy")
- ENTITIES_IN_TEXT (surface_form:entity_id:confidence_score - separated by ";")
- SENTIMENT_OF_TEXT(pos neg)

Example:
01234567891234136 fa2fd3jtga  Mon Jan 28 16:19:29 +0000 2013  tyga:Tyga:-1.2792934088573558;happy birthday:Happy_Birthday_to_You:-1.6150737334466199	2 -1

In order to run the code you must have installed Apache Spark Ecosystem.

Datasets must be indexed monthly on your File System i.e 2010-01, 2010-02.

Two packages have been created for the aforementioned scores. 

**Single Entity Measures** which contains the methods:
- **entity popularity**  
- **entity attitude**  
- **entity sentimentality**  
- **entity controversiality**  
- **entity-to-entity connectedness** 
- **entity k-network** 
- **entity k-positive-network** 
- **entity k-negative-network**  

**Entity Time Measures** which contains the methods:
- **entity high attitude**  
- **entity low attitude**   
- **entity high popularity**  
- **entity low popularity**   
- **entity high controversiality** 
- **entity low controversiality**   

First package is **Single Entity Measures** which containes two classes. **SingleEntityMeasures** and **ListEntityMeasures**.

**SingleEntityMeasures** class has these three options:

1) First option will calculate the popularity, attitude, sentimentality and controversiality
2) Second option will calculate the connectedness between 2 entities
3) Third otpion will calculate the Top-K, Top-K Positive and Top-K Negative Networks.

The arguments for each option are listed below:

1) date_range_1 date_range_2 1 entity_id sentiment_delta_threshold_for_controversiality(double) directory_of_dataset

ie.
spark-submit --class SingleEntityMeasures.SingleEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 1 Alexis_Tsipras 2.0 TPDL_Dataset/

2) date_range_1 date_range_2 2 main_entity_id other_entity_id directory_of_dataset

ie.
spark-submit --class SingleEntityMeasures.SingleEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 2 Alexis_Tsipras Greek_withdrawal_from_the_eurozone TPDL_Dataset/

3) date_range_1 date_range_2 3 entity_id delta sentiment_delta_threshold_for_controversiality(double) top_k directory_of_dataset

ie.
spark-submit --class SingleEntityMeasures.SingleEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 3 Barack_Obama 0.001 10 TPDL_Dataset/

**ListEntityMeasures**
This class produces the below scores for a **list of entities** instead of a single entity like in **SingleEntityMeasures**.
- popularity score
- attitude score
- sentimentality score
- controversiality score
- Top-K Network
- Top-K Positive Network
- Top-K Negative Network

The arguments are listed below:

1) date_range_1 date_range_2 "text file which contains list of entities ('\n' separated) " sentiment_delta_threshold_for_controversiality(double) top_k directory_of_dataset

ie.
spark-submit --class SingleEntityMeasures.ListEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 entitiesfile.txt 0.001 10 TPDL_Dataset/


The second package **Entity Time Measures** contains 6 classes:
- HighAttitude
- LowAttitude
- HighPopularity
- LowPopularity
- HighControversiality
- LowControversialty

Classes HighAttitude, LowAttitude, HighPopularity, LowPopularity receive 5 arguments while HighControversiality and HighControversiality receive one more (which is the delta threshold). These classes aggregate the top n periods of a specified interval.

The arguments for HighAttitude, LowAttitude, HighPopularity, LowPopularity are listed below:

1) entity_id top_n date_range_1 date_range_2 granularity(in days!) directory_of_dataset 

ie. 
spark-submit --class EntityTimeMeasures.HighAttitude TPDLscala.jar Barack_Obama 30 2015-10-01 2015-11-04 TPDL_Dataset/

This command will calculate the High Attidue scores of entity "Barack_Obama" from 2015-10-01 till 2015-11-04 by spliting this interval every 30 days and afterwards it will return the highest top_n values (including the time interval of these scores). 

HighControversiality and HighControversiality take the same arguments but also they receive a delta threshold for controversiality. 

2) entity_id top_n date_range_1 date_range_2 granularity(in days!) sentiment_delta_threshold_for_controversiality(double)  directory_of_dataset 

ie. 
spark-submit --class EntityTimeMeasures.HighAttitude TPDLscala.jar Barack_Obama 30 2015-10-01 2015-11-04 0.001 TPDL_Dataset/


