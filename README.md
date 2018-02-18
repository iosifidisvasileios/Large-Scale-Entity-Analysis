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

Programm has 3 options:

1) First option will calculate the popularity, attitude, sentimentality and controversiality
2) Second option will calculate the connectedness between 2 entities
3) Third otpion will calculate the Top-K Network.

The arguments for each option are listed below:

1) date_range_1 date_range_2 1 entity_id sentiment_delta_threshold_for_controversiality(double) directory_of_dataset

ie.
spark-submit --class MeasureAggregators.SingleEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 1 Alexis_Tsipras 2.0 TPDL_Dataset/

2) date_range_1 date_range_2 2 main_entity_id other_entity_id directory_of_dataset

ie.
spark-submit --class MeasureAggregators.SingleEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 2 Alexis_Tsipras Greek_withdrawal_from_the_eurozone TPDL_Dataset/

3) date_range_1 date_range_2 3 entity_id delta top_k directory_of_dataset

ie.
spark-submit --class MeasureAggregators.SingleEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 3 Barack_Obama 0.001 10 TPDL_Dataset/
