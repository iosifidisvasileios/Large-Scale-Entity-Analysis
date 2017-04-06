# Large-Scale-Entity-Analysis

This repository was developed for analysing large scale datasets which contain entities.

Dataset must be in this form:

ID(long)\tUsername(String)\tDate\tEntities(Separated by ";" and ":" for score FEL)\tSentiment(pos neg)

example:
01234567891234136	fa2fd3jtga	Mon Jan 28 16:19:29 +0000 2013	tyga:Tyga:-1.2792934088573558;happy birthday:Happy_Birthday_to_You:-1.6150737334466199;you:You:-1.913389349273529;	2 -1

In order to run the code you must have installed Apache Spark Ecosystem.

Datasets must be indexed monthly on your File System i.e 2010-01, 2010-02.

Programm has 3 options:

1) First option will calculate the popularity, attitude, sentimentality and controversiality
2) Second option will calculate the connectedness between 2 entities
3) Third otpion will calculate the Top-K Network, Eplus and Eminus Top-K Networks.

The arguments for each option are listed below:

1) date_range_1 date_range_2 option entity_name_1 delta_threshold percentageForControversiality directory_of_dataset

ie.
spark-submit --class MeasureAggregators.SingleEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 1 Alexis_Tsipras 10 0.1 TPDL_Dataset/

2) date_range_1 date_range_2 option entity_name_1 entity_name_2 directory_of_dataset

ie.
spark-submit --class MeasureAggregators.SingleEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 2 Alexis_Tsipras Greek_withdrawal_from_the_eurozone TPDL_Dataset/

3) date_range_1 date_range_2 option entity_name_1 delta_threshold top_k

ie.
spark-submit --class MeasureAggregators.SingleEntityMeasures TPDLscala.jar 2015-10-01 2015-11-04 3 Barack_Obama 2 10
