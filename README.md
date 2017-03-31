# Large-Scale-Entity-Analysis

This repository was developed for analysing large scale datasets which contains entities.

Dataset must be in this form:

ID(long)\tUsername(String)\tDate\tEntities(Separated by ";" and ":" for score FEL)\tSentiment(pos neg)

example:
01234567891234136	fa2fd3jtga	Mon Jan 28 16:19:29 +0000 2013	tyga:Tyga:-1.2792934088573558;happy birthday:Happy_Birthday_to_You:-1.6150737334466199;you:You:-1.913389349273529;	2 -1

In order to run the code you must have installed Apache Spark Ecosystem.

