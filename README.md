# First Name statistics using Hadoop

Each of the following packages contains a Map class, a Reduce class and a Main class to run the MapReduce Job to compute a statistic.

## task1
Counts the number of first names for each origin:

`hadoop jar FirstNamesStats-1.0.jar task1.CountFirstNameByOrigin path/to/firstnames_file path/to/output_dir`

## task2
Counts the number of first names for each origin count:

`hadoop jar FirstNamesStats-1.0.jar task2.CountNamesByNbOrigins path/to/firstnames_file path/to/output_dir`

## task3
Computes the proportion of male and female first names:

`hadoop jar FirstNamesStats-1.0.jar task3.CountProportionGender path/to/firstnames_file path/to/output_dir`



