# Consumer Complaints Challenge 

Refer to [Consumer Complaints](https://github.com/InsightDataScience/consumer_complaints) challenge from [InsightDataScience](https://github.com/InsightDataScience/) for more details. 

The goal of this notebook is to solve the challenge using Apache Spark. 

The most important sections are **Input dataset** and **Expected output**, which are quoted below:

## Input dataset

Below are the contents of an example `complaints.csv` file: 
```
Date received,Product,Sub-product,Issue,Sub-issue,Consumer complaint narrative,Company public response,Company,State,ZIP code,Tags,Consumer consent provided?,Submitted via,Date sent to company,Company response to consumer,Timely response?,Consumer disputed?,Complaint ID
2019-09-24,Debt collection,I do not know,Attempts to collect debt not owed,Debt is not yours,"transworld systems inc. is trying to collect a debt that is not mine, not owed and is inaccurate.",,TRANSWORLD SYSTEMS INC,FL,335XX,,Consent provided,Web,2019-09-24,Closed with explanation,Yes,N/A,3384392
```
Each line of the input file, except for the first-line header, represents one complaint. Consult the [Consumer Finance Protection Bureau's technical documentation](https://cfpb.github.io/api/ccdb/fields.html) for a description of each field.  

For this challenge, we want for each product and year that complaints were received, the total number of complaints, number of companies receiving a complaint and the highest percentage of complaints directed at a single company.

## Expected output

Each line in the output file should list the following fields in the following order:
* product (name should be written in all lowercase)
* year
* total number of complaints received for that product and year
* total number of companies receiving at least one complaint for that product and year
* highest percentage (rounded to the nearest whole number) of total complaints filed against one company for that product and year. Use standard rounding conventions (i.e., Any percentage between 0.5% and 1%, inclusive, should round to 1% and anything less than 0.5% should round to 0%)

## Objectives

1. In Task 1, we work on a solution with PySpark on Google Colab using a sample of the data. The data is available on Google Drive and is to be downloaded by the `gdown` command in Task 1.

2. In Task 2, we create a standalone Python script that work on the full dataset using GCP DataProc. The full dataset is downloaded from [here](https://www.consumerfinance.gov/data-research/consumer-complaints/#download-the-data). The data is available on the class bucket as: `gs://bdma/data/complaints.csv`