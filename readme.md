### IOSR proj 3

More data can be downloaded from: http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time

Tick *Prezipped File* to get the correct schema

### Running

Spark streaming job, watches for csv files appearing in data folder and then computes the queries.
This is the speed layer part of lambda architecture.

```bash
spark-submit spark_csv_df.py
```

### Things to do
- add more types to the schema
- add more queries as pointed out in the presentation
- add kafka support instead of files
