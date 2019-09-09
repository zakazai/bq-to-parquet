## BigQuery to Parquet via Avro

### Preparation
Create a BigQuery table by uploading `sample.json` with these configurations : 
- Create table from = `Upload`
- File format = `JSON (Newline delimited)`
- Schema =  `Auto detect`
- Partitioning = `Partition by ingestion time`

Then update the parameter at `BigQueryToParquetTest.java`

### Run
```
./gradlew test
```

Change the implementation of BigQueryToAvro at `BigQueryUtil.java` line 25. Use `BigQueryAvroUtils` (default from Beam) or `BigQueryAvroUtilsV2` (modified from Beam)

If you use default implementation for Beam, you'll get this exception
```
org.apache.avro.UnresolvedUnionException: Not in union ["null",{"type":"record","name":"record","namespace":"Translated Avro Schema for record","doc":"org.apache.beam.sdk.io.gcp.bigquery","fields":[{"name":"key_2","type":["null","string"]},{"name":"key_1","type":["null","double"]}]}]: {"key_2": "asdasd", "key_1": 123123.123}
```

Now run the test again, but use `BigQueryAvroUtilsV2`

### Check
The result will be available at this folder under `result` folder.
To check the result, run
```
$ parquet-tools schema result/output-00000-of-00001
$ parquet-tools cat result/output-00000-of-00001
```
