# AWS Fake Data Pipeline Personal Project

This is a data pipeline that uses various AWS services (S3, Glue, etc.) that makes it possible to analyze mock data using AWS Athena, a service that uses SQL to interact with files stored in AWS S3 buckets.

Here's how it works:

1. The data is generated using a fake data library as Parquet files and is uploaded into a raw data folder inside an S3 bucket.

2. After the files are uploaded, a Lambda function is invoked. This function checks that all files are confirmed to be uploaded in the folder for the pipeline to continue processing data.
   
3. If the Lambda function confirms that all files are uploaded into the folder, an AWS Glue job is triggered, which does the basic task of dropping all null rows using very basic SQL by selecting rows that do not contain any null values. The pipeline waits for the Glue job to finish before moving on. The Glue job generates the cleaned files as Parquet files inside a new "cleaned" folder.

4. Once the Glue job is finished, the pipeline renames all of the files to appropriate names based on the first column of the Parquet files. In this case, if the first column is named "product_id", then the corresponding file is renamed to "products.parquet". It also places each file in separate folders which is necessary for the final step of the pipeline.

5. Finally, a Glue crawler is activated which crawls through each folder and generates a table containing the schema of the table. This Glue table can be used for AWS Athena, a SQL tool that can interact with S3 files, for data analysis.

All of these tasks were orchestrated with Prefect.