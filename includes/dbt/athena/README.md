Welcome to my sample project! 

This dbt project connects to raw Google Ads api response data stored in S3. Amazon Athena is the sql engine that stages the raw file as a sql tables, parses the json objects into a relational star schema in the intermediate data layer, and then produces an artifact table result from a join across ad metrics and ad dimensions tables in the mart data layer. Tests are configured to ensure data integrity across layers of the data model. 

### Using the sample project

Try running the following commands:
- dbt run
- dbt test



