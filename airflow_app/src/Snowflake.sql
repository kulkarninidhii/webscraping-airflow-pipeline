use warehouse Assignment4;
create database pdf_extract;
create schema pdf_extract.pdf_schema;


create table pdf_extract.pdf_schema.ptab
( File_Name varchar,
  Topic varchar,
  Heading varchar,
  Learning_Outcomes varchar
);   


COPY INTO pdf_extract.pdf_schema.ptab
	FROM 's3://validateddocpool/PDF.csv'
	CREDENTIALS=(AWS_KEY_ID='access_key' AWS_SECRET_KEY='secret_access_key')
	FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF=('<NULL>', 'NULL', ''))
	ON_ERROR = 'CONTINUE'; 


