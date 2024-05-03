create warehouse Assignment4;
create database raw_test;
create database raw_prod;
create database clean_test;
create database clean_prod;
create schema raw_test.cfa;
create schema raw_test.pdf;
create schema raw_prod.cfa;
create schema raw_prod.pdf;


create table raw_test.cfa.ctable
( Name_of_the_topic varchar,
  Year integer,
  Level integer,
  Introduction_Summary varchar,
  Learning_Outcomes varchar,
  Link_to_the_Summary_Page varchar,
  Link_to_the_PDF_File varchar
); 

create table raw_prod.cfa.ctable
( Name_of_the_topic varchar,
  Year integer,
  Level integer,
  Introduction_Summary varchar,
  Learning_Outcomes varchar,
  Link_to_the_Summary_Page varchar,
  Link_to_the_PDF_File varchar
); 

create table raw_test.pdf.ptable
( File_Name varchar,
  Topic varchar,
  Heading varchar,
  Learning_Outcomes varchar
);   


COPY INTO raw_test.cfa.ctable
	FROM 's3://validateddocpool/CFA.csv'
	CREDENTIALS=(AWS_KEY_ID='access_key' AWS_SECRET_KEY='secret_access_key')
	FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF=('<NULL>', 'NULL', ''))
	ON_ERROR = 'CONTINUE';

COPY INTO raw_test.pdf.ptable
	FROM 's3://validateddocpool/PDF.csv'
	CREDENTIALS=(AWS_KEY_ID='access_key' AWS_SECRET_KEY='secret_access_key')
	FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF=('<NULL>', 'NULL', ''))
	ON_ERROR = 'CONTINUE'; 


create table raw_prod.pdf.ptable
( File_Name varchar,
  Headings varchar,
  Topics varchar,
  Topics_Count integer
); 

