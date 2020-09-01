Create table addresses (Id character varying(255), longitude float, latitude float, start_date character varying(255), end_date character varying(255));
copy addresses from '/n/holyscratch01/cga/dkakkar/data/BIL/Input_addresses.csv' (FORMAT csv, HEADER, DELIMITER ',');
Alter table addresses add column startdate date;
Alter table addresses add column enddate date;
Update addresses set startdate=TO_DATE(start_date,'YYYYMMDD');
Update addresses set enddate=TO_DATE(end_date,'YYYYMMDD');
Alter table addresses drop column start_date;
Alter table addresses drop column end_date;
