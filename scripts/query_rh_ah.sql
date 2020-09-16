Drop table IF exists resultstdmean;
Drop table IF exists resultstmean;
Drop table IF exists temp;

Create table resultstdmean (address_id character varying(255),day date, tdmean FLOAT);


\copy resultstdmean from '/n/holyscratch01/cga/dkakkar/data/resultstdmean.csv' (FORMAT csv, delimiter E'\t');

Create table resultstmean (address_id character varying(255),day date, tmean FLOAT);


\copy resultstmean from '/n/holyscratch01/cga/dkakkar/data/resultstmean1981.csv' (FORMAT csv, delimiter E'\t');


Create table temp as (Select a.address_id, a.day, a.tdmean, b.tmean from resultstdmean a inner join resultstmean b on a.address_id=b.address_id AND a.day=b.day);

Alter table temp add column RH FLOAT;

Alter table temp add column AH FLOAT;

Update temp set RH=100*(EXP((17.625*tdmean)/(243.04+tdmean))/EXP((17.625*tmean)/(243.04+tmean)));
Update temp SET AH=(6.112*EXP((17.67*tmean)/(tmean +243.5)) * RH * 2.1674)/(273.15+ tmean);

\copy (Select address_id, day, RH, AH from temp) to '/n/holyscratch01/cga/dkakkar/data/results_rh_ah_test.csv';
