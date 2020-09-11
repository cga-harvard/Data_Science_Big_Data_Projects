
Create table results as (SELECT  a.id as id, regexp_replace(CAST(b.filedate AS TEXT),'-','','g') AS day, 
ST_VALUE(b.rast, ST_Transform(ST_SetSRID( ST_Point(a.longitude, a.latitude), 4326), 4269)) as tmax 
from addresses a 
inner join tmaxunion b 
ON
ST_Intersects(ST_Envelope(b.rast), ST_Transform(ST_SetSRID( ST_Point(a.longitude, a.latitude), 4326), 4269)) = 't'
AND startdate <= filedate AND enddate >= filedate);
Alter table results add column RH float;
Alter table results add column AH float;  
Update results SET RH=;
Update results SET AH=;                 
Copy(Select * from results) TO '/n/holyscratch01/cga/dkakkar/data/results.csv';
