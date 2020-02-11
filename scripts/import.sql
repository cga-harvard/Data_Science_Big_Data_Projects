Create table partisan(id varchar(255), lat double precision, lon double precision, pid varchar(255), state varchar(255), grp varchar(255));
\copy partisan from '$location' delimiter ',' CSV HEADER ;
Create extension postgis;                                                                                                                                     
                                                                                                                                     
                                                                                                                                     
ALTER TABLE partisan ADD COLUMN geom geometry(Point, 4326);
UPDATE partisan SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);                                                                                                                                      
                                                                                                                                     
Create index US_geom_gix on partisan  using gist(geom);
Alter table partisan Add column row_id Serial primary key;
                                                                                                                                     
CREATE INDEX us_geohash ON partisan (ST_GeoHash(ST_Transform(geom,4326)));                                                                                                                                     
                                                                                     
CLUSTER partisan using us_geohash;
                                      
