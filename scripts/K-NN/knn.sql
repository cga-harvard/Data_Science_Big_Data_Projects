/*KNN calculation begins here:*/
Create table RI as (select id,geom,grp,pid,lat,lon from partisan where grp= 'RI');
Alter table RI Add column row_id Serial primary key;
Create index RI_gix on RI using gist(geom);
CREATE INDEX RI_hash ON RI(ST_GeoHash(ST_Transform(geom,4326)));
CLUSTER RI USING RI_hash;
Create table knn_1000_RI (source_id character varying(255), neighbor_id character varying(255), source_pid  character varying(255), neighbor_pid  character varying(255), lat float, lon float, dist float);
DO $$
BEGIN
FOR counter IN 1..(Select count(*) from RI)
LOOP
INSERT INTO knn_1000_RI(
SELECT a.id as source_id, b.id as neighbor_id, a.pid as source_pid, b.pid as neighbor_pid, a.lat as source_lat, a.lon as source_lon, ST_DistanceSphere((SELECT geom FROM RI WHERE row_id = counter), b.geom) AS dist
FROM RI a, partisan b
WHERE a.id <> b.id
AND a.row_id = counter
ORDER BY (SELECT geom FROM RI WHERE row_id = counter) <-> b.geom
LIMIT 1000)
;
END LOOP;
END; $$
;
Copy (SELECT * FROM knn_1000_RI) to '/n/holyscratch01/cga/dkakkar/data/knn_1000_RI.csv';
