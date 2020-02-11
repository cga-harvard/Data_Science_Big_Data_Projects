/*KNN calculation begins here:*/
Create table CA1 as (select id,geom,grp from partisan where grp= 'CA1');
Alter table CA1 Add column row_id Serial primary key;
Create index CA1_gix on CA1 using gist(geom);
CREATE INDEX CA1_hash ON CA1(ST_GeoHash(ST_Transform(geom,4326)));
CLUSTER CA1 USING CA1_hash;
Create table knn_1000_CA1 (source_id character varying(255), neighbor_id character varying(255),dist float);
DO $$
BEGIN
FOR counter IN 1..(Select count(*) from CA1)
LOOP
INSERT INTO knn_1000_CA1(
SELECT a.id as source_id, b.id as neighbor_id, ST_DistanceSphere((SELECT geom FROM CA1 WHERE row_id = counter), b.geom) AS dist
FROM CA1 a, partisan b
WHERE a.id <> b.id
AND a.row_id = counter
ORDER BY (SELECT geom FROM CA1 WHERE row_id = counter) <-> b.geom
LIMIT 1000)
;
END LOOP;
END; $$
;
