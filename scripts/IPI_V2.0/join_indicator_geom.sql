CREATE TABLE Indicator_District_Geometry AS
SELECT
    a.*,
    b.geom
FROM
    Indicator_District_Data a
LEFT JOIN
    sdr_District_BDY_NewAP b
ON
    CAST(a.District_ID AS INT) = b.DistrictID;