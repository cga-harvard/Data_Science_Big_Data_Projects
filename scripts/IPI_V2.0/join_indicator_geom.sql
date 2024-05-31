CREATE TABLE Indicator_District_Data_Geom AS
SELECT Indicator_District_Data.*, IPI722DistrictShapefile.Geom
FROM Indicator_District_Data
JOIN IPI722DistrictShapefile
ON Indicator_District_Data.District_id = IPI722DistrictShapefile.DistrictID;