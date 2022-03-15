# -*- coding: utf-8 -*-
##This script creates polygons from  coordinates in a given table.
##It runs in ArcGIS Pro version 2.8.
##Written by Phillip Gomez, September, 2021.


BEGIN
#import libraries, set environments (including CRS- change as needed!)
import arcpy, os
arcpy.env.overwriteOutput = True

projection = arcpy.SpatialReference(26990)
arcpy.env.outputCoordinateSystem = projection
#set up workspace, bring in table
ws = r'C:\Users\pjg397\Documents\Detroit_Local_PG\Detroit_Parcels.gdb'
table = r'C:\Users\pjg397\Documents\Detroit_Local_PG\Detroit_Parcels.gdb\full_detroit_IQSS'
arcpy.env.workspace = ws
#add unique field called ID_TO_JOIN that converts OBJECTID to a string

fields = ['OBJECTID', 'OBJECTID_to_Join']

arcpy.management.AddField(table, fields[1], 'TEXT')

with arcpy.da.UpdateCursor(table, fields) as cursor:
    for row in cursor:
        row[1]= row[0] #this is saying as the loop goes down the table, make row[1] (OBJECTID_to_Join) equal row[0] (OBJECTID)
        cursor.updateRow(row) #actually write (update) this into the table
del cursor

#create empty feature class, add field for unique values Street_Cross
arcpy.CreateFeatureclass_management(ws, 'full_detroit_IQSS_poly', 'POLYGON', spatial_reference = projection)
testing = os.path.join(ws, 'full_detroit_IQSS_poly')
arcpy.management.AddField('full_detroit_IQSS_poly', 'OBJECTID_to_Join', 'TEXT')
#function will later be used to convert null values to zeros

def nulls2zeros(fc):

    fieldObs = arcpy.ListFields(fc)  
    fieldNames = []  
    for field in fieldObs:  
        fieldNames.append(field.name)  
    del fieldObs  
    fieldCount = len(fieldNames)  

    with arcpy.da.UpdateCursor(fc, fieldNames) as curU:  
        for row in curU:  
            rowU = row  
            for field in range(fieldCount):  
                if rowU[field] == None:  
                    rowU[field] = "0"  

            curU.updateRow(rowU)

    del curU
nulls2zeros(table)

#create new field to hold clean coordinates
old_field = 'polygon_y'
coord_field = 'POLY_COORDS'

#be liberal with field length
arcpy.management.AddField(table, coord_field, 'TEXT', field_length = 8000)

#find undesirables, populate clean field to be without undesirables
chars = ',-.0123456789'
with arcpy.da.UpdateCursor(table, [old_field, coord_field]) as cursor:
    for row in cursor:
        if row[0] == '0':
            row[1] = '-9999'
        else:
            row[1] = ''.join(c for c in row[0] if c in chars)
        cursor.updateRow(row)
del cursor
#build geometry from coord_field

with arcpy.da.SearchCursor(table, [coord_field, 'OBJECTID_to_Join']) as table_cursor:
    for row in table_cursor:
        try:
            #skip null rows
            if row[0] != '-9999':

                #turn string of coordinates into a list but split them at the commas
                coords= list(row[0].split(','))
                
                #delete empty spaces or whole numbers like 900.
                for x in coords:
                    if len(x)<4:
                        coords.remove(x)
                        print("Removed {0} from OBJECTID= {1}".format(x, row[1]))
                    
                #break up list into list of pairs only if there are an even number of coordinates
                if (len(coords) % 2) == 0:
                    pairs = [coords[i:i + 2] for i in range(0, len(coords), 2)]
                    coord_ints = [[round(float(i),2) for i in p] for p in pairs]
                else:
                    print('{0} NUBER OF COORDINATES, CANNOT SPLIT INTO PAIRS: OBJECTID= {1}'.format(len(coords),row[1]))
                
                #create insert cursor for geometry creation, empty array to populate with coordinates
                geocursor = arcpy.da.InsertCursor(testing, ['SHAPE@', 'OBJECTID_to_Join'])
                array = arcpy.Array()

                #grab pairs of coordinates from list, put into array
                for a in pairs:
                    array.add(arcpy.Point(a[1],a[0]))

                #set crs, build polygon 
                wgs = arcpy.SpatialReference(4326)
                poly = arcpy.Polygon(array, wgs)
                geocursor.insertRow([poly, row[1]])
        except ValueError:
            print("VALUE ERROR:OBJECTID= ",row[0])

END
