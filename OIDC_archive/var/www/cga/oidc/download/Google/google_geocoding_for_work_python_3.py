#Program by Fei Carnes, 2016-2018
#Last updated by Wendy Guan, 10/4/2018

import sys, urllib, base64, hashlib, hmac, json, unicodedata, time
import urllib.parse
import urllib.request

#This script uses the Google Maps for work geocoding service, allowing for geocoding up to 100,000 addresses per day.
#It works with Python version 3
#Setup:  1)Save this script into the same folder your input addresses are in.
#        2)Format your input addresses into a tab-delimited text file without headers with 6 columns: ID, address, city, state, postal code, country.
#        3)Change "google_geocoding_sample.txt" below to the name of your input file name.
inputfile = r"google_geocoding_sample.txt"
#        4)Open a command prompt, and change directories into the folder where this file is.  At the command
#        prompt, type in:  google_geocoding_for_work_python_3.py  The script will run, geocoding each input address, and
#        outputting the results into a file named "google_geocoding_output.txt".
#        Updated by Jeff Blossom, 5/23/2016
#        Updated by Devika Kakkar, 8/11/2016
outputfile = r"google_geocoding_output.txt"

google_url = "http://maps.googleapis.com"
geocoding_endpoint = "/maps/api/geocode/json?"
key = "paste your Google Maps API key here"
#to get an API key visit https://gis.harvard.edu/Google-Maps-API-Premium 
channel = ""

field1 = "ID"
field2 = "In_Address"
field3 = "In_City"
field4 = "In_State"
field4a = "In_postal_code"
field5 = "In_Country"
field6 = "Address_Matched"
field7 = "City_Matched"
field8 = "State_Matched"
field8a = "Postal_Code_Matched"
field9 = "Country_Matched"
field10 = "Location_Type"
field11 = "Latitude"
field12 = "Longitude"


f_in = open(inputfile, 'r')
f_out = open(outputfile, 'w')
f_out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (field1, field2, field3, field4, field4a, field5, field6, field7, field8, field8a, field9, field10, field11, field12))
for line in f_in:
   fields = line.strip().replace("\"", "").split('\t')
   address = "%s+%s,%s,%s,%s" % (fields[2], fields[1], fields[3], fields[4],fields[5])
   address = address.replace("n/a", "").replace(" ", "+")
   values = { }
   values['client']= client
   values['address'] = address
   data= urllib.parse.urlencode(values)
   #Generate valid signature
   encodedParams = data
   #decode the private key into its binary format
   decodeKey = base64.urlsafe_b64decode(privateKey)
   urltosign = geocoding_endpoint + encodedParams
   urltosign_encoded = urltosign.encode('utf-8')
   #create a signature using the private key and the url encoded, string using HMAC SHA1. This signature will be binary.
   signature = hmac.new(decodeKey,urltosign_encoded, hashlib.sha1)
   #encode the binary signature into base64 for use within a URL
   encodedsignature = base64.urlsafe_b64encode(signature.digest())
   dencodedsignature = encodedsignature.decode("utf-8")
   signedurl = google_url + geocoding_endpoint + encodedParams + "&signature=" + dencodedsignature
   response = urllib.request.urlopen(signedurl)
   the_page = response.read()
   response.close
   json_str = the_page.decode()
   data_json = json.loads(json_str)
   city_name = "N/A"
   admin1 = "N/A"
   country = "N/A"
   streetNum = "N/A"
   street = "N/A"
   postal_code = "N/A"
   for i in range(len(data_json["results"])):
      for component in data_json["results"][i]["address_components"]:
         if "locality" in component["types"]:
            city_name = component["long_name"]
      if city_name != "N/A":
         break
   for i in range(len(data_json["results"])):
      for component in data_json["results"][i]["address_components"]:
         if "administrative_area_level_1" in component["types"]:
            admin1 = component["long_name"]
      if admin1 != "N/A":
         break
   for i in range(len(data_json["results"])):
      for component in data_json["results"][i]["address_components"]:
         if "postal_code" in component["types"]:
            postal_code = component["long_name"]
      if postal_code!= "N/A":
         break
   for i in range(len(data_json["results"])):
      for component in data_json["results"][i]["address_components"]:
         if "country" in component["types"]:
            country = component["long_name"]
      if country != "N/A":
         break
   for i in range(len(data_json["results"])):
      for component in data_json["results"][i]["address_components"]:
         if "street_number" in component["types"]:
            streetNum = component["long_name"]
      if streetNum != "N/A":
         break
   for i in range(len(data_json["results"])):
      for component in data_json["results"][i]["address_components"]:
         if "route" in component["types"]:
            street = component["long_name"]
      if street != "N/A":
         break
   p_city = city_name
   p_admin1 = admin1
   p_country = country
   p_address = streetNum + " " + street
   p_postal_code = postal_code
 #  print("City_matched", p_city, "state matched", p_admin1, "Country matched", p_country, "Address matched", p_address, "Postal code matched", p_postal_code) 
   print("Processing" + "  " + "ID "+ fields[0])
   try:
      f_out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], p_address, p_city, p_admin1 , p_postal_code, p_country, data_json["results"][0]["geometry"]["location_type"], data_json["results"][0]["geometry"]["location"]["lat"], data_json["results"][0]["geometry"]["location"]["lng"]))
   #except: continue
   except: f_out.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], "not found"))
   time.sleep(.5)
   #print data.read()

print ("Finished geocoding")
f_out.flush()
f_in.close()
f_out.close()
