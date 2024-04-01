#Program by Fei Carnes, 2016-2018
#Last updated by Wendy Guan, 10/4/2018

import sys, urllib, urllib2, json, base64, hashlib, hmac, time

#Change the directory HERE for the input file
#(!!! must be a tab-delimited text file without headers, and only has 5 fields: ID, origin X, origin Y, destination X, and destination Y)
inputfile = r".\googlein.txt"
#Change the directory HERE for the output file
outputfile = r".\googleout.txt"

google_url = "http://maps.googleapis.com"
distance_endpoint = "/maps/api/distancematrix/json?"
key = "paste your Google Maps API key here"
#to get an API key visit https://gis.harvard.edu/Google-Maps-API-Premium 
channel = ""
#specifies the mode of transport to use when calculating directions. Valid values are: driving, walking , or bicycling 
mode = "driving"

field1 = "ID"
field2 = "origin_x"
field3 = "origin_y"
field4 = "destination_x"
field5 = "destination_y"
field6 = "distance_meters"
field7 = "duration_seconds"

f_in = open(inputfile, 'r')
f_out = open(outputfile, 'w')
f_out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (field1, field2, field3, field4, field5, field6, field7))
f_in_line = f_in.readlines()

for line in f_in_line:
   fields = line.strip().replace("\"", "").split('\t')
   #assign x/y coordinates array to origin and destination
   origin = "%s,%s" % (fields[2], fields[1])
   destination = "%s,%s" % (fields[4], fields[3])
   #Generate valid signature
   encodedParams = urllib.urlencode({"origins":origin,"destinations":destination,"mode":mode,"client":client,"sensor":"false"});
   #encodedParams = urllib.urlencode("origins=%s&destinations=%s&mode=%s&client=%s") % (origin, destination, mode, client)
   #decode the private key into its binary format
   decodeKey = base64.urlsafe_b64decode(privateKey)
   urltosign = distance_endpoint + encodedParams
   #create a signature using the private key and the url encoded, string using HMAC SHA1. This signature will be binary.
   signature = hmac.new(decodeKey, urltosign, hashlib.sha1)
   #encode the binary signature into base64 for use within a URL
   encodedsignature = base64.urlsafe_b64encode(signature.digest())
   signedurl = google_url + distance_endpoint + encodedParams + "&signature=" + encodedsignature
   result = urllib.urlopen(signedurl)
   result_json = json.loads(result.read())
   check_status = result_json["rows"][0]["elements"][0]["status"]
   try:
      f_out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], result_json["rows"][0]["elements"][0]["distance"]["value"], result_json["rows"][0]["elements"][0]["duration"]["value"]))
   except:
      f_out.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], result_json["rows"][0]["elements"][0]["status"]))
print "Finished!"
print "Process Ends: %s" % time.asctime( time.localtime(time.time()) )
f_in.close()
f_out.close()
