#Google Distance Matrix API (Using Google Maps for Work License in Python Script 3.X)
#It is used to calculate distance in meters and travel durations in seconds for pairs of origin and destination coordinates

#Version: V3.0
#Updated on: November 3, 2016
#Authors: Kathryn Nicholson from HKS. 
#         Fei Carnes from Center for Geographic Analysis (main contact if you have any questions).
#Last updated by Wendy Guan, 10/4/2018

#The script runs within while loop to retry every 4 hours in case daily limit is reached
#On the 4th retry of the same ID, the process will wait until the remaining time such that a full 24 hours has passed
#since the full batch originally started (as daily limits are reset 24 hours after large batch not a fixed time of day)
#and then retries 1 final time until moving on to the next ID

#Very Important!!!: The input must to be: tab-delimited text file without headers, and only 5 fields: ID, origin X, origin Y, destination X, and destination Y

import sys, urllib, base64, hashlib, hmac, json, unicodedata, time
import urllib.parse, urllib.request
import smtplib # import smtplib for actual sending function
from urllib.error import HTTPError
from optparse import OptionParser
from smtplib import SMTPException

# -------------------- Command line options to specify input & output & email notificaton (optional) --------------------
parser = OptionParser()
# Input file must be a .txt tab-delimited text file without headers, and have following 5 fields:  ID, origin X, origin Y, destination X, destination Y
# Output file must also be a .txt text file, writes input fields + distance in meters and duration in seconds
parser.add_option("-i", "--input-file", dest="input_file",
	help="full path to input file (tab delim .txt)", metavar='"/input.txt"')
parser.add_option("-o", "--output-file", dest="output_file",
	help="full path to output file (tab delim .txt)", metavar='"/output.txt"')
# (Optional) add email address to send Distance matrix status notifications
parser.add_option("-e", "--email", dest="email",
	help="email address for status notifications", metavar='"address@gmail.com"')
# load command line args into parser
(options, args) = parser.parse_args()

# make both input and output files required arguments

# -------------------- change the input & output directory , notification email option -------------------------------------------------------------
if options.input_file:
	# Set input .txt path from command line option
	input_file = os.path.expanduser(options.input_file)
else:
	## Change the directory (within the double quotation marks) for the input file if dont use command line option
	inputfile = r".\input.txt"

if options.output_file:
	# Set output .txt path from command line option
	output_file = os.path.expanduser(options.output_file)
else:
	# Change the directory for the output file if dont use command line option
	outputfile = r".\output.txt"

# make email address optional argument, no notification if not specified
# adding a status email address if specified
if options.email:
	#set the notification email from the comman line option
	email = options.email
	print ("Using email address: %s for query status notifications\n" % email)
else:
	#set the notification email (optional) in python 3.It only works for GMail
	# Googel blocks sign-in attempts from apps which do not use modern security standards.
	#Thus, you have to turn ON this "Access for less secure apps" option here: https://www.google.com/settings/security/lesssecureapps
	
	email = "address@gmail.com" #change to your gmail address
	email_pw = "your_email_password" #your gmail password
	email_content ="distance matrix calc finished"
	print ("Using email address: %s for query status notifications\n" % email)

# --------------------------- Main Code Starts here ----------------------------------------------------------------------

# Set google url and service key
google_url = "https://maps.googleapis.com"
distance_endpoint = "/maps/api/distancematrix/json?"
key = "paste your Google Maps API key here"
#to get an API key visit https://gis.harvard.edu/Google-Maps-API-Premium 
# Specify the mode of transport. Valid values are: driving, walking, bicycling, or transit
mode = "driving"

# Set up the output file
field1 = "ID"
field2 = "origin_x"
field3 = "origin_y"
field4 = "destination_x"
field5 = "destination_y"
field6 = "distance_meters"
field7 = "duration_seconds"

# set read/write permissions to specified input/output files
f_in = open(inputfile, 'r')
f_out = open(outputfile, 'w')
f_out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (field1, field2, field3, field4, field5, field6, field7))
f_in_line = f_in.readlines()
# Set an initial delay of 0.01 second between two consecutive requests to allow the default 1000 queries per second limit
delay = 0.01

# store starting date and time of batch and display to user
process_start_dtime = time.asctime( time.localtime(time.time()) )
print ("Process Start: %s\n" % process_start_dtime)

# store hour and minutes when process started to use to determine when daily limit is reset
# daily query limit starts +24 hours from when first query in batch sent
start_time = time.localtime()
start_hour = start_time.tm_hour
start_min = start_time.tm_min

try: 
	# Several parameters to handle over-limit errors
	for line in f_in_line:
		attempts = 0
		success = False

		#format input file to be tab-delimited
		fields = line.strip().replace("\"", "").split('\t')

		#assign coordinates to origin and destination
		#Google lists X=longitude and Y=latitude; the order should be Y, X
		#print (fields[0], fields[1], fields[2])
		origin = "%s,%s" % (fields[1], fields[2])
		destin = "%s,%s" % (fields[3], fields[4])

		# Generate valid signature
		encodedParams = urllib.parse.urlencode({"origins":origin,"destinations":destin,"mode":mode,"client":client})
		#encodedParams = urllib.parse.urlencode({"origins":origin,"destinations":destin,"mode":mode,"avoid":avoid,"departure_time":depart_time,"client":client})

		# Decode the private key into its binary format
		decodeKey = base64.urlsafe_b64decode(privateKey)
		urltosign = distance_endpoint + encodedParams
		urltosign_encode = urltosign.encode('utf-8')
		# Create signature using the private key and the url encoded, string using HMAC SHA1. This signature will be binary.
		signature = hmac.new(decodeKey, urltosign_encode, hashlib.sha1)
		# encode the binary signature into base64 for use within a URL
		encodedsignature = base64.urlsafe_b64encode(signature.digest())
		decodedsignature = encodedsignature.decode("utf-8")
		signedurl = google_url + distance_endpoint + encodedParams + "&signature=" + decodedsignature
		#print(signedurl)

		#loop as long as there are failed rows which have been tried less than 4 times
		while success != True and attempts <4:
			#wrap while loop in a try to handle any error raised
			try:
				if attempts ==1:
					failed_id= fields[0]
					if options.email:
						os.system('echo "distance matrix calc error, retrying first time" | mail -s "distance matrix failed" %s' % email)

				time.sleep(delay) #pause between requests to meet Google limitation

				#send url and read returned json results
				result = urllib.request.urlopen(signedurl)
				page = result.read()
				result.close
				json_str = page.decode()
				result_json = json.loads(json_str)
				#print(result_json)

				# -GetStatus- parses the answer and returns the status code
				#Google returns 2 types of status codes, defined:
				#1) status = google returned top-level status; 2) check_status = element-level request result returned status
				# Possible status codes: OK, UNKNOWN_ERROR, REQUEST_DENIED, INVALID_REQUEST, OVER_QUERY_LIMIT, MAX_ELEMENTS_EXCEEDED

				status = result_json["status"]

				if status == "OK":
				# if the top level status is OK, the element level status can be: OK, NOT_FOUND, _ZERO_RESULTS
					check_status = result_json["rows"][0]["elements"][0]["status"]

					if check_status == "OK":
						f_out.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], result_json["rows"][0]["elements"][0]["distance"]["value"], result_json["rows"][0]["elements"][0]["duration"]["value"]))
						print ("status:%s timestamp:%s ID = %s is processed\n" % (check_status, time.strftime("%I:%M:%S"), fields[0]))
						success = True
					elif check_status == "ZERO_RESULTS":
						f_out.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], check_status))
						print ("status:%s timestamp:%s ID = %s driving directions could not be found\n" % (check_status, time.strftime("%I:%M:%S"), fields[0]))
						success = True
					elif check_status == "NOT_FOUND":
						f_out.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], check_status))
						print ("status:%s timestamp:%s ID = %s orig/dest pairing could not be geocoded\n" % (check_status, time.strftime("%I:%M:%S"), field[0]))
						success = True
				# if the top-level status returned unknown, a server error occurred and the request may succeed if resent
				elif status == "UNKNOWN_ERROR":
					# print the error and ID line out for user
					print ("status:%s timestamp:%s ID = %s returned UNKNOWN_ERROR, retrying this request\n" % (status, time.strftime("%I:%M:%S"), fields[0]))
					# add an extra sleep before re-sending unknown error entry, then retry entry
					time.sleep(10)
					# count number of consecutive unknown errors being retried
					attempts += 1
					success = False
					# re-try line
					continue
				# if the top-level status was denied or invalid, the request has been denied by Google (key or service problem), so continue to next line
				elif status == "REQUEST_DENIED" or status == "INVALID_REQUEST":
					f_out.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], status))
					print("status:%s timestamp:%s ID = %s is malformed, check your URL\n" % (status, time.strftime("%I:%M:%S"), fields[0]))
					success = True

					
					#if the top-level status returns daily limit, store last ID and retry if current request session has retried <= 3 entries
					# set sleep time to wait until next day to re-try request when daily limit has been refreshed
				elif status == "OVER_QUERY_LIMIT":
					print ("OVER_QUERY_LIMIT, Daily limit has been reached")
					print ("status:%s timestamp:%s ID = %s is still processing, set to retry\n" % (status, time.strftime("%I:%M:%S"), fields[0]))
					# don't write anything to the file while retrying until the 4th retry
					# store the hour that the query limit was reached to start loop again +1 day after query batch was first sent
					# daily limit restarts 24 hours after batch initially sent
					time_limit_reached = time.localtime()
					hour_reached = time_limit_reached.tm_hour
					total_wait_hours = 24 - (hour_reached - start_hour)

					# subtract 24 if military time brought us over 24 hours
					if total_wait_hours > 24: total_wait_hours = total_wait_hours - 24
					print ("Estimated query limit will reset in %d hours, +1 day after started on %s" % (total_wait_hours, process_start_dtime))
					
					#divide total hour wait by 3 to retry 3 times in 24 hour period
					wait_hours = total_wait_hours/3

					# convert hours to wait to seconds for sleep time
					print ("retry attempt # %d" % attempts)
					print ("Waiting %d hours to retry \n\n" % wait_hours)
					wait_seconds = wait_hours*3600 + start_min*60
					
					#increase subsequent 0.01 second delay if over quota status is detected
					delay += 0.01

					#implement sleep to wait until midnight next day to retry id
					time.sleep(wait_seconds)
					
					#count how many times we're retrying the same first ID that hit limit
					attempts +=1
					# mark success False to retry this ID
					success = False

					# if this is the 3rd attempt on this same ID (2nd retry) due to daily limit reached,
					# wait until full +1 day reset since start of full batch and retry 1 3rd time,
					# then after on 4th attempt move on to next id
					if attempts == 3:
						print ("Last attempt, waiting %d hours until full query reset, +1 day after started on %s" % (total_wait_hours, process_start_dtime))

						# write over_query_limit failure to the output file on the last retry to make sure that next line has correctly retried ID
						f_out.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], status))

						# wait the full time between retry time and original process start
						total_wait_seconds = total_wait_hours*3600
						time.sleep(total_wait_seconds)

					#retry this ID at beginning of loop
					continue

				else:
					print("status:%s timestamp:%s ID = %s can't be processed\n" % (status, time.strftime("%I:%M:%S"), fields[0]))
					f_out.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (fields[0], fields[1], fields[2], fields[3], fields[4], status))

					#Count number of consecutive unknown errors
					attempts += 1
					success = False
					continue
			except:
				pass

	print("Finished")
	print("Process Ends: %s\n" % time.asctime( time.localtime(time.time()) ))
	f_in.close()
	f_out.close()
except HTTPError as e:
	content = e.read()

#send email when distance calculations are complete
if options.email:
	os.system('echo "distance matrix calc finished" | mail -s "distance matrix calc finished" %s' % email)
else:
	try:
		mail = smtplib.SMTP('smtp.gmail.com', 587)
		mail.ehlo()
		mail.starttls()
		mail.login(email, email_pw)
		mail.sendmail(email,email,email_content)
		mail.close()
		print ("Successfully sent email")
	except SMTPException:
		print ("Error: unable to send email")
