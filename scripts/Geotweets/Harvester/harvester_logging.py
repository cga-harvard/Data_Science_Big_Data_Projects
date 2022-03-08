import os
import datetime
import smtplib
from email.mime.text import MIMEText
date= datetime.datetime.now() - datetime.timedelta(0,3600) #check for file associated with two hours ago- 7200 seconds
date = date.strftime('%Y-%m-%d %H')
year = date[:4]
month= date[5:7]
day= date[8:10]
hour = date[11:13]
DHG = year + '_' + month + '_' + day + '_' + hour
fname = "geo_tweets_hour_" + DHG + ".csv"
#print(fname)
dirname = '/data/geotweets_cga/geotweets_data/'
print("Directory name ", dirname)
recipients = ['blewis@cga.harvard.edu', 'devikakakkar29@gmail.com']

def send_email (message, status):
    fromaddr = 'bop.harvard@gmail.com'
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login('bop.harvard@gmail.com','433633ab')
    server.sendmail(fromaddr, recipients, 'Subject: %s\r\n%s' % (status, message))
    server.quit()

file_exists = os.path.isfile(os.path.join(dirname, fname))

if not file_exists:
    print("Sending email")
    send_email("Restart Harvester using restart script","Geowteet Harvester down")
    print("Stopping and restarting harvester")
    os.execl('/data/geotweets_cga/scripts/restart.sh', '')
    server.quit()
elif os.stat(os.path.join(dirname, fname)).st_size < 10^6: # if file size < 1MB
    print("Sending email")
    send_email("Restart Harvester using restart script","Geowteet Harvester down")
    print("Stopping and restarting harvester")
    os.execl('/data/geotweets_cga/scripts/restart.sh', '')
    server.quit()  

#send_email("Restart Harvester","Harvester down")

"""
server = smtplib.SMTP('smtp.gmail.com:587')
server.ehlo()
server.starttls()
server.login('bop.harvard@gmail.com','433633ab') #need to change this to something else?

#recipients = ['blewis@cga.harvard.edu', 'devikakakkar29@gmail.com']
msg = MIMEText('File missing or less than 1 MB --> ' +'Related Date_Time: ' + DHG )
msg['Subject'] = 'Twitter Harvester Email Alert'
msg['From'] = 'bop.harvard@gmail.com'
msg['To'] = ['blewis@cga.harvard.edu', 'devikakakkar29@gmail.com']

file_exists = os.path.isfile(os.path.join(dirname, fname))
#if not file_exists:
if  file_exists:
    print("Sending email")
    server.send_message(msg)
    print("Stopping and restarting harvester")
    os.execl('/home/ubuntu/geo-tweet-harvester/scripts/restart.sh', '')
    server.quit()
elif os.stat(os.path.join(dirname, fname)).st_size < 10^6: # if file size < 1MB
    print("Sending email")
    server.send_message(msg)
    print("Stopping and restarting harvester")
    os.execl('/home/ubuntu/geo-tweet-harvester/scripts/restart.sh', '')
    server.quit()     
"""  
