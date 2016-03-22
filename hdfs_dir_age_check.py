# Python Script to check the age of HDFS files in any given directories.
# Accepts a json config file with the following keys: file_age_seconds, directories_to_monitor (will accept multiple values) with 
# folders_files_blacklist (will accept multiple values) nested within directories_to_monitor, logfile, httpfs_or_webhdfs_host and httpfs_or_webhdfs_port, emails_to_send_alert_to 
#(will accept multiple values) and source_email_address - Script will fail if any are missing
# Config file name: hdfs_file_age_check.json - user should be in the same directory as this config file
#
# Example usage of json configuration file:
#
#######################################################
#{
#    "file_age_hours": "24",
#    "directories_to_monitor": {
#        "directory1": [
#            {
#            "name": "/directory1",
#            "folders_files_blacklist": [
#                "file1",
#                "dir1",
#                ...
#                ]
#            }
#        ],
#        "directory2": [
#            {
#            "name": "/directory2",
#            "folders_files_blacklist": [
#                "file1",
#                "dir1",
#                ...
#                ]
#            }
#        ],
#        ...
#    },
#    "httpfs_or_webhdfs_host": "hostname",
#    "httpfs_or_webhdfs_port": "14000",
#    "emails_to_send_alert_to": [
#        "person1@email.com",
#        "person2@email.com",
#        ...
#    ],
#    "source_email_address": "server@email.com",
#	 "logfile": "/tmp/hdfs_file_age_check.log"
#}
#######################################################
#
# Author: Robert Barton - robert.william.barton@hotmail.co.uk
#
#######################################################


import json, pycurl, time, calendar, smtplib, logging, optparse
from StringIO import StringIO
from email.mime.text import MIMEText
from optparse import OptionParser


# This is the bit that determines whether or not a file hasn't been written to within the given time period (uses epoch) specified in the configuration file (configured in hours)
def File_age_check(file_age_seconds, age_json):
    return calendar.timegm(time.gmtime()) - int(age_json[:10]) <= file_age_seconds


# This function checks the files found against the configured blacklists
def black_list_check(folders_files_blacklist, filename):
    return filename in folders_files_blacklist


# Here we find all the directories and files in our configured paths that are older than the configured alert time
def get_old_hdfs_files(hdfs_api_host, hdfs_api_port, directories_to_monitor, file_age_seconds, file_age_hours):
    hdfs_old_files = []
    for path in directories_to_monitor:
        for dirname in directories_to_monitor[path]:

            # Here we extract each configured blacklist for each configured directory
            configured_directory = dirname['name']
            folders_files_blacklist = dirname['folders_files_blacklist']

            url =  'http://' + hdfs_api_host + ':' + hdfs_api_port + '/webhdfs/v1' + configured_directory + '?op=LISTSTATUS'
            buffer = StringIO()
            curl = pycurl.Curl()
            curl.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_GSSNEGOTIATE)
            curl.setopt(pycurl.USERPWD, ':')
            curl.setopt(pycurl.URL, url)
            curl.setopt(pycurl.WRITEDATA, buffer)
            curl.perform()
            curl.close()

            hdfs_data = json.loads(buffer.getvalue().decode("utf-8"))

            for directory in hdfs_data.values():
                for response in directory['FileStatus']:
                    if black_list_check(folders_files_blacklist, response['pathSuffix']) == False and File_age_check(file_age_seconds, str(response['modificationTime'])) == False:
                        old_file = configured_directory + '/' + response['pathSuffix']
                        hdfs_old_files.append(old_file)
                        logging.info("File %s has not been written to in over %s Hours" % (old_file, file_age_hours))

    return hdfs_old_files


# Create the content of the Alert message
def compose_the_alert(get_old_hdfs_files, file_age_hours):
    message = "The following files in HDFS are over %s hours old, please investigate:\n\n" % file_age_hours
    for f in get_old_hdfs_files:
        message = message + f + "\n"
    return message


# Send out the alert to the 
def send_out_the_alert(compose_the_alert, emails_to_send_alert_to, source_email_address, file_age_hours):
    msg = MIMEText(compose_the_alert, 'plain')
    for mail in emails_to_send_alert_to:
        msg['Subject'] = 'ALERT: HDFS files over %d Hours Old, PLEASE INVESTIGATE' % file_age_hours
        msg['From'] = source_email_address
        msg['To'] = mail
        s = smtplib.SMTP('localhost')
        s.sendmail(source_email_address, [mail], msg.as_string())
        s.quit()


# Main part of the script, where we take the configured values from the json configuration file, setup logging and run the main functions defined above
def main():
    parser = optparse.OptionParser(usage='usage: %prog -c <config_file.json> ')
    parser.add_option('-c', '--config', dest="config_filename", default="hdfs_dir_age_check.json")
    (opts, args) = parser.parse_args()

    with open(opts.config_filename) as data_file:
        data = json.load(data_file)

    # Hours kept separate from seconds for nicer log and alert messages
    file_age_hours = int(data['file_age_hours'])
    file_age_seconds = 3600 * file_age_hours
    directories_to_monitor = data['directories_to_monitor']
    hdfs_api_host = data['httpfs_or_webhdfs_host']
    hdfs_api_port = data['httpfs_or_webhdfs_port']
    emails_to_send_alert_to = data['emails_to_send_alert_to']
    source_email_address = data['source_email_address']

    # Setup logging
    logfile = data['logfile']
    logging.basicConfig(level=logging.INFO, filename=logfile, format='%(asctime)s %(levelname)s %(message)s' )

    # Compile all the functions
    send_out_the_alert(compose_the_alert(get_old_hdfs_files(hdfs_api_host, hdfs_api_port, directories_to_monitor, file_age_seconds, file_age_hours), file_age_hours), emails_to_send_alert_to, source_email_address, file_age_hours)


# Run the script
if __name__ == "__main__":
    main()
