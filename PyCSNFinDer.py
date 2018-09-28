"""
Copyright 2018 California Institute of Technology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

'''
Created on August 6, 2013

@author: Julian

'''
import os
import sys
import time
import signal
import datetime
import threading
import logging.handlers
import math
import Queue
import ntplib
import json
import zipfile
import yaml
import boto
import boto.sqs
from boto.sqs.message import Message
import gzip



import socket
socket.setdefaulttimeout(120.0) # seconds

from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer

from GetDiskUsage import disk_usage

from PhidgetsSensorFinDer import PhidgetsSensor



# endpoints: uptime, version, getdata, getlatestsample, getfiles, getrecentpicks, {default}

# 2016/02/02: PhidgetsClient v2.1: Allow list of NTP servers in client's YAML file
# 2016/06/06: Modify Phidgets disconnect/sample health check to restart phidget more aggressively
# 2016/06/15: Allow NTP fit outliers when persistent, and set lower limit on fit sigma
# 2016/06/21: Add "uptime" endpoint which shows how long the client has been up and running
# 2016/08/15: Add "version" endpoint, which returns the SOFTWARE_VERSION string
# 2016/10/10: New picker will immediately report de-meaned accelerations above 0.005g on any axis.
# 2016/11/01: Extra monitoring messages showing the various thread statuses. Extra checks on heartbeat send.
# 2016/11/03: Add socket timeout with 120 second value
# 2016/11/22: Storage check thread will stop sample collection if little disk space left, will resume when more free space
# 2017/07/24: Switch from bz2 to gzip compression - avoids Lenny issues with bz2
# 2017/07/25: Replace Picker code with simple 0.005g threshold on LTA, and add Amazon SQS
# 2017/12/11: v1.4 Enhanced error reporting for Web server exceptions
# 2018/01/23: v1.5 Added is_secure=False for boto s3 connection
# 2018/03/30: v1.6 Added getrecentpicks endpoint
# 2018/04/06: v1.7 Removed requirement for network connectivity when client starts up
# 2018/05/11: v2.0 Add pick sending directly to FinDer via ActiveMQ

SOFTWARE_VERSION = 'PyAmazonCSN Client v2.0 2018/05/11'
CLIENT_FIELDS = [
    'registered',
    'server',
    'client_id',
    'building',
    'floor',
    'latitude',
    'longitude',
    'name',
    'operating_system',
    'software_version',
    'time_servers',
] 

# for IAM user csn.bot
aws_access_key_id = 'XXXXXXXXXXXXXXXXXXXXXXX' 
aws_secret_access_key = 'YYYYYYYYYYYYYYYYYYYYYYYYY' 
aws_bucket_wavedata = 'csnreadings'
aws_bucket_stationdata = 'csnstations'

TARGET_SERVER = 'xxxxxxxxxxxxxxxxxxxxxxxx.amazonaws.com'

PICK_QUEUE_NAME = 'pick_queue'
SQS_REGION = 'us-west-2'


LOGFILE = 'PyAmazonCSN.log'
if sys.platform == 'win32':
    LOGFILE_DIRECTORY = '/tmp'
    TEMPORARY_FILE_DIRECTORY = '/tmp/phidgetsdata'    
else:
    LOGFILE_DIRECTORY = '/var/tmp'
    TEMPORARY_FILE_DIRECTORY = '/var/tmp/phidgetsdata'
BYTES_IN_LOGFILE = 2000000 
LOGFILE_BACKUPS = 10

# the maximum number of pending data files that will be sent on each heartbeat
MAX_PENDING_UPLOADS = 10



TIME_SERVERS = ["ntp-02.caltech.edu","0.us.pool.ntp.org","1.us.pool.ntp.org","2.us.pool.ntp.org","3.us.pool.ntp.org"]


HEARTBEAT_INTERVAL = datetime.timedelta(seconds=600)
NTP_INTERVAL = 60.0
STORAGE_CHECK_INTERVAL = datetime.timedelta(seconds=1200)
FILESTORE_NAMING = "%Y%m%dT%H%M%S"

MIN_POINTS_TIME_FIT = 10
OUTLIER_DETECTION_POINTS_TIME_FIT = 20
OUTLIER_ALLOWED_TIME = 120.0 
MIN_SIGMA = 0.001 # minimum sigma allowed for dispersion on fit points
MAX_POINTS_TIME_FIT = 60
MAX_OFFSET_ALLOWED = 3600.0

PERCENT_STORAGE_USED_THRESHOLD = 80.0
PERCENT_STORAGE_STOP_DATA_THRESHOLD = 90.0

PORT_NUMBER = 9000

STATION = 'CSN_client'

# For FinDer
from stompy.simple import Client # pip install stompy


ACTIVEMQ_SERVER = 'xxx.yyy.zzz.aaa'
STOMP_PORT = 61613
ACTIVEMQ_TOPIC = '/topic/csn.data'
ACTIVEMQ_USERNAME = 'XXXXXXXX'
ACTIVEMQ_PASSWORD = 'YYYYYYYY'


queue = Queue.Queue()

heartbeats = True
ntp = True
web = True
check_storage = True
health = True

class PyCSN(object):

    def connectActiveMQ(self):
        try:
            stomp = Client(host=ACTIVEMQ_SERVER, port=STOMP_PORT)
            stomp.connect(username=ACTIVEMQ_USERNAME, password=ACTIVEMQ_PASSWORD)
        except Exception, err:
            logging.error('Error %s', err)
            stomp = None
        return stomp

    def webServerThread(self):
        try:
            #Create a web server and define the handler to manage the
            #incoming request
            server = HTTPServer(('', PORT_NUMBER), PyCSNWebServer)
    
            server.serve_forever()
 
        except Exception, errtxt:
            server.socket.close()
            logging.error('Web server thread error %s', errtxt)
            web = False
        
        server.close()
        logging.warn('Web server thread terminated') 

    def ntpThread(self):
        ntp_client = ntplib.NTPClient()
        # we start off needing to accumulate points for the line fit quickly
        startup_seconds = 1
        ntp_interval = startup_seconds
        
        line_fit_m = 0.0
        line_fit_c = 0.0
        
        x_points = []
        y_points = []
        
        base_time = time.time()
        start_time = base_time
        last_fit_point_used = base_time
        while ntp:
            if ntp_interval < NTP_INTERVAL:
                startup_seconds += 2
                ntp_interval = startup_seconds
            else:
                ntp_interval = ntp_interval
                
            response = None
            
            for time_server in self.time_servers:
                try:
                    start_time = time.time()
                    response = ntp_client.request(time_server, version=3)
                    break
                except:
                    logging.warn('NTP: No response from %s',time_server)
              
            # if there is a response, and the offset is not ridiculous, add it to the fit
            # (a ridiculous offset can be caused if the NTP server is broken, for example)        
            if response: 
                if abs(response.offset) < MAX_OFFSET_ALLOWED : 
                    # add a point to the line for fitting
                    diff_epoch = (start_time-base_time)
                    
                    # after initial point accumulation, reject offsets that are way off the prediction
                    
                    good_point = True
                    
                    if len(y_points) >= OUTLIER_DETECTION_POINTS_TIME_FIT:
                        #logging.info('New offset value %10.8f Previous %s', response.offset, y_points)
                        mean = sum(y_points) / len(y_points) #numpy.mean(y_points)
                        sigma = math.sqrt(sum((x-mean)**2 for x in y_points) / len(y_points)) #numpy.std(y_points)
                        
                        sigma = max(MIN_SIGMA, sigma)
                        
                        ksigma = abs(response.offset-mean)/sigma
                        if ksigma > 5.0:
                            outlier_elapsed = time.time() - last_fit_point_used 
                            if outlier_elapsed < OUTLIER_ALLOWED_TIME:
                                good_point = False
                            else:
                                logging.info('NTP offset has kSigma %s but will be included as last fit point was %s seconds ago', ksigma, outlier_elapsed) 
                        #logging.info('New point %10.8f Mean %10.8f Sigma %10.8f kSigma %10.8f Good=%s', response.offset, mean, sigma, ksigma, good_point)
                            
                    if good_point: 
                        last_fit_point_used = time.time()
                        x_points.append(diff_epoch)
                        y_points.append(response.offset)
                        
                        if len(x_points) > MIN_POINTS_TIME_FIT:
                            
                            while len(x_points) > MAX_POINTS_TIME_FIT:
                                x_points = x_points[1:]
                                y_points = y_points[1:]
                            
                            # get the current estimate
                            offset_estimate = diff_epoch*line_fit_m + line_fit_c
                            logging.info('Time server %s Reports Offset: %10.6f Fit Estimate: %10.6f Diff: %8.6fs',\
                                         time_server,response.offset,offset_estimate,response.offset-offset_estimate)
                            
                            # do the line fit to solve for y = mx+c
                            x_mean = sum(x_points) / len(x_points) #numpy.mean(x_points)
                            y_mean = sum(y_points) / len(y_points) #numpy.mean(y_points)
                            sum_xy = 0.0
                            sum_x2 = 0.0
                            for x,y in zip(x_points, y_points):
                                sum_xy += (x-x_mean)*(y-y_mean)
                                sum_x2 += (x-x_mean)*(x-x_mean)
                            line_fit_m = sum_xy / sum_x2
                            line_fit_c = y_mean - (line_fit_m*x_mean)
                                            
                        for sensor in self.sensors.values():
                            sensor.setTimingFitVariables(base_time, line_fit_m, line_fit_c)
                else:
                    logging.warning('Time server %s reports excessive offset of %s - ignored', time_server, response.offset)
                    
            elapsed = time.time() - start_time
            if ntp_interval > elapsed:
                time.sleep(ntp_interval-elapsed) 
        logging.warn('NTP thread terminated')   

    def storageThread(self):
        # this thread periodically checks the storage space available in the Sensor's data directory,
        # and cleans out older files when the storage space becomes more full than a threshold
        # If the storage is more than PERCENT_STORAGE_STOP_DATA_THRESHOLD full, then data collection
        # into files is stopped, until such time as it falls back below PERCENT_STORAGE_USED_THRESHOLD 
        while(check_storage):

                for sensor in self.sensors.values():
                    if sensor.datastore_uploaded:                                
                        # get percentage disk space used
                        usage = disk_usage(sensor.datastore_uploaded)
                        percent = 100. * float(usage.used) / float(usage.total)
                        logging.info('%s%% used storage', percent)
                    
                        if percent > PERCENT_STORAGE_USED_THRESHOLD:
                            # remove oldest files from uploaded directories
                            while percent > PERCENT_STORAGE_USED_THRESHOLD:
                                files = os.listdir(sensor.datastore_uploaded)
                                if len(files) <= 0:
                                    logging.info('No more files in %s to remove', sensor.datastore_uploaded)
                                    break
                                files.sort(key=lambda x: os.path.getmtime(os.path.join(sensor.datastore_uploaded,x)))
                                fullname = os.path.join(sensor.datastore_uploaded,files[0])
                                if not os.path.isfile(fullname):
                                    logging.error('Unexpected %s', fullname)
                                    break 
                                try:
                                    os.remove(fullname)
                                    logging.info('Storage cleanup, removed %s', fullname)
                                except:
                                    logging.warn('Exception removing file %s', fullname)
                                    break
                                usage = disk_usage(sensor.datastore_uploaded)
                                percent = 100. * float(usage.used) / float(usage.total)
                            
                            # Now check if the datastore storage used is still so great that data collection should stop
                            usage = disk_usage(sensor.datastore)
                            percent = 100. * float(usage.used) / float(usage.total)
                            if percent > PERCENT_STORAGE_STOP_DATA_THRESHOLD and sensor.collect_samples:
                                logging.warning('Disk space %s used: will stop data collection', percent)
                                sensor.StopSampleCollection()
                                
                            logging.info('Finished storage cleaning, percent usage %s', percent)
                                
                        elif not sensor.collect_samples:
                            logging.warning('Disk space %s used: will resume collecting samples')
                            sensor.StartSampleCollection()
 
                time.sleep(STORAGE_CHECK_INTERVAL.total_seconds())
        logging.warn('storage thread terminated')
        return
    
    def heartbeatThread(self):
        # This uploads the YAML file to Amazon every HEARTBEAT_INTERVAL
        while(heartbeats):
            start_time = datetime.datetime.utcnow()

            try:
                logging.info('Uploading data files ...')
                self.uploadDataFiles()
            except IOError, e:
                logging.error("IO Error on heartbeat %s %s", e.errno, e)
            except Exception, e:
                logging.error('Unexpected error on send_heartbeat %s', e)
            try:    
                station_yaml = STATION + '.yaml'      
                self.to_file(station_yaml)
                # We upload the client's configuration file to Amazon
                key_name = self.client_id + '.yaml'             
                key = self.bucket_stationdata.new_key(key_name)       
                key.set_contents_from_filename(station_yaml)
                logging.info('File %s was uploaded as %s', station_yaml, key_name)
                      
            except Exception, e:
                logging.error('Unexpected error writing new YAML file %s', e)
            elapsed = datetime.datetime.utcnow() - start_time
            #print 'Finished sending heartbeats after ',elapsed
            if HEARTBEAT_INTERVAL > elapsed:
                time.sleep((HEARTBEAT_INTERVAL-elapsed).total_seconds())
        logging.warn('Heartbeat thread terminated')

    def to_file(self, filename):
        """Shorthand for generating a dictionary and writing it to a file."""
        with self.metadata_lock:
            config = self.to_dict()
            with open(filename, 'w') as config_file:
                config_file.write(yaml.dump(config, default_flow_style=False))
     

                
    def to_dict(self):
        """Generates a dictionary of the client"""
        with self.metadata_lock:
            details = {}
            for field in CLIENT_FIELDS:
                if hasattr(self, field):
                    details[field] = getattr(self, field)
            if self.sensors:
                details['sensors'] = []
                for sensor_id, sensor in self.sensors.items():
                    sensor_details = sensor.to_dict()
                    sensor_details['sensor_id'] = sensor_id
                    details['sensors'].append(sensor_details)

            return details

        
    def GetLatestSamples(self):
        # Retrieves the latest samples from the (main) Phidget
        return list(self.phidget_sensor.data_buffer)
    
    def GetDataFiles(self, time_from, time_to, latest=False):
        
        files_to_send = []
        
        for sensor in self.sensors.values():
            if not sensor.datastore_uploaded:
                logging.info('Sensor upload directory does not exist')
                continue
                                            
            files = os.listdir(sensor.datastore_uploaded)
            if len(files) <= 0:
                logging.info('No files in %s', sensor.datastore_uploaded)
                return None
            logging.info('There are %d files in the uploaded directory', len(files))
                
            files.sort(key=lambda x: os.path.getmtime(os.path.join(sensor.datastore_uploaded,x)))
            
            if latest:
                # simply return the latest
                fullname = os.path.join(sensor.datastore_uploaded, files[-1])
                if not os.path.isfile(fullname):
                    logging.error('Unexpected error for %s', fullname)
                    return None
                files_to_send.append(fullname)
                file_start_string = files[-1].split('_')[1].split('.')[0]
                temp_name = file_start_string + '.dat'
                outfilename = os.path.join(TEMPORARY_FILE_DIRECTORY,temp_name)
            else:   
                 
                logging.info('Time from %s to %s', time_from, time_to)
                temp_name = datetime.datetime.strftime(time_from,FILESTORE_NAMING)+\
                'to'+\
                datetime.datetime.strftime(time_to,FILESTORE_NAMING)+\
                '.dat'
                outfilename = os.path.join(TEMPORARY_FILE_DIRECTORY,temp_name)
                
                for this_file in files:
                    file_start_string = this_file.split('_')[1].split('.')[0]
    
                    file_start_datetime = datetime.datetime.strptime(file_start_string,FILESTORE_NAMING)
                    # the files are nominally 10 minutes long, so find the nominal end time
                    file_end_datetime = file_start_datetime + datetime.timedelta(seconds=600)
                    #logging.info(str(file_start_datetime))
                    if file_start_datetime < time_from and file_end_datetime < time_from:
                        continue
                    if file_start_datetime > time_to:
                        continue
                  
                    logging.info('Found file '+file_start_string+' '+str(file_start_datetime)+' '+str(file_end_datetime))
    
                    fullname = os.path.join(sensor.datastore_uploaded, this_file)
                    if not os.path.isfile(fullname):
                        logging.error('Unexpected error for %s', fullname)
                        continue
                    files_to_send.append(fullname)

            if len(files_to_send) > 0:
                logging.info(files_to_send)

                with open(outfilename, 'w') as fout:
                    for file_name in files_to_send:
                        logging.info('Adding %s',file_name)
                        with open(file_name,'r') as fin:
                            for line in fin:
                                fout.write(line)
                        fin.close()
                fout.close()
                return outfilename    
            
        return None

    def GetRecentPicks(self):
        # returns the latest picks from the (main) Phidget
        return list(self.phidget_sensor.recent_picks)
    
    def GetSensorNtpCorrectedTimestamp(self):
        # returns the corrected time stamp for the (main) Phidgets sensor
        return self.phidget_sensor.GetNtpCorrectedTimestamp()
    
    def SendClientUpdate(self):
        logging.error('SendClientUpdate not implemented')

       
    
    def __init__(self):
        
        print 'PyCSN Amazon Starting!'
        
        log = logging.getLogger('')
        log.setLevel(logging.INFO)
        
        the_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        
        logfile = LOGFILE
        if os.path.isdir(LOGFILE_DIRECTORY):
            logfile = os.path.join(LOGFILE_DIRECTORY, LOGFILE)
        
        print 'Will log to', logfile
        
        fh = logging.handlers.RotatingFileHandler(logfile, maxBytes=BYTES_IN_LOGFILE, backupCount=LOGFILE_BACKUPS)
        fh.setFormatter(the_format)
        log.addHandler(fh)        

        if sys.platform == 'win32':
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(the_format)
            log.addHandler(ch)

        try:
            # is_secure from:
            # https://stackoverflow.com/questions/28115250/boto-ssl-certificate-verify-failed-certificate-verify-failed-while-connecting
            self.s3 = boto.connect_s3(aws_access_key_id, aws_secret_access_key, is_secure=False) #, debug=2)
            self.bucket = self.s3.get_bucket(aws_bucket_wavedata)
            self.bucket_stationdata = self.s3.get_bucket(aws_bucket_stationdata)
       
        except Exception, errtxt:
            logging.error('Error connecting to Amazon S3/SQS %s', errtxt)
            #raise sys.exit(errtxt)
        
        
        current_directory = os.path.dirname(os.path.realpath(__file__))   
           
        self.stationFile = os.path.join(current_directory, STATION + '.yaml')
        
        logging.info('Instantiating client from %s', self.stationFile)
        
        with open(self.stationFile) as config_file:
            config = yaml.load(config_file.read())
            
        for field in CLIENT_FIELDS:
            setattr(self, field, config.get(field))
                                 
        self.registered = True
            
        self.server = TARGET_SERVER

        self.client_id = config.get('client_id', 'T100001')
        
        time_servers = config.get('time_servers','') 
        if len(time_servers) == 0:
            self.time_servers = TIME_SERVERS
        else:
            if not type(time_servers) is list:
                time_servers = [time_servers]
            servers = set(TIME_SERVERS) - set(time_servers)
            self.time_servers = time_servers + list(servers)
            logging.info('Station configuration includes NTP server(s) %s: Will use %s', time_servers, self.time_servers)

        logging.info('Connecting to FinDer ActiveMQ broker')
        stomp = self.connectActiveMQ()
                           
        if stomp == None:
            logging.error('No connection to ActiveMQ')

        
        # Create locks for changing metadata
        self.metadata_lock = threading.RLock()
            
        self.software_version = SOFTWARE_VERSION

        self.sensors = {}
        
        self.phidget_sensor = None
        
        for sensor_dict in config.get('sensors', []):
            sensor_id = sensor_dict.get('sensor_id')
            if sensor_id is not None:
                # include the SQS queue for picks in the sensor's configuration
                # as well as the lat/lon/floor
                sensor_dict['software_version'] = SOFTWARE_VERSION
                sensor_dict['stomp'] = stomp
                sensor_dict['stomp_topic'] = ACTIVEMQ_TOPIC
                sensor_dict['connect_stomp'] = self.connectActiveMQ
                #sensor_dict['pick_queue'] = self.pick_queue
                sensor_dict['latitude'] = config.get('latitude')
                sensor_dict['longitude'] = config.get('longitude')
                sensor_dict['floor'] = config.get('floor')
                sensor_dict['client_id'] = self.client_id
                self.sensors[sensor_id] = PhidgetsSensor(config=sensor_dict)
                # Store reference to the first sensor (and usually the only one
                if self.phidget_sensor is None:
                    self.phidget_sensor = self.sensors[sensor_id]
                

        if len(self.sensors) == 0:
            logging.error("Client has no sensors")
            return
        
        for sensor_id, sensor in self.sensors.items():
            logging.info('Client has Sensor %s',sensor_id)
            if not sensor.phidget_attached:
                raise sys.exit('Phidget not attached - abort')                
            else:    
                sensor.setFileStoreInterval(HEARTBEAT_INTERVAL.total_seconds()) 

        
        # we start off assuming no clock drift i.e. the adjustment is zero
        
        self.clock_offset = 0.0
        
        logging.info('Starting NTP thread')
        try:
            ntpDaemon = threading.Thread(target=self.ntpThread)
            ntpDaemon.setDaemon(True)
            ntpDaemon.start()
        except Exception, errtxt:
            raise sys.exit(errtxt)
    
        logging.info('Starting heartbeat thread')
        try:
            heartbeatDaemon = threading.Thread(target=self.heartbeatThread)
            heartbeatDaemon.setDaemon(True)
            heartbeatDaemon.start()
        except Exception, errtxt:
            raise sys.exit(errtxt)
        
        logging.info('Starting web server on port %s', PORT_NUMBER)
        try:
            webServerDaemon = threading.Thread(target=self.webServerThread)
            webServerDaemon.setDaemon(True)
            webServerDaemon.start()
        except Exception, errtxt:
            raise sys.exit(errtxt)

        logging.info('Starting storage check thread')
        try:
            storageDaemon = threading.Thread(target=self.storageThread)
            storageDaemon.setDaemon(True)
            storageDaemon.start()
        except Exception, errtxt:
            raise sys.exit(errtxt)
        
     
        
        signal.signal(signal.SIGTERM, self.catch_sigterm)
         
    def run(self):

        HEALTH_TIME = 60
        self.server_start_time = time.time()
        logging.info('Running ...')
        try:
            health = True
            while health:
               
                time.sleep(HEALTH_TIME)
                time_now = self.GetSensorNtpCorrectedTimestamp()
        
                for sensor_id, sensor in self.sensors.items():
                    if sensor.collect_samples and time_now - sensor.last_sample_seconds > 600:
                        logging.warn('Health Check: Sensor %s No sensor data for %s seconds. Restart Phidget', sensor_id, time_now - sensor.last_sample_seconds )                      
                        sensor.StartPhidget()
                    elif not sensor.phidget_attached:
                        logging.warn('Health Check: Sensor %s Not attached. Restart Phidget',sensor_id)
                        sensor.StartPhidget()
                
                if not heartbeats:
                    logging.warning('Health Check: Heartbeat thread has stopped')
                if not ntp:
                    logging.warning('Health Check: NTP thread has stopped')
                if not web:
                    logging.warning('Health Check: Web thread has stopped')
                if not check_storage:
                    logging.warning('Health Check: Check storage thread has stopped')
                    
                                                            
            logging.warn('Main thread terminating')        
        except KeyboardInterrupt:
            logging.warn('Keyboard Interrupt: PyCSN processing terminating')
            
        self.cleanup()
        
    def catch_sigterm(self, signum, frame):
        logging.warning('Caught SIGTERM %s %s - Calling the cleaner', signum, frame)
        self.cleanup()
        
    def cleanup(self):
        heartbeats = False
        ntp = False
        web = False
        check_storage = False
        health = False
        
        # Stop sample collection from all sensors
        logging.info('Stopping Sensor Data Collection and uploading files')
        for sensor in self.sensors.values():
            sensor.StopSampleCollection()
        # process any files that need to be uploaded
        try:
            self.uploadDataFiles()
        except IOError, e:
            logging.error("IO Error on uploadDataFiles %s %s", e.errno, e)
        except Exception, e:
            logging.error('Unexpected error on uploadDataFiles %s', e)

        sys.exit()


    def uploadDataFiles(self):
                
        for sensor in self.sensors.values():
            pending_uploads = sensor.datafiles_not_yet_uploaded()
            if len(pending_uploads) > 0:
                logging.info('There are %s pending data files to upload', len(pending_uploads))
                pending_count = 0 
                for filename in pending_uploads:
                    if not filename.endswith('.dat'):
                        continue                   
                    """
                    filename_zip = filename + '.bz2'
                    output = bz2.BZ2File(filename_zip, 'wb')
                    infile = open(filename, 'rb')
                    output.write(infile.read())
                    output.close()
                    infile.close()
                    """
                    filename_zip = filename + '.gz'
                    infile = open(filename, 'rb')
                    with gzip.open(filename_zip, 'wb') as f:
                        f.write(infile.read())
                    f.close()
                    infile.close()
                    filename_day = os.path.basename(filename).split('_')[1][:8] # extract the YYYYMMDD part  
            
                    key_name = filename_day + '/' + self.client_id + '/' + os.path.basename(filename_zip)
                    
                    try:                    
                        key = self.bucket.new_key(key_name)       
                        key.set_contents_from_filename(filename_zip)
                        logging.info('File %s was uploaded as %s', filename, key_name)
                        sensor.mark_file_uploaded(filename)
                        os.remove(filename_zip)
                    except Exception, e:
                        logging.error('Error uploading file %s to Amazon as %s: %s', filename, key_name, e)
                    
                    pending_count += 1
                    if pending_count >= MAX_PENDING_UPLOADS:
                        logging.warn('Limit of %s pending uploads added', pending_count)
                        break

        

class PyCSNWebServer(BaseHTTPRequestHandler):

    TIMESTAMP_NAMING = "%Y-%m-%dT%H:%M:%S"
    TIMESTAMP_NAMING_PLOT = "[%-H, %-M, %-S]"
    TIMESTAMP_JSON = "%Y%m%d %H:%M:%S.%f"
    TIMESTAMP_NAMING_PRETTY = "2000-09-21T23:20:00"
    SAMPLES_TO_SEND = 500
    
    def log_message(self, format, *args):
        # uncomment following to enable Web log entries
        #logging.info("%s - - [%s] %s" % (self.address_string(),self.log_date_time_string(),format%args))
        return
    
    def send_message(self, message):
        self.send_response(200)
        self.send_header('Content-type','text/html')
        self.end_headers()
        # Send the html message
        self.wfile.write("<body>")

        self.wfile.write("<h1>CSN Phidgets Client</h1>")
        self.wfile.write(message)
        return

    #Handler for the GET requests
    def do_GET(self):
        
        global this_client

        try:
            if self.path == '/favicon.ico':
                return
            
            elif self.path == '/uptime':
                self.send_response(200)
                self.send_header('Content-type','text/html')
                self.end_headers()
                
                elapsed_time = time.time() - this_client.server_start_time
                
                uptime = datetime.timedelta(seconds=elapsed_time)
                self.wfile.write("Uptime "+str(uptime))
                return
            
            elif self.path == '/version':
                self.send_response(200)
                self.send_header('Content-type','text/html')
                self.end_headers()
                self.wfile.write(str(SOFTWARE_VERSION))
                return                
        
            elif self.path == '/getdata':
                # The client will send a JSON encoded set of the very latest samples from the sensor
                #logging.info('Request for latest sensor data in JSON format')
                self.send_response(200)
                self.send_header('Content-type','application/json')
                self.end_headers()
    
                latest_samples = this_client.GetLatestSamples()
                if latest_samples == None:
                    self.wfile.write("No Sensor Data available")
                    return
                if len(latest_samples) > self.SAMPLES_TO_SEND:
                    latest_samples = latest_samples[-self.SAMPLES_TO_SEND:]
                json_samples = json.dumps(latest_samples)
                #json_samples = json.dumps([(datetime.datetime.fromtimestamp(t).strftime(self.TIMESTAMP_JSON),acc) for (t,acc) in latest_samples])
                self.wfile.write(json_samples)
                return
            
            elif self.path == '/getlatestsample':
                # The client will send the JSON encoded very latest sample from the sensor
                self.send_response(200)
                self.send_header('Content-type','application/json')
                self.end_headers()
                
                logging.debug('Request for latest sensor reading in JSON format')
    
                latest_samples = this_client.GetLatestSamples()
                if latest_samples == None:
                    latest_sample = []
                else:
                    latest_sample = latest_samples[-1]
                    
    
                json_sample = json.dumps(latest_sample)
                #json_samples = json.dumps([(datetime.datetime.fromtimestamp(t).strftime(self.TIMESTAMP_JSON),acc) for (t,acc) in latest_samples])
                self.wfile.write(json_sample)
                return

            elif self.path == '/getrecentpicks':
                # The client will send the JSON encoded recent picks from the sensor
                self.send_response(200)
                self.send_header('Content-type','application/json')
                self.end_headers()
                
                logging.debug('Request for latest picks in JSON format')
    
                recent_picks = this_client.GetRecentPicks()

                json_sample = json.dumps(recent_picks)
                self.wfile.write(json_sample)
                return                
            
            elif self.path.startswith('/getfiles'):
                # the request is for data files
                requested_times = self.path.split('/')[2:]
                logging.info('Request for files %s, time range %s', self.path, requested_times)
                requested_datetimes = []
                
                for time_string in requested_times:
                    try:
                        time_datestring = datetime.datetime.strptime(time_string, self.TIMESTAMP_NAMING)
                        requested_datetimes.append(time_datestring)
                    except:
                        logging.warning('Invalid time %s', time_string)
                        self.send_message('Invalid time specified "'+time_string+'" should be formatted like '+self.TIMESTAMP_NAMING_PRETTY)
                        return
                data_file_created = None
                if len(requested_datetimes) == 0:
                    # we cannot return the current data file being written, so return the most recently completed
                    from_time = datetime.datetime.now() - datetime.timedelta(seconds=1200)
                    to_time = datetime.datetime.now()
                    logging.info('No dates specified: return latest file')
                    data_file_created = this_client.GetDataFiles(None, None, latest=True)
                elif len(requested_datetimes) == 1:
                    logging.info('Start date specified: return file containing that start date and next')
                    from_time = requested_datetimes[0]
                    to_time = from_time + datetime.timedelta(seconds=600)
                    data_file_created = this_client.GetDataFiles(from_time, to_time)               
                elif len(requested_datetimes) == 2:
                    logging.info('Start and end date specified: return files from start to end and intervening')
                    from_time = requested_datetimes[0]
                    to_time = requested_datetimes[1]
                    if to_time - from_time > datetime.timedelta(days=7):
                        self.send_message('This is more than one week of data - please reduce your request interval')
                        return
                    data_file_created = this_client.GetDataFiles(from_time, to_time)                
                else:
                    logging.warning('Too many date/time objects')
                    self.send_message('Too many date/time fields in request')
                    return
                if data_file_created is not None:
                    base_name = os.path.basename(data_file_created) 
                    zip_file_name = data_file_created+'.zip'
                    base_zip_name = os.path.basename(zip_file_name)
                    logging.info('Creating zip file %s from %s', base_zip_name, data_file_created)
    
                    self.send_response(200)
                    self.send_header('Content-type','application/octet_stream')
                    self.send_header('Content-Disposition','attachment; filename='+base_zip_name)
                    self.end_headers()
    
                    logging.info('Name %s',zip_file_name)
                    try:
                        zip_file =  zipfile.ZipFile(zip_file_name, 'w')
                        zip_file.write(data_file_created, base_name, compress_type=zipfile.ZIP_DEFLATED)
                        zip_file.close()
                        self.wfile.write(open(zip_file_name,'rb').read())
                    except:
                        logging.error('Error writing zip %s', zip_file_name)
                    finally:
                        os.remove(zip_file_name)
                        os.remove(data_file_created)
                        logging.info('Sent zip and cleaned up')
                    return
                else:
                    self.send_message('No data found')
                            
                return
            else:
                self.send_response(200)
                self.send_header('Content-type','text/html')
                self.end_headers()
                # Send the html message
                self.wfile.write("<body>")
                self.wfile.write("<h1>CSN Phidgets Client</h1>")
        
                if self.path == '/update':
                    # the client will update itself at the SAF server cloud
                    this_client.SendClientUpdate()
                    self.wfile.write("Client was updated on the server")
                    logging.info('Web server client update request processed')
                    return
                
                # the client will show a web page of plots of the most recent data and picks
                                    
                E_data = N_data = Z_data = ''
                
                latest_samples = this_client.GetLatestSamples()
                if latest_samples == None:
                    self.wfile.write("No Sensor Data available")
                    return
                
                num_samples_available = len(latest_samples)
                
                recent_picks = this_client.GetRecentPicks()
                
                #logging.info('Recent picks %s', recent_picks)
                
                num_seconds_to_plot = 60
                
                #time_now = datetime.datetime.utcnow()
                time_now = this_client.GetSensorNtpCorrectedTimestamp()
                time_from = time_now - num_seconds_to_plot
                       
                num_samples_in_plot = 0
                num_picks_in_plot = 0
                if len(recent_picks) > 0:
                    while len(recent_picks) > 0:
                        this_pick = recent_picks[0]
                        pick_time = this_pick[0]
                        pick_accelerations = this_pick[1]
                        if pick_time < time_from:
                            recent_picks = recent_picks[1:]
                        else:
                            break
                if len(recent_picks) == 0:
                    pick_time = None
        
                latest_time_found = time_from
                earliest_time_found = time_now
                    
                    
                last_accelerations = [0., 0., 0.]
                for sample in latest_samples:
                    timestamp = sample[0]
                    if timestamp < time_from:
                        continue
                    if timestamp > latest_time_found:
                        latest_time_found = timestamp
                    if timestamp < earliest_time_found:
                        earliest_time_found = timestamp
                    if pick_time is not None:
                        while pick_time < timestamp:
                            pick_time_datetime = datetime.datetime.fromtimestamp(pick_time)
                            #timestamp_string = pick_time_datetime.strftime(self.TIMESTAMP_NAMING_PLOT)
                            timestamp_string = '[{d.hour}, {d.minute}, {d.second}, {ms}]'.format(d=pick_time_datetime, ms=pick_time_datetime.microsecond/1000)
                            num_picks_in_plot += 1
                            logging.info('Plot Pick %s %s', pick_time, pick_accelerations)
                            # we use "last_accelerations" otherwise the Z picks are near +-g and effectively rescale Z
                            if pick_accelerations[2] != 0.0:
                                Z_data += '['+timestamp_string+','+str(last_accelerations[2])+', "Pick"],'
                            elif pick_accelerations[0] != 0.0:
                                N_data += '['+timestamp_string+','+str(last_accelerations[0])+', "Pick"],'
                            elif pick_accelerations[1] != 0.0:
                                E_data += '['+timestamp_string+','+str(last_accelerations[1])+', "Pick"],'
                            recent_picks = recent_picks[1:]                    
                            if len(recent_picks) > 0:
                                this_pick = recent_picks[0]
                                pick_time = this_pick[0]
                                pick_accelerations = this_pick[1]
                            else:
                                pick_time = time_now # false pick beyond limits of plot                        
                            
                            
                    #elapsed_seconds = (timestamp - time_from).total_seconds()
                    accelerations = sample[1]
                    last_accelerations = accelerations
                    num_samples_in_plot += 1
                    datetime_timestamp = datetime.datetime.fromtimestamp(timestamp)
                    #timestamp_string = datetime_timestamp.strftime(self.TIMESTAMP_NAMING_PLOT)
                    timestamp_string = '[{d.hour}, {d.minute}, {d.second}, {ms}]'.format(d=datetime_timestamp, ms = datetime_timestamp.microsecond/1000)
        
                    E_data += '['+timestamp_string+','+str(accelerations[0])+', null],'
                    N_data += '['+timestamp_string+','+str(accelerations[1])+', null],'
                    Z_data += '['+timestamp_string+','+str(accelerations[2])+', null],'
                    
        
                        
                logging.info('Plotting %s samples and %s picks', num_samples_in_plot, num_picks_in_plot)
                
                num_seconds_plotted = int(latest_time_found - earliest_time_found)
                
                datetime_time_from = datetime.datetime.fromtimestamp(earliest_time_found)        
                self.wfile.write(str(num_samples_in_plot)+" samples and "+str(num_picks_in_plot)+" picks from "+datetime_time_from.strftime(self.TIMESTAMP_NAMING)+" for "+str(num_seconds_plotted)+" seconds")         
                
                page_str="""
        
            <script type="text/javascript" src="https://www.google.com/jsapi"></script>
            <script type="text/javascript">
              google.load("visualization", "1", {packages:["corechart"]});
              google.setOnLoadCallback(drawChart);
              function drawChart() {
                var E_data = new google.visualization.DataTable();
                E_data.addColumn('timeofday','Time');
                E_data.addColumn('number','Acceleration');
                E_data.addColumn({type: 'string', role: 'annotation'});
                E_data.addRows([ %s ]);
                
                var N_data = new google.visualization.DataTable();
                N_data.addColumn('timeofday','Time');
                N_data.addColumn('number','Acceleration');
                N_data.addColumn({type: 'string', role: 'annotation'});
                N_data.addRows([ %s ]);
        
                var Z_data = new google.visualization.DataTable();
                Z_data.addColumn('timeofday','Time');
                Z_data.addColumn('number','Acceleration');
                Z_data.addColumn({type: 'string', role: 'annotation'});
                Z_data.addRows([ %s ]);
        
                var E_options = {
                  title: 'CSN Phidget Sensor Accelerations: E',
                  legend: {position: 'none'},
                  hAxis: {title: 'Time', titleTextStyle: {color: 'blue'},  
                  gridlines: {count: 10}},
                  vAxis: {title: 'Acceleration (g)', titleTextStyle: {color: 'blue'},
                  gridlines: {count: 5}}
                };
                var N_options = {
                  title: 'CSN Phidget Sensor Accelerations: N',
                  legend: {position: 'none'},
                  hAxis: {title: 'Time', titleTextStyle: {color: 'blue'},
                  gridlines: {count: 10}} ,
                  vAxis: {title: 'Acceleration (g)', titleTextStyle: {color: 'blue'},
                  gridlines: {count: 5}}
                };
                var Z_options = {
                  title: 'CSN Phidget Sensor Accelerations: Z',
                  legend: {position: 'none'},
                  hAxis: {title: 'Time', titleTextStyle: {color: 'blue'},
                  gridlines: {count: 10}},
                  vAxis: {title: 'Acceleration (g)', titleTextStyle: {color: 'blue'},
                  gridlines: {count: 5}}
                };
                
                var E_chart = new google.visualization.LineChart
                        (document.getElementById('E_chart_div'));
                var N_chart = new google.visualization.LineChart
                        (document.getElementById('N_chart_div'));
                var Z_chart = new google.visualization.LineChart
                        (document.getElementById('Z_chart_div'));                
                E_chart.draw(E_data, E_options);
                N_chart.draw(N_data, N_options);
                Z_chart.draw(Z_data, Z_options);
                
              }
            </script>
            <div id="E_chart_div"></div>
            <div id="N_chart_div"></div>
            <div id="Z_chart_div"></div>
            </body>
            """ % (E_data, N_data, Z_data)
                
                self.wfile.write(page_str)
                
                return
        except Exception, e:
            logging.error('Web server do_get error %s',e)
        return


def main():
    global this_client
    
    try:
        this_client = PyCSN()
        this_client.run()
    except Exception, e:
        logging.error('Error starting client: %s', e)


if __name__ == '__main__': main()
