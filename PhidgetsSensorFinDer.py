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
# coding=utf-8

"""Phidgets CSN Sensor"""
#import sys
import os
import errno
import shutil
import math

import datetime
import time

import logging
import threading
import Queue
import json

#import boto.sqs
#from boto.sqs.message import Message


from Phidgets import Phidget
from Phidgets.PhidgetException import PhidgetErrorCodes, PhidgetException
from Phidgets.Events.Events import AccelerationChangeEventArgs, AttachEventArgs, DetachEventArgs, ErrorEventArgs
from Phidgets.Devices.Spatial import Spatial, SpatialEventData, TimeSpan

# For FinDer


G_TO_CMS2 = 981.0
TIMESTAMP_NAMING = "%Y-%m-%dT%H:%M:%S.%f"



RANDOM_WAIT_TIME = 10
MINIMUM_WAIT_TIME = 2

# our special order CSN Phidgets are scaled to +/- 2g instead of +/- 6g
PHIDGETS_ACCELERATION_TO_G = 1.0/3.0
# we request samples at 250 per second
PHIDGETS_NOMINAL_DATA_INTERVAL_MS = 4
PHIDGETS_NOMINAL_DATA_INTERVAL = 0.001 * PHIDGETS_NOMINAL_DATA_INTERVAL_MS
# We decimate to 50=5 or 250=1 samples per second
PHIDGETS_DECIMATION = 1

LTA = 10.0
STA = 0.5
Gap = 1.0
KSIGMA_THRESHOLD = 2.5

# picker interval is the expected interval between sample timestamps sent to the picker
PICKER_INTERVAL = 0.02
PICKER_THRESHOLD = 0.0025

RECENT_PICKS_COUNT = 100

MINIMUM_REPICK_INTERVAL_SECONDS = 1.0
PHIDGETS_RESAMPLED_DATA_INTERVAL_MS = PHIDGETS_NOMINAL_DATA_INTERVAL_MS * PHIDGETS_DECIMATION
FILESTORE_NAMING = "%Y%m%dT%H%M%S"

class PhidgetsSensor(object):

    def __init__(self, config=None):
        if not config:
            config = {}
        self.phidget_attached = False
        self.collect_samples = True
        self.picker = True
        self.sensor_data_lock = threading.Lock()

        self.delta_fields = set()
        self.sensor_data = {}
        self.sensor_data['Type'] = 'ACCELEROMETER_3_AXIS'
        self.sensor_data['Calibrated'] = True
        self.sensor_data['Model'] = 'Phidgets 1056'
        self.decimation = PHIDGETS_DECIMATION
        if 'software_version' in config:
            self.software_version = config.get('software_version')
        else:
            self.software_version = 'PyCSN Unknown'
        if 'decimation' in config:
            self.decimation = config.get('decimation')
        if 'picker' in config:
            self.picker = config.get('picker')
        if 'pick_threshold' in config:
            self.pick_threshold = config.get('pick_threshold')
        else:
            self.pick_threshold = PICKER_THRESHOLD
        if 'serial' in config:
            self.sensor_data['Serial'] = config.get('serial')
        else:
            self.sensor_data['Serial'] = ''
        if 'datastore' in config:
            self.datastore = config.get('datastore')
        else:
            self.datastore = '/var/tmp/phidgetsdata'
            
        #if 'pick_queue' in config:
        #    self.pick_queue = config.get('pick_queue')
        #else:
        #    self.pick_queue = None
            
        if 'latitude' in config:
            self.latitude = config.get('latitude')
        else:
            self.latitude = 0.0
        if 'longitude' in config:
            self.longitude = config.get('longitude')
        else:
            self.longitude = 0.0
        if 'floor' in config:
            self.floor = config.get('floor')
        else:
            self.floor = '1'
        
        if 'client_id' in config:
            self.client_id = config.get('client_id')
        else:
            self.client_id = 'Unknown'
            
        if 'stomp' in config:
            self.stomp = config.get('stomp')
        else:
            self.stomp = None
            
        if 'stomp_topic' in config:
            self.stomp_topic = config.get('stomp_topic')
        else:
            self.stomp_topic = None
            
        if 'connect_stomp' in config:
            self.connect_stomp = config.get('connect_stomp')
        else:
            self.connect_stomp = None
                    
        self.datastore_uploaded = self.datastore + '/uploaded'
        self.datastore_corrupted = self.datastore + '/corrupted'
        logging.info('Sensor datastore directory is %s',self.datastore)
            
        # Make sure data directory exists
        try:
            os.makedirs(self.datastore)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                logging.error('Error making directory %s', self.datastore)
                raise exception      
        try:
            os.makedirs(self.datastore_uploaded)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                logging.error('Error making directory %s', self.datastore_uploaded)
                raise exception
        try:
            os.makedirs(self.datastore_corrupted)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                logging.error('Error making directory %s', self.datastore_corrupted)
                raise exception            
         

        self.sensor_data['Units'] = 'g'
        self.sensor_data['Num_samples'] = 50
        self.sensor_data['Sample_window_size'] = 1
        self.accelerometer = None
        self.time_start = None
        self.reported_clock_drift = 0.0
        self.file_store_interval = 600.0
        self.last_phidgets_timestamp = None
        self.last_datastore_filename = None
        self.last_datastore_filename_uploaded = None
        self.writing_errors = 0
        self.last_sample_seconds = time.time()
        # a lock for updating the timing variables
        self.timing_lock = threading.Lock()
        
        self.timing_gradient = 0.0
        self.timing_intercept = 0.0
        self.timing_base_time = time.time()
        
        self.recent_picks = []
    
        # Everything looks OK, start the collect samples and picking threads and the Phidget

        self.sensor_raw_data_queue = Queue.Queue()
        
        raw_data_thread = threading.Thread(target=self.ProcessSensorReading, args=['raw'])
        raw_data_thread.setDaemon(True)
        raw_data_thread.start()
        
        self.sensor_raw_data_queue.join()
        logging.info('Raw Data Thread started')
          
        # start the Picker thread, if this is required
        if self.picker:
            
            self.sensor_readings_queue = Queue.Queue()      
            thread = threading.Thread(target=self.Picker, args=['Simple Threshold'])
            thread.setDaemon(True)
            thread.start()
            logging.info('Picker started')        
            self.sensor_readings_queue.join()
            logging.info('Sensor readings thread started')
            

 
        else:
            logging.info('This Phidget will not calculate or send picks')

        
        logging.info('Client initialised: now starting Phidget')
        
        self.data_buffer = []
         
        # Start the Phidget       
        self.StartPhidget()
        
        if not self.phidget_attached:
            raise
                 
        self.sensor_data['Serial'] = str(self.accelerometer.getSerialNum())
       
        self.DisplayDeviceInfo()


       
    
    def StartPhidget(self):
        # Initialise the Phidgets sensor
      
        self.phidget_attached = False
        self.time_start = None
        
        try:
            if self.accelerometer:
                self.accelerometer.closePhidget()
            
            self.accelerometer = Spatial()
            self.accelerometer.setOnAttachHandler(self.AccelerometerAttached)
            self.accelerometer.setOnDetachHandler(self.AccelerometerDetached)
            self.accelerometer.setOnErrorhandler(self.AccelerometerError)
            self.accelerometer.setOnSpatialDataHandler(self.SpatialData)
            
            #self.accelerometer.enableLogging(Phidget.PhidgetLogLevel.PHIDGET_LOG_WARNING, None)
            #self.accelerometer.enableLogging(Phidget.PhidgetLogLevel.PHIDGET_LOG_VERBOSE, None)
           
            self.accelerometer.openPhidget()
            
            self.accelerometer.waitForAttach(10000)
            
            # set data rate in milliseconds (we will decimate from 4ms to 20ms later)
            # we now do this in the Attach handler              
            #self.accelerometer.setDataRate(PHIDGETS_NOMINAL_DATA_INTERVAL_MS)

        except RuntimeError as e:
            logging.error("Runtime Exception: %s", e.details)
            return
        except PhidgetException as e:
            logging.error("Phidget Exception: %s. Is the Phidget not connected?", e.details)
            return
        
        self.phidget_attached = True
    
    def ProcessSensorReading(self, args):
        # handles incoming samples from the Phidgets sensor 
        
        logging.info('Initialise ProcessSensorReading')
        
        while True:
            e, sample_timestamp = self.sensor_raw_data_queue.get()
            
            if self.collect_samples:
                
                if self.time_start == None:
                    self.time_start = sample_timestamp
                    self.last_timestamp_sent_to_picker = 0
                    
                    self.last_sample_seconds = self.time_start
                    self.sample_count = 0
                    self.accelerations = []
        
                    self.datastore_filename = self.MakeFilename(sample_timestamp)
                    logging.info('Storing samples to %s',self.datastore_filename)
                    self.datastore_file = open(self.datastore_filename, 'w')
                    self.first_sample_timestamp_in_file = None
                    self.last_phidgets_timestamp = 0.0
                    
                for index, spatialData in enumerate(e.spatialData):
                    phidgets_timestamp = spatialData.Timestamp.seconds + (spatialData.Timestamp.microSeconds * 0.000001)
                    # the accelerations are scaled and reordered to conform to the SAF standard of [N, E, Z]
                    accs = [PHIDGETS_ACCELERATION_TO_G*spatialData.Acceleration[1],\
                            PHIDGETS_ACCELERATION_TO_G*spatialData.Acceleration[0],\
                            PHIDGETS_ACCELERATION_TO_G*spatialData.Acceleration[2]]
        
                    # we keep a running sum of accelerations in self.accelerations, which we will use to average later
                    if len(self.accelerations) == 0:
                        self.accelerations = accs
                    else:
                        for i in range(len(self.accelerations)):
                            self.accelerations[i] += accs[i]
                    
                    sample_increment = 1
                    
                    if self.last_phidgets_timestamp:
                        sample_increment = int(round( (phidgets_timestamp - self.last_phidgets_timestamp) / PHIDGETS_NOMINAL_DATA_INTERVAL))
                        if sample_increment > 4*self.decimation:
                            logging.warn('Missing >3 samples: last sample %s current sample %s missing samples %s',\
                                         self.last_phidgets_timestamp, phidgets_timestamp, sample_increment)
                        elif sample_increment == 0:
                            logging.warn('Excess samples: last sample %s current sample %s equiv samples %s',\
                                         self.last_phidgets_timestamp, phidgets_timestamp, sample_increment)
                        
                    self.last_phidgets_timestamp = phidgets_timestamp
                               
                    self.last_sample_seconds = sample_timestamp
                   
                    self.sample_count += 1
                    
                    if self.sample_count >= self.decimation:
                       
                        # we average the last self.decimation samples
                         
                        accelerations = [acc / float(self.decimation) for acc in self.accelerations]
                        
                        #invert channel 1 (E-W) - this results in +1g being reported when the sensor is resting on its E side
                        accelerations[1] = -accelerations[1]
                        
                        data_entry = [sample_timestamp, accelerations]
                        
                        if self.picker:
                            if sample_timestamp - self.last_timestamp_sent_to_picker >= PICKER_INTERVAL:
                                # put this reading in the queue for the Picker
                                self.last_timestamp_sent_to_picker = sample_timestamp
                                self.sensor_readings_queue.put(data_entry)
                        
                        # append this reading to the data buffer
                        # @TODO add lock
                        self.data_buffer.append(data_entry)
                        
                        if self.datastore_file:
                            if self.first_sample_timestamp_in_file == None:
                                self.first_sample_timestamp_in_file = sample_timestamp
                            try:
                                self.datastore_file.write("%20.5f" % sample_timestamp + ' ' +
                                                      ' '.join("%10.7f" % x for x in accelerations)+'\n')
                                self.writing_errors = 0
                            except Exception, e:
                                self.writing_errors += 1
                                if self.writing_errors <= 10:
                                    logging.error('Error %s writing sample to file %s with timestamp %s',e,self.datastore_filename,sample_timestamp)
                    
                        self.sample_count = 0
                        self.accelerations = []
                
                        if sample_timestamp - self.first_sample_timestamp_in_file >= self.file_store_interval:
                            logging.info('File %s store interval elapsed with %s samples, rate %s samples/second',self.datastore_filename,len(self.data_buffer),float(len(self.data_buffer))/self.file_store_interval)
                            # we will change to a new file in the datastore
                            if self.datastore_file:
                                self.datastore_file.close()
                            #logging.info('File closed')
                            self.last_datastore_filename = self.datastore_filename                        
                            self.data_buffer = []
                            self.time_start = None

        logging.error('Raw Data Processing Thread terminated')   

    def Picker(self, args):
        logging.info('Initialise picker %s', args)
        # LTA = 10 seconds
        # Gap = 1 seconds
        # STA = 0.5 seconds
        delta = PHIDGETS_NOMINAL_DATA_INTERVAL_MS * self.decimation * 0.001
        LTA_count = int(LTA / delta)
        accelerations = [[], [], []]
        timestamp = self.GetNtpCorrectedTimestamp()
        last_pick_timestamp = [timestamp, timestamp, timestamp]

        count_readings = 0

        # N, E, Z
        
        while True:
            (timestamp, accs) = self.sensor_readings_queue.get()

            count_readings += 1
            
            for i in range(len(accs)):
                accelerations[i].append(accs[i])
            
                if count_readings < LTA_count:
                    continue
                
                lta_mean = sum(accelerations[i][:LTA_count]) / LTA_count
               
                if timestamp - last_pick_timestamp[i] > MINIMUM_REPICK_INTERVAL_SECONDS:
                    
                    if abs(accs[i] - lta_mean) > self.pick_threshold:
                        # pick based on raw acceleration only
                        last_pick_timestamp[i] = timestamp
                        accs_to_send = [0., 0., 0.]
                        accs_to_send[i] = abs(accs[i]-lta_mean)
                        timestamp_string = "%20.5f" % timestamp
                        logging.info("Pick on axis %s at time %s, RAW G %s", i, timestamp_string, accs_to_send[i])
                        # send pick immediately
                        self.send_event(timestamp, accs_to_send)
                        self.recent_picks.append((timestamp, accs_to_send))
                        while len(self.recent_picks) > RECENT_PICKS_COUNT:
                            self.recent_picks = self.recent_picks[1:]

                
                del accelerations[i][0]    

    def kSigmaPicker(self, args):
        logging.info('Initialise kSigma picker with args %s', args)
        # LTA = 10 seconds
        # Gap = 1 seconds
        # STA = 0.5 seconds
        delta = PHIDGETS_NOMINAL_DATA_INTERVAL_MS * self.decimation * 0.001
        LTA_count = int(LTA / delta)
        STA_count = int(STA / delta)
        Gap_count = int(Gap / delta)
        ksigma_count = LTA_count + Gap_count + STA_count
        accelerations = [[], [], []]
        timestamp = self.GetNtpCorrectedTimestamp()
        last_pick_timestamp = [timestamp, timestamp, timestamp]
        in_pick = [False, False, False]
        max_acceleration = [0., 0., 0.]
        ksigma_at_pick = [0., 0., 0.]


        count_readings = 0

        # N, E, Z
        
        while True:
            (timestamp, accs) = self.sensor_readings_queue.get()

            count_readings += 1
            
            for i in range(len(accs)):
                accelerations[i].append(accs[i])
            
                if count_readings < ksigma_count:
                    continue
                
                lta_mean = sum(accelerations[i][:LTA_count]) / LTA_count
                lta_mean2 = sum([v*v for v in accelerations[i][:LTA_count]]) / LTA_count
                try:
                    lta_sigma = math.sqrt(lta_mean2 - (lta_mean*lta_mean))
                except Exception, e:
                    logging.warning('LTA sigma calculation error %s %s', lta_mean2, lta_mean)
                    lta_sigma = 0.0001
                sta_abs_mean = sum([abs(v-lta_mean) for v in accelerations[i][LTA_count+Gap_count:]]) / STA_count
                ksigma = sta_abs_mean / lta_sigma
                
                
                
                if not in_pick[i] and timestamp - last_pick_timestamp[i] > MINIMUM_REPICK_INTERVAL_SECONDS:
                    
                    if abs(accs[i] - lta_mean) > self.pick_threshold:
                        # pick based on raw acceleration only
                        last_pick_timestamp[i] = timestamp
                        accs_to_send = [0., 0., 0.]
                        accs_to_send[i] = abs(accs[i]-lta_mean)
                        timestamp_string = "%20.5f" % timestamp
                        logging.info("Pick on axis %s at time %s, RAW G %s", i, timestamp_string, accs_to_send[i])
                        # send pick immediately
                        self.send_event(datetime.datetime.fromtimestamp(timestamp), accs_to_send)
                        self.recent_picks.append((timestamp, accs_to_send))
                        while len(self.recent_picks) > RECENT_PICKS_COUNT:
                            self.recent_picks = self.recent_picks[1:]
                        
                    elif ksigma > KSIGMA_THRESHOLD:
                        in_pick[i] = True
                        last_pick_timestamp[i] = timestamp
                        max_acceleration[i] = abs(accs[i]-lta_mean)
                        ksigma_at_pick[i] = ksigma
                        timestamp_string = "%20.5f" % timestamp
                        logging.info("Pick on axis %s at time %s, kSigma %s", i, timestamp_string, ksigma)

                        
                elif in_pick[i]:
                    if ksigma < ksigma_at_pick[i]:
                        accs_to_send = [0., 0., 0.]
                        accs_to_send[i] = max_acceleration[i]
                        self.send_event(datetime.datetime.fromtimestamp(last_pick_timestamp[i]), accs_to_send)
                        self.recent_picks.append((last_pick_timestamp[i], accs_to_send))
                        while len(self.recent_picks) > RECENT_PICKS_COUNT:
                            self.recent_picks = self.recent_picks[1:]

                        in_pick[i] = False
                    else:
                        if abs(accs[i]-lta_mean) > max_acceleration[i]:
                            max_acceleration[i] = abs(accs[i]-lta_mean)
                
                del accelerations[i][0]    


    def DisplayDeviceInfo(self):
            print("|------------|----------------------------------|--------------|------------|")
            print("|- Attached -|-              Type              -|- Serial No. -|-  Version -|")
            print("|------------|----------------------------------|--------------|------------|")
            print("|- %8s -|- %30s -|- %10d -|- %8d -|" % (self.accelerometer.isAttached(), self.accelerometer.getDeviceName(), self.accelerometer.getSerialNum(), self.accelerometer.getDeviceVersion()))
            print("|------------|----------------------------------|--------------|------------|")
            print("Number of Axes: %i" % (self.accelerometer.getAccelerationAxisCount()))
            print('Max Acceleration Axis 0: {} Min Acceleration Axis 0: {}'.format(self.accelerometer.getAccelerationMax(0), self.accelerometer.getAccelerationMin(0)))
            print('Max Acceleration Axis 1: {} Min Acceleration Axis 1: {}'.format(self.accelerometer.getAccelerationMax(1), self.accelerometer.getAccelerationMin(1)))
            print('Max Acceleration Axis 2: {} Min Acceleration Axis 2: {}'.format(self.accelerometer.getAccelerationMax(2), self.accelerometer.getAccelerationMin(2)))
    

    def setFileStoreInterval(self, file_store_interval):
        # sets the interval for writing the data to a new file
        self.file_store_interval = file_store_interval
    
    #Event Handler Callback Functions
    def AccelerometerAttached(self, e):
        attached = e.device
        self.phidget_attached = True
        logging.info("Accelerometer %s Attached!", attached.getSerialNum())
        # set data rate in milliseconds (we will decimate from 4ms to 20ms later)
        self.accelerometer.setDataRate(PHIDGETS_NOMINAL_DATA_INTERVAL_MS)
        logging.info("Phidget data rate interval set to %s milliseconds", PHIDGETS_NOMINAL_DATA_INTERVAL_MS)

    
    def AccelerometerDetached(self, e):
        detached = e.device
        self.phidget_attached = False
        logging.error('Accelerometer %s Detached!',(detached.getSerialNum()))
            
    def AccelerometerError(self, e):
        try:
            source = e.device
            logging.error("Accelerometer %s: Phidget Error %s: %s", source.getSerialNum(), e.eCode, e.description)
        except PhidgetException as e:
            logging.error("Phidget Exception %s: %s", e.code, e.details)  
            
    def AccelerometerAccelerationChanged(self, e):
        source = e.device
        logging.error("Accelerometer %s: Axis %s: %s", source.getSerialNum(), e.index, e.acceleration)

    def MakeFilename(self, timestamp):
        timestamp_datetime = datetime.datetime.fromtimestamp(timestamp).strftime(FILESTORE_NAMING)
        sps = int (1000 / (PHIDGETS_NOMINAL_DATA_INTERVAL_MS * self.decimation))
        return self.datastore + '/' + str(sps) + '_' + timestamp_datetime + '.dat'

    def setTimingFitVariables(self, base_time, gradient, intercept):
        # this is called by the main client which is monitoring the system clock compared with NTP
        # we acquire a lock on the timing variables to update them
        with self.timing_lock:
            self.timing_base_time = base_time
            self.timing_gradient = gradient
            self.timing_intercept = intercept


    def GetNtpCorrectedTimestamp(self):
        # from the current system time we use the NTP thread's line fit to estimate the true time
        time_now = time.time()
        # we ensure that the timing variables are not being updated concurrently by acquiring a lock on them
        with self.timing_lock:
            offset_estimate = (time_now-self.timing_base_time) * self.timing_gradient + self.timing_intercept       
        return time_now + offset_estimate

    def StopSampleCollection(self):
        # This stops more samples being collected, and closes the current samples file
        self.collect_samples = False
        self.datastore_file.close()

    def StartSampleCollection(self):
        # This restarts sample collection (into files)
        self.collect_samples = True
        
    def SpatialData(self, e):
               
        if not self.phidget_attached:
            return
        
        sample_timestamp = self.GetNtpCorrectedTimestamp()
        
        self.sensor_raw_data_queue.put((e, sample_timestamp))

    def to_dict(self):
        # No need to persist anything that doesn't change.
        details = {}
        details['module'] = 'PhidgetsSensor'
        details['class'] = 'PhidgetsSensor'
        with self.sensor_data_lock:
            details['sensor_id'] = self.sensor_data
            details['serial'] = self.sensor_data['Serial']
        details['datastore'] = self.datastore
        details['decimation'] = self.decimation
        details['picker'] = self.picker
        details['pick_threshold'] = self.pick_threshold
        if self.last_datastore_filename_uploaded:
            details['last_upload'] = self.last_datastore_filename_uploaded
        else:
            details['last_upload'] = ''

        return details

    def get_metadata(self):
        logging.error('PhidgetSensor get_metadata Not implemented')

        
    def datafiles_not_yet_uploaded(self):
        # Returns a list of files that are older than last_datastore_filename (or current time) in the
        # data file directory that have not yet been uploaded
        # we subtract 10 minutes off the time to avoid finding the currently open file (although we could in 
        # principle trap that)
        
        file_list = []
        last_file_date = self.GetNtpCorrectedTimestamp() - 600.0
            
        logging.info('Searching for files older than %s',last_file_date)
        for f in os.listdir(self.datastore):
            filename = self.datastore + '/' + f
            if filename == self.datastore_filename:
                logging.info('Will not add currently opened file %s', filename)
                continue
            if os.path.isfile(filename):
                    try:
                        #t = os.path.getctime(filename)
                        file_date = os.path.getctime(filename)
                        if file_date < last_file_date:
                            if len(file_list) < 20:
                                logging.info('Not yet uploaded: %s',filename)
                            elif len(file_list) == 20:
                                logging.info('Not yet uploaded: %s (will not show more)', filename)
                            file_list.append(filename)
                    except:
                        logging.error('Error getting file time for %s', filename)
                
        return file_list
 
    def mark_file_uploaded(self, filename):
        # when a sensor data file has been successfully uploaded to the server we move it to the uploaded directory
        try:            
            shutil.copy2(filename, self.datastore_uploaded)
            self.last_datastore_filename_uploaded = filename
            os.remove(filename)
        except:
            logging.warning('Failed to move %s to %s', filename, self.datastore_uploaded)
        
    def mark_file_corrupted(self, filename):
        # when a sensor data file fails to convert to stream we move it to the corrupt directory
        try:            
            shutil.copy2(filename, self.datastore_corrupted)
        except:
            logging.warning('Failed to move %s to %s', filename, self.datastore_corrupted)
        # always remove the corrupt file from the main directory  
        try:     
            os.remove(filename)
        except:
            logging.error('Failed to delete %s', filename)
            
        

    def set_sensor_id(self, sensor_id):
        with self.sensor_data_lock:
            self.sensor_data['sensor_id'] = sensor_id


    def send_event(self, event_time, values):
        
        """
        pick_message = {'timestamp': event_time,
                        'station': self.client_id,
                        'latitude': self.latitude,
                        'longitude': self.longitude,
                        'demeaned_accelerations': values,
                        'floor': self.floor,
                        }

        try:
        
            message_body = json.dumps(pick_message)
            
            m = Message()
            m.set_body(message_body)
            self.pick_queue.write(m)
            
            #logging.info('Sent pick to Amazon SQS: %s', pick_message)
            
        except Exception, e:
            logging.error('Failed to send pick message: %s', e)
        """    
        # Send pick to FinDer
        station_name = self.client_id
        station_name = station_name[0:1] + station_name[3:]
        finder_accs = [acc*G_TO_CMS2 for acc in values]
        channel = 'HNN'
        if finder_accs[1] > 0.0: channel = 'HNE'
        if finder_accs[2] > 0.0: channel = 'HNZ'
        pick_datetime = datetime.datetime.utcfromtimestamp(float(event_time))
        pick_time = pick_datetime.strftime(TIMESTAMP_NAMING)
        finder_location = str(self.latitude) + ' ' + str(self.longitude)
        timenow_timestamp = self.GetNtpCorrectedTimestamp()
        timenow = datetime.datetime.utcfromtimestamp(timenow_timestamp)
        activemq_message = '1 ' + timenow.strftime(TIMESTAMP_NAMING)[:-3] + ' '+self.software_version+'\n'
        line = '%s CSN.%s.%s.-- %s %s %s %s\n' % \
                (finder_location, station_name, channel, pick_time, abs(finder_accs[0]), abs(finder_accs[1]), abs(finder_accs[2]))
        activemq_message += line
        activemq_message += 'ENDOFDATA\n'
        logging.info("ActiveMQ message to send:\n%s", activemq_message[:-1])
        try:        
            self.stomp.put(activemq_message, destination=self.stomp_topic)
        except Exception, err:
            logging.error('Error sending pick to ActiveMQ %s', err)
            logging.info('Trying to reconnect to ActiveMQ broker')
            self.stomp = self.connect_stomp()



