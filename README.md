# PyCSN
Client code for the Caltech Community Seismic Network

Prerequisites

0) Python 2.7
1) Phidgets client library
2) Python Phidgets library
3) Python Boto (Amazon) library
4) Python Stomp client library (stompy)
5) Python Poster library (poster)
6) Python YAML library (pyyaml)
7) Python NTP library (ntplib)
8) Phidgets Spatial 3/3/3 MEMS accelerometer

Running the Client

The CSN_client.yaml file contains the configuration of the CSN client, including the Lat/Lon, Building, Floor, and client name. This should be edited to contain the required detaails.

The main program is in "PyCSNFinDer.py". This contains the credentials for accessing both the Amazon S3 service that will store the sensor data, and the location and credentials for the ActiveMQ broker which will receive the Picks from the client.
