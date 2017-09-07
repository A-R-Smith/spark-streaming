# IoT Spark-Streaming Project
The project contains modules for the Spark Streaming analytic pipeline for the IoT Smart Home/Venue pilot. They are meant to run on a spark cluster.
They are meant to consume data feeds from IoT sensors. They consume these streaming data feeds from a Kafka broker and in some cases write streaming data feeds back to a Kafka broker.

##Modules

### GeoEnricher
   This module enriches the IoT data with geo-spacial information of the sensors
   
### EventGenerator
 This module detects anomalies in the sensor data. Right now, its done with simple thresholding. This generates events for temperature, humidity and gas sensors.
                                                                                                                                                                
### WaterEventGenerator
This specialized module detects anomalies in the water sensor data.                                                                                                                                                      