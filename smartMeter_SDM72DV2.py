#!/usr/bin/python3 -u 

import orno
import time

instrument=orno.orno('/dev/ttyAMA0',useMQTT=True, logFile="smartMeter_sdm72dv2.log", type=orno.SDM72DV2)

instrument.mqtt_broker       = 'HOSTNAME'
instrument.mqtt_port         = 1886
instrument.mqtt_username     = 'MQTT_USERNAME'
instrument.mqtt_password     = 'MQTT_PASSWORD'
instrument.mqtt_topic        = 'SmartMeter/EASTRON/SDM72DV2'
instrument.debug             = False
instrument.polling_interval  = 10

time.sleep(60)

instrument.mqtt_enable()

while True:
    instrument.query()
    instrument.mqtt_publish()
    time.sleep(instrument.polling_interval)