#!/usr/bin/python
#
# ORNO SmartMeter Sample 
#
# This class helps to communicate with an ORNO smart meter via RS-485 ModBus protocol. 
# It is possible to send results to MQTT topic.
#
# See REAME.MD for more information and hardware setup.
#
# Current support:
#  Single Phase Meter OR-WE-514
#
# Author: Marc-Oliver Blumenauer 
#         marc@l3c.de
#
# License: MIT
#
import orno

instrument=orno.orno('/dev/ttyAMA0',useMQTT=True)

instrument.mqtt_broker       = 'mother'
instrument.mqtt_port         = 1886
instrument.mqtt_username     = 'Broki'
instrument.mqtt_password     = '!2022!Broki!'
instrument.mqtt_topic        = 'SmartMeter/ORNO/WE-514'
instrument.debug             = False
instrument.polling_interval  = 10

instrument.doLoop()
