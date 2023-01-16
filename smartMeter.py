#!/usr/bin/python

import orno

instrument=orno.orno('/dev/ttyAMA0',useMQTT=True)

instrument.mqtt_broker       = 'mother'
instrument.mqtt_port         = 1886
instrument.mqtt_username     = 'Broki'
instrument.mqtt_password     = '!2022!Broki!'
instrument.mqtt_topic        = 'SmartMeter/TEST'
instrument.debug             = False
instrument.polling_interval  = 10

instrument.doLoop()
