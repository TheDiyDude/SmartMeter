
#### ORNO Class 

> Installationschritte (Abhängigkeiten)
```
pip3 install minimalmodbus
pip3 install pyserial
pip3 install random
pip3 install paho-mqtt
```

> Beschreibung der Methoden (Parameter)

> Aufbau Hardware (Raspberry Pi, RS-485 Modul, Kabel, Anschluss)

> Beispiele (siehe unten)

##### Sample infinite loop and sending to MQTT Broker

```
import orno

instrument=orno.orno('/dev/ttyAMA0',useMQTT=True)

instrument.mqtt_broker       = 'broker-hostname'
instrument.mqtt_port         = 1886
instrument.mqtt_username     = 'user'
instrument.mqtt_password     = 'password'
instrument.mqtt_topic        = 'SmartMeter/ORNO/WE-514'
instrument.debug             = False
instrument.polling_interval  = 10

instrument.doLoop()
```
![Result](iobroker_mqtt_results.png)

##### Sample query with count (100 times every 10 seconds) and sending to MQTT Broker

```
import orno

instrument=orno.orno('/dev/ttyAMA0',useMQTT=True)

instrument.mqtt_broker       = 'broker-hostname'
instrument.mqtt_port         = 1886
instrument.mqtt_username     = 'user'
instrument.mqtt_password     = 'password'
instrument.mqtt_topic        = 'SmartMeter/ORNO/WE-514'
instrument.debug             = False
instrument.polling_interval  = 10

instrument.doLoop(count=100)
```

![Result](Bildschirmfoto%202023-01-15%20um%2022.01.34.png)

##### Sample to check connection and print results

```
import orno

instrument=orno.orno('/dev/ttyUSB0', slave_id=10, useMQTT=False)
instrument.query()
instrument.print()
```
###### Result
```
L1 Voltage       229.37 V
L1 Frequency     50.03 Hz
L1 Current       0.030 A
L1 Power         6.881 W
L1 Power Factor  0.125
>>>
``` 