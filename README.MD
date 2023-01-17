
#### ORNO Class 

> Installationschritte (Abhängigkeiten)
```
pip3 install minimalmodbus
pip3 install pyserial
pip3 install random
pip3 install paho-mqtt
```

###### Beschreibung der Methoden (Parameter)
> class orno(port, slave_id, useMQTT)
>
```
> port\t\tSerial Device like COM4 or /dev/ttyUSB0
>
> slave_id\tThe ID of the Modbus Device to communicate to
>
> useMQTT\tTrue or False, if True it is necessary to set MQTT communication parameters
> \t\t\tmqtt_broker\tIP address or hostname of MQTT server
> \t\t\tmqtt_port\tthe port of the MQTT broker
> \t\t\tmqtt_topic\tthe main topic where the ORNO objects L1_volage etc being published
> \t\t\tmqtt_username\tthe username for MQTT broker authentication
> \t\t\tmqtt_password\tthe password for MQTT broker authentication
```

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