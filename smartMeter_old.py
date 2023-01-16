#!/usr/bin/python3
import minimalmodbus
import serial
import time
from paho.mqtt import client as mqtt_client
import random

broker = 'mother'
port = 1886
topic = "SmartMeter/ORNO"
client_id = f'SmartMeter-{random.randint(0, 1000)}'
username = 'Broki'
password = '!2022!Broki!'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

#Adapter fuer Mobus Anlegen 
smartmeter = minimalmodbus.Instrument('/dev/ttyAMA0', 1,) # port name, slave address (in decimal)
smartmeter.serial.baudrate = 9600 # Baud
smartmeter.serial.bytesize = 8
smartmeter.serial.parity   = serial.PARITY_EVEN # vendor default is EVEN
smartmeter.serial.stopbits = 1
smartmeter.serial.timeout  = 0.6  # seconds
smartmeter.mode = minimalmodbus.MODE_RTU   # rtu or ascii mode
smartmeter.clear_buffers_before_each_transaction = False
smartmeter.debug = False 

status = True

client = connect_mqtt()

L1U  = f"{topic}/L1_Spannung"
L1F  = f"{topic}/L1_Frequenz"
L1I  = f"{topic}/L1_Strom"
L1P  = f"{topic}/L1_Power"
L1PF = f"{topic}/L1_PF"

while status: 
	L1Frequenz    = smartmeter.read_register(304,2,3)
	L1Spannung    = smartmeter.read_register(305,2,3)
	L1Strom       = smartmeter.read_register(314,3,3)
	#L1Power       = smartmeter.read_register(321,3,3)
	L1Power       = L1Spannung * L1Strom
	L1PowerFaktor = smartmeter.read_register(344,3,3)
	txt = "Spannung       {U:.2f} V"
	print(txt.format(U=L1Spannung))
	txt = "Frequenz       {F:.2f} Hz"
	print(txt.format(F=L1Frequenz))
	txt = "Strom          {I:.3f} A"
	print(txt.format(I=L1Strom))
	txt = "Leistung       {P:.3f} W"
	print(txt.format(P=L1Power))
	txt = "Power Faktor   {PF:.3f}"
	print(txt.format(PF=L1PowerFaktor))
	print("---------------------------------")
	msg = f"{L1Spannung}"
	client.publish(L1U, msg)
	msg = f"{L1Frequenz}"
	client.publish(L1F, msg)
	msg = f"{L1Strom}"
	client.publish(L1I, msg)
	msg = f"{L1Power}"
	client.publish(L1P, msg)
	msg = f"{L1PowerFaktor}"
	client.publish(L1PF, msg)
	time.sleep(5)

# MQTT
#client = connect_mqtt()
#L1Spannung = 220.22
#msg = f"{L1Spannung}"
#L1U = f"{topic}/L1_Spannung"
#result = client.publish(L1U, msg)
#status = result[0]
#if status == 0:
#	print(f"Send `{msg}` to topic `{topic}`")
#else:
#	print(f"Failed to send message to topic {topic}")
