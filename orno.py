#
# ORNO SmartMeter Class 
#
# This class helps to communicate with an ORNO smart meter via RS-485 ModBus protocol. 
# It is possible to send results to MQTT topic.
#
# Current support:
#  Single Phase Meter OR-WE-514
#
# Author: Marc@l3c.de
#
# License: MIT
#
import minimalmodbus
import serial
import time
import random
from paho.mqtt import client as mqtt_client

class orno:
  def __init__(self, port, slave_id=1, useMQTT=False):
    self.debug = False
    self.port = port
    self.slave_id = slave_id
    self.polling_interval = 5
    self.smartmeter = minimalmodbus.Instrument(self.port, self.slave_id)
    self.smartmeter.serial.baudrate = 9600
    self.smartmeter.serial.bytesize = 8
    self.smartmeter.serial.parity = serial.PARITY_EVEN
    self.smartmeter.serial.stopbits = 1
    self.smartmeter.serial.timeout = 0.6
    self.smartmeter.mode = minimalmodbus.MODE_RTU
    self.smartmeter.clear_buffers_before_each_transaction = False
    self.smartmeter.debug = False
    self.useMQTT = useMQTT
    self.isMQTT_connected = False
    if self.useMQTT:
      self.mqtt_broker='localhost'
      self.mqtt_port=1886
      self.mqtt_topic="SmartMeter/ORNO"
      self.mqtt_username='USERNAME'
      self.mqtt_password='PASSWORD'
      self.mqtt_connect_retry_count = 30

  def query(self):
    self.L1_frequency = self.smartmeter.read_register(304,2,3)
    self.L1_voltage   = self.smartmeter.read_register(305,2,3)
    self.L1_current   = self.smartmeter.read_register(314,3,3)
    #self.L1_power     = self.smartmeter.read_register(321,3,3)
    self.L1_power     = self.L1_voltage * self.L1_current
    self.L1_PF        = self.smartmeter.read_register(344,3,3)

  def print(self):
    self.txt = "L1 Voltage       {U:.2f} V"
    print(self.txt.format(U=self.L1_voltage))
    self.txt = "L1 Frequency     {F:.2f} Hz"
    print(self.txt.format(F=self.L1_frequency))
    self.txt = "L1 Current       {I:.3f} A"
    print(self.txt.format(I=self.L1_current))
    self.txt = "L1 Power         {P:.3f} W"
    print(self.txt.format(P=self.L1_power))
    self.txt = "L1 Power Factor  {PF:.3f}"
    print(self.txt.format(PF=self.L1_PF))

  def doLoop(self, count=0, infinite=True):
    if self.useMQTT and not self.isMQTT_connected:
      self.mqtt_enable()
      if self.debug:
        print(f"Enabling MQTT connection: {self.isMQTT_connected}")
    if self.debug:
      print(f"Start polling every {self.polling_interval} seconds slave id '{self.slave_id}'")
    if infinite and count < 1:
      while True:
        self.query()
        if self.useMQTT:
          self.mqtt_publish()
        time.sleep(self.polling_interval)
    else:
       for i in range(0,count):
         self.query()
         if self.useMQTT:
           self.mqtt_publish()
         time.sleep(self.polling_interval)

  def mqtt_enable(self):
    self.mqtt_client_id=f'ORNO-{random.randint(1000, 8000)}'
    self.L1U  = f"{self.mqtt_topic}/L1_Voltage"
    self.L1F  = f"{self.mqtt_topic}/L1_Frequency"
    self.L1I  = f"{self.mqtt_topic}/L1_Current"
    self.L1P  = f"{self.mqtt_topic}/L1_Power"
    self.L1PF = f"{self.mqtt_topic}/L1_PF"
    if self.debug:
      print(f"Using MQTT Broker: '{self.mqtt_broker}:{self.mqtt_port}' with user '{self.mqtt_username}' and secret '{self.mqtt_password}'")
      print(f"Using MQTT Topic : '{self.mqtt_topic}'")
    try:
      self.mqtt=self.mqtt_connect()
      self.isMQTT_connected = True
    except Exception as err:
      print(f"Unexpected {err=}, {type(err)=}")
      print(f"Current Retry Count is {self.mqtt_connect_retry_count}")

  def mqtt_publish(self):
    try:
      self.client.publish(self.L1U, f"{self.L1_voltage}")
      self.client.publish(self.L1F, f"{self.L1_frequency}")
      self.client.publish(self.L1I, f"{self.L1_current}")
      self.client.publish(self.L1P, f"{self.L1_power}")
      self.client.publish(self.L1PF, f"{self.L1_PF}")
    except Exception as err:
      print(f"Unexpected {err=}, {type(err)=}")
      print(f"Current Retry Count is {self.mqtt_connect_retry_count}")

  def mqtt_on_connect(self, client, userdata, flags, rc):
    if rc == 0:
      print("ORNO/MQTT: Connected to MQTT Broker!")
      client.connected_flag=True
    else:
      print("ORNO/MQTT: Failed to connect, return code %d\n", rc)
      client.bad_connection_flag=True

  def mqtt_connect(self):
    self.client = mqtt_client.Client(self.mqtt_client_id)
    self.client.bad_connection_flag=False
    self.client.username_pw_set(self.mqtt_username, self.mqtt_password)
    self.client.on_connect = self.mqtt_on_connect
    try:
      self.client.connect(self.mqtt_broker, self.mqtt_port)
      #print(f"client_bad_connection_flag: {self.client.bad_connection_flag}")
    except Exception as err:
      print(f"Unexpected {err=}, {type(err)=}")
      print(f"Current Retry Count is {self.mqtt_connect_retry_count}")
    return self.client
