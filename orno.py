#
# ORNO SmartMeter Class 
#
# This class helps to communicate with an ORNO smart meter via RS-485 ModBus protocol. 
# It is possible to send results to MQTT topic.
#
# Current support:
#  Single Phase Meter OR-WE-514
#
# Author: Marc-Oliver Blumenauer 
#         marc@l3c.de
#
# License: MIT
#
import minimalmodbus
import serial
import time
import random
import os
from paho.mqtt import client as mqtt_client
from datetime import datetime, date, time, timezone

L1_Frequency     = 304
L1_Voltage       = 305
L1_Current       = 314
L1_Power         = -1
L1_PF            = 344
L1_ActivePower   = 321  # 3 decimals kvar
L1_ReactivePower = 329  # 3 decimals kvar
L1_ApparentPower = 337  # 3 decimals kva
TotalPower       = 40961 # 2 decimals kWh

class orno:
  def __init__(self, port, slave_id=1, useMQTT=False, log=True):
    self.debug = False
    self.log = log
    self.logFile = datetime.now().strftime(f"{os.path.basename(__file__)}-%Y%m%d%H%M%S.log")
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
    if self.log:
      try:
        self.logFH = open(self.logFile,"w")
        self.logMessage(f"ORNO Init - Startup.")
        self.logMessage(f"{self.smartmeter}")
      except IOError as ioError:
        print(f"ORNO Error: Cannot create logfile: {ioError}")

  def logMessage(self, message):
    if self.log:
      try:
        self.txt = datetime.now().strftime("%Y%m%d %H:%M:%S>> ")+ f"{message}\n"
        self.logFH.write(self.txt)
      except IOError as ioError:
        print(f"ORNO Error: Cannot log message '{message}:\n{ioError}")

  def query(self, register=0, decimals=2):
    if register == 0:
      self.L1_frequency = self.smartmeter.read_register(L1_Frequency,2,3)
      self.L1_voltage   = self.smartmeter.read_register(L1_Voltage,2,3)
      self.L1_current   = self.smartmeter.read_register(L1_Current,3,3)
      #self.L1_power     = self.smartmeter.read_register(L1_Power,3,3)
      self.L1_power     = self.L1_voltage * self.L1_current
      self.L1_PF        = self.smartmeter.read_register(L1_PF,3,3)
      self.TotalPower   = self.smartmeter.read_register(TotalPower,2,3)
      self.L1_APower    = self.smartmeter.read_register(L1_ActivePower,3,3)
      self.L1_RPower    = self.smartmeter.read_register(L1_ReactivePower,3,3)
      self.L1_ApPower   = self.smartmeter.read_register(L1_ApparentPower,3,3)
    elif register == -1:
      self.L1_voltage   = self.smartmeter.read_register(L1_Voltage,2,3)
      self.L1_current   = self.smartmeter.read_register(L1_Current,3,3)
      self.L1_power     = self.L1_voltage * self.L1_current
      return self.L1_power
    else:
      return self.smartmeter.read_register(register,decimals,3)


  def print(self):
    self.txt = "L1 Voltage        {U:.2f} V"
    print(self.txt.format(U=self.L1_voltage))
    self.txt = "L1 Frequency      {F:.2f} Hz"
    print(self.txt.format(F=self.L1_frequency))
    self.txt = "L1 Current        {I:.3f} A"
    print(self.txt.format(I=self.L1_current))
    self.txt = "L1 Power          {P:.3f} W"
    print(self.txt.format(P=self.L1_power))
    self.txt = "L1 Active Power   {AP:.3f} kW"
    print(self.txt.format(AP=self.L1_APower))
    self.txt = "L1 Reactive Power {RP:.3f} kvar"
    print(self.txt.format(RP=self.L1_RPower))
    self.txt = "L1 Apparent Power {ApP:.3f} kva"
    print(self.txt.format(ApP=self.L1_ApPower))
    self.txt = "L1 Power Factor   {PF:.3f}"
    print(self.txt.format(PF=self.L1_PF))
    self.txt = "Total Power       {TP:.2f} kWh"
    print(self.txt.format(TP=self.TotalPower))

  def doLoop(self, count=0, infinite=True):
    if self.useMQTT and not self.isMQTT_connected:
      self.mqtt_enable()
      self.logMessage(f"Enabling MQTT connection: {self.isMQTT_connected}")
    self.logMessage(f"Start polling every {self.polling_interval} seconds slave id '{self.slave_id}'")
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
    self.L1AP = f"{self.mqtt_topic}/L1_ActivePower"
    self.L1RP = f"{self.mqtt_topic}/L1_ReactivePower"
    self.L1ApP= f"{self.mqtt_topic}/L1_ApparentPower"
    self.L1PF = f"{self.mqtt_topic}/L1_PF"
    self.TP   = f"{self.mqtt_topic}/TotalPower"
    self.logMessage(f"Using MQTT Broker: '{self.mqtt_broker}:{self.mqtt_port}' with user '{self.mqtt_username}' and secret '{self.mqtt_password}'")
    self.logMessage(f"Using MQTT Topic : '{self.mqtt_topic}'")
    try:
      self.mqtt=self.mqtt_connect()
      self.isMQTT_connected = True
    except Exception as err:
      print(f"Unexpected {err=}, {type(err)=}")
      print(f"Current Retry Count is {self.mqtt_connect_retry_count}")
      self.logMessage(f"Unexpected {err=}, {type(err)=}")

  def mqtt_publish(self):
    try:
      self.client.publish(self.L1U, f"{self.L1_voltage}")
      self.client.publish(self.L1F, f"{self.L1_frequency}")
      self.client.publish(self.L1I, f"{self.L1_current}")
      self.client.publish(self.L1P, f"{self.L1_power}")
      self.client.publish(self.L1AP, f"{self.L1_APower}")
      self.client.publish(self.L1RP, f"{self.L1_RPower}")
      self.client.publish(self.L1ApP, f"{self.L1_ApPower}")
      self.client.publish(self.L1PF, f"{self.L1_PF}")
      self.client.publish(self.TP, f"{self.TotalPower}")
    except Exception as err:
      print(f"Unexpected {err=}, {type(err)=}")
      print(f"Current Retry Count is {self.mqtt_connect_retry_count}")
      self.logMessage(f"Unexpected {err=}, {type(err)=}")

  def mqtt_on_connect(self, client, userdata, flags, rc):
    if rc == 0:
      self.logMessage("ORNO/MQTT: Connected to MQTT Broker!")
      client.connected_flag=True
    else:
      self.logMessage("ORNO/MQTT: Failed to connect, return code %d\n", rc)
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
      self.logMessage(f"Unexpected {err=}, {type(err)=}")
    return self.client
