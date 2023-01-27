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
import time as t
import random
import os
from paho.mqtt import client as mqtt_client
from datetime import datetime

WE520            = 0
WE517            = 1

#L1_Frequency     = 304
#L1_Voltage       = 305
#L1_Current       = 314
#L1_Power         = -1
#L1_PF            = 344
#L1_ActivePower   = 321  # 3 decimals kvar
#L1_ReactivePower = 329  # 3 decimals kvar
#L1_ApparentPower = 337  # 3 decimals kva
#TotalPower       = 40961 # 2 decimals kWh

TotalPower                    = 0xA001,-1
L1_Voltage                    = 0x131,0x0E
L2_Voltage                    = 0x131,0x10
L3_Voltage                    = 0x131,0x12
L1_Frequency                  = 0x130,0x14
L2_Frequency                  = 0x130,0x14
L3_Frequency                  = 0x130,0x14
L1_Current                    = 0x13A,0x16
L2_Current                    = 0x13A,0x18
L3_Current                    = 0x13A,0x1A
Total_ActivePower             = -1,0x1C
L1_ActivePower                = 0x141,0x1E
L2_ActivePower                = 0x141,0x20
L3_ActivePower                = 0x141,0x22
Total_ReactivePower           = -1,0x24
L1_ReactivePower              = 0x149,0x26
L2_ReactivePower              = 0x149,0x28
L3_ReactivePower              = 0x149,0x2A
Total_ApparentPower           = -1,0x2C
L1_ApparentPower              = 0x151,0x2E
L2_ApparentPower              = 0x151,0x30
L3_ApparentPower              = 0x151,0x32
Total_PF                      = -1,0x34
L1_PF                         = 0x158,0x36
L2_PF                         = 0x158,0x38
L3_PF                         = 0x158,0x3A
Total_ActiveEnergy            = -1,0x0100
L1_ActiveEnergy               = -1,0x0102
L2_ActiveEnergy               = -1,0x0104
L3_ActiveEnergy               = -1,0x0106
Total_ForwardActiveEnergy     = -1,0x0108
L1_ForwardActiveEnergy        = -1,0x010A
L2_ForwardActiveEnergy        = -1,0x010C
L3_ForwardActiveEnergy        = -1,0x010E
Total_ReverseActiveEnergy     = -1,0x0110
L1_ReverseActiveEnergy        = -1,0x0112
L2_ReverseActiveEnergy        = -1,0x0114
L3_ReverseActiveEnergy        = -1,0x0116
T1_TotalActiveEnergy          = -1,0x0130
T1_ForwardActiveEnergy        = -1,0x0132
T1_ReverseActiveEnergy        = -1,0x0134
T2_TotalActiveEnergy          = -1,0x013C
T2_ForwardActiveEnergy        = -1,0x013E
T2_ReverseActiveEnergy        = -1,0x0140
T3_TotalActiveEnergy          = -1,0x0148
T3_ForwardActiveEnergy        = -1,0x014A
T3_ReverseActiveEnergy        = -1,0x014C
T4_TotalActiveEnergy          = -1,0x0154
T4_ForwardActiveEnergy        = -1,0x0156
T4_ReverseActiveEnergy        = -1,0x0158
GridFrequency                 = 0x130,0x14
class orno:
  def __init__(self, port, slave_id=1, useMQTT=False, debug=False, log=True, logFile="", type=0):
    self.debug = debug
    self.log = log
    self.logFile = logFile
    self.type = type
    self.defaultlogFile = datetime.now().strftime(f"{os.path.basename(__file__)}-%Y%m%d%H%M%S.log")
    self.port = port
    self.slave_id = slave_id
    self.polling_interval = 5
    self.mqtt_actual_connection_try = 0
    self.mqtt_connect_retry_count = 60
    self.mqtt_connect_sleep_time  = 320
    self.smartmeter = minimalmodbus.Instrument(self.port, self.slave_id)
    self.smartmeter.serial.baudrate = 9600
    self.smartmeter.serial.bytesize = 8
    self.smartmeter.serial.parity = serial.PARITY_EVEN
    self.smartmeter.serial.stopbits = 1
    self.smartmeter.serial.timeout = 0.6
    self.smartmeter.mode = minimalmodbus.MODE_RTU
    self.smartmeter.clear_buffers_before_each_transaction = False
    self.smartmeter.debug = debug
    self.useMQTT = useMQTT
    self.isMQTT_connected = False
    if self.useMQTT:
      self.mqtt_broker='localhost'
      self.mqtt_port=1886
      self.mqtt_topic="SmartMeter/ORNO"
      self.mqtt_username='USERNAME'
      self.mqtt_password='PASSWORD'     
    if self.log:
      try:
        if self.logFile == "":
          self.logFile = self.defaultlogFile
        else:
          self.logFile = datetime.now().strftime(f"{os.path.splitext(self.logFile)[0]}-%Y%m%d%H%M%S{os.path.splitext(self.logFile)[1]}")
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
        self.logFH.flush()
      except IOError as ioError:
        print(f"ORNO Error: Cannot log message '{message}:\n{ioError}")

  def query(self, register=0, decimals=2):
    if register == 0 and self.type == 0:
      self.L1_frequency = self.smartmeter.read_register(L1_Frequency[self.type],2,3)
      self.L1_voltage   = self.smartmeter.read_register(L1_Voltage[self.type],2,3)
      self.L1_current   = self.smartmeter.read_register(L1_Current[self.type],3,3)
      self.L1_power     = self.L1_voltage * self.L1_current
      self.L1_PF        = self.smartmeter.read_register(L1_PF[self.type],3,3)
      self.TotalPower   = self.smartmeter.read_register(TotalPower[self.type],2,3)
      self.L1_APower    = self.smartmeter.read_register(L1_ActivePower[self.type],3,3)
      self.L1_RPower    = self.smartmeter.read_register(L1_ReactivePower[self.type],3,3)
      self.L1_ApPower   = self.smartmeter.read_register(L1_ApparentPower[self.type],3,3)
    elif register == 0 and self.type == 1:
      self.L1_frequency = self.read_float(L1_Frequency[self.type],2)
      self.L1_voltage   = self.read_float(L1_Voltage[self.type],2)
      self.L1_current   = self.read_float(L1_Current[self.type],2)
      self.L1_power     = self.L1_voltage * self.L1_current
      self.L1_PF        = self.read_float(L1_PF[self.type],2)
      self.L1_APower    = self.read_float(L1_ActivePower[self.type],2)
      self.L1_RPower    = self.read_float(L1_ReactivePower[self.type],2)
      self.L1_ApPower   = self.read_float(L1_ApparentPower[self.type],2)
      self.L1_AEnergy   = self.read_float(L1_ActiveEnergy[self.type],2)
      self.L1_FAEnergy  = self.read_float(L1_ForwardActiveEnergy[self.type],2)
      self.L1_RAEnergy  = self.read_float(L1_ReverseActiveEnergy[self.type],2)
      self.TotalPower   = self.read_float(L1_ActiveEnergy[self.type],2)
    elif register == -1 and self.type == 0:
      self.L1_voltage   = self.smartmeter.read_register(L1_Voltage[self.type],2,3)
      self.L1_current   = self.smartmeter.read_register(L1_Current[self.type],3,3)
      self.L1_power     = self.L1_voltage * self.L1_current
      return self.L1_power
    else:
      if self.type == 0:
        return self.smartmeter.read_register(register,decimals,3)
      elif self.type == 1:
        return self.read_float(register,decimals)

  def read_float(self, register=0, num=2, code=3, order=0):
      return self.smartmeter.read_float(register,code,num,order)
  
  def print(self):
    if self.type == 0:
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
    elif self.type == 1:
      print(f"L1 Voltage                 {self.L1_voltage:.0f} V")
      print(f"L1 Frequency               {self.L1_frequency:.2f} Hz")
      print(f"L1 Current                 {self.L1_current:.3f} A")
      print(f"L1 Power                   {self.L1_power:.3f} W")
      print(f"L1 Active Power            {self.L1_APower:.3f} kW")
      print(f"L1 Reactive Power          {self.L1_RPower:.3f} kvar")
      print(f"L1 Apparent Power          {self.L1_ApPower:.3f} kva")
      print(f"L1 Power Factor            {self.L1_PF:.2f}")
      print(f"L1 Active Energy           {self.L1_AEnergy:.3f}")
      print(f"L1 Forward Active Energy   {self.L1_FAEnergy:.3f}")
      print(f"L1 Reverse Active Energy   {self.L1_RAEnergy:.3f}")
      print(f"Total Power                {self.TotalPower:.2f} kWh")
      
  def doLoop(self, count=0, infinite=True):
    if self.useMQTT and not self.isMQTT_connected:
      self.mqtt_enable()
      self.logMessage(f"Enabling MQTT connection: {self.isMQTT_connected}")
    if self.debug:
      self.logMessage(f"Start polling every {self.polling_interval} seconds slave id '{self.slave_id}'")
    if infinite and count < 1:
      while True:
        self.query()
        if self.useMQTT:
          self.client.loop(0.01)
          self.mqtt_publish()
        t.sleep(self.polling_interval)
    else:
      for i in range(0,count):
        self.query()
        if self.useMQTT:
           self.client.loop(0.01)
           self.mqtt_publish()
        t.sleep(self.polling_interval)
 
  def mqtt_prepareTopics(self, type=0):
    if type == 0:
      self.L1U  = f"{self.mqtt_topic}/L1_Voltage"
      self.L1F  = f"{self.mqtt_topic}/L1_Frequency"
      self.L1I  = f"{self.mqtt_topic}/L1_Current"
      self.L1P  = f"{self.mqtt_topic}/L1_Power"
      self.L1AP = f"{self.mqtt_topic}/L1_ActivePower"
      self.L1RP = f"{self.mqtt_topic}/L1_ReactivePower"
      self.L1ApP= f"{self.mqtt_topic}/L1_ApparentPower"
      self.L1PF = f"{self.mqtt_topic}/L1_PF"
      self.TP   = f"{self.mqtt_topic}/TotalPower"

  def mqtt_enable(self):
    self.mqtt_client_id=f'ORNO-{random.randint(1000, 8000)}'
    self.mqtt_prepareTopics(self.type)
    if self.debug:
      self.logMessage(f"Using MQTT Broker: '{self.mqtt_broker}:{self.mqtt_port}' with user '{self.mqtt_username}' and secret '{self.mqtt_password}'")
      self.logMessage(f"Using MQTT Topic : '{self.mqtt_topic}'")
    try:
      self.mqtt=self.mqtt_connect()
      self.isMQTT_connected = True
    except Exception as err:
        self.logMessage(f"mqtt_enable() ERROR: {err}")

  def mqtt_publish(self):
    try:
      if self.client.connected_flag:
        self.client.publish(self.L1U, f"{self.L1_voltage}")
        self.client.publish(self.L1F, f"{self.L1_frequency}")
        self.client.publish(self.L1I, f"{self.L1_current}")
        self.client.publish(self.L1P, f"{self.L1_power}")
        self.client.publish(self.L1AP, f"{self.L1_APower}")
        self.client.publish(self.L1RP, f"{self.L1_RPower}")
        self.client.publish(self.L1ApP, f"{self.L1_ApPower}")
        self.client.publish(self.L1PF, f"{self.L1_PF}")
        self.client.publish(self.TP, f"{self.TotalPower}")
      else:
        self.logMessage(f"Publish Error: no connection")
        self.client.loop(0.01)
        self.mqtt_actual_connection_try = self.mqtt_actual_connection_try + 1
        if self.mqtt_actual_connection_try < self.mqtt_connect_retry_count:
          self.client.loop(0.01)
          t.sleep(1)
          self.logMessage(f"mqtt_publish(): Error - retry {self.mqtt_actual_connection_try}")
          self.mqtt_publish()
        else:
          raise
    except Exception as err:
        self.logMessage(f"mqtt_publish() ERROR: {err}")
        self.mqtt_enable()
        self.client.loop(0.01)

  def mqtt_on_disconnect(self, client, userdata, flags, rc=0):
    self.logMessageprint("DisConnected flags"+"result code "+str(rc)+"client_id  ")
    client.connected_flag=False

  def mqtt_on_connect(self, client, userdata, flags, rc):
    if rc == 0:
      self.logMessage("ORNO/MQTT: Connected to MQTT Broker!")
      client.connected_flag=True
      self.mqtt_actual_connection_try = 0
    else:
      self.logMessage("ORNO/MQTT: Failed to connect, return code %d\n", rc)
      client.bad_connection_flag=True
  
  def mqtt_on_log(self, client, userdata, level, buf):
      if self.debug:
        self.logMessage(f"{buf}")

  def mqtt_connect(self):
    self.client = mqtt_client.Client(self.mqtt_client_id)
    self.client.on_log = self.mqtt_on_log
    self.client.connected_flag=False 
    self.client.bad_connection_flag=False 
    self.client.retry_count=0 
    self.client.username_pw_set(self.mqtt_username, self.mqtt_password)
    self.client.on_connect = self.mqtt_on_connect
    self.client.on_disconnect = self.mqtt_on_disconnect
    try:
      self.client.connect(self.mqtt_broker, self.mqtt_port)
      self.client.loop(0.01)
    except Exception as err:
        self.logMessage(f"mqtt_connect() ERROR: {err}")
    return self.client
