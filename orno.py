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
import socket
from paho.mqtt import client as mqtt_client
from datetime import datetime

WE514            = 0
WE516            = 1
WE517            = 1

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
    self.mqtt_client_id=""
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
      self.L2_frequency = self.read_float(L1_Frequency[self.type],2)
      self.L2_voltage   = self.read_float(L2_Voltage[self.type],2)
      self.L2_current   = self.read_float(L2_Current[self.type],2)
      self.L2_power     = self.L2_voltage * self.L2_current
      self.L2_PF        = self.read_float(L2_PF[self.type],2)
      self.L2_APower    = self.read_float(L2_ActivePower[self.type],2)
      self.L2_RPower    = self.read_float(L2_ReactivePower[self.type],2)
      self.L2_ApPower   = self.read_float(L2_ApparentPower[self.type],2)
      self.L2_AEnergy   = self.read_float(L2_ActiveEnergy[self.type],2)
      self.L2_FAEnergy  = self.read_float(L2_ForwardActiveEnergy[self.type],2)
      self.L2_RAEnergy  = self.read_float(L2_ReverseActiveEnergy[self.type],2)
      self.L3_frequency = self.read_float(L3_Frequency[self.type],2)
      self.L3_voltage   = self.read_float(L3_Voltage[self.type],2)
      self.L3_current   = self.read_float(L3_Current[self.type],2)
      self.L3_power     = self.L3_voltage * self.L3_current
      self.L3_PF        = self.read_float(L3_PF[self.type],2)
      self.L3_APower    = self.read_float(L3_ActivePower[self.type],2)
      self.L3_RPower    = self.read_float(L3_ReactivePower[self.type],2)
      self.L3_ApPower   = self.read_float(L3_ApparentPower[self.type],2)
      self.L3_AEnergy   = self.read_float(L3_ActiveEnergy[self.type],2)
      self.L3_FAEnergy  = self.read_float(L3_ForwardActiveEnergy[self.type],2)
      self.L3_RAEnergy  = self.read_float(L3_ReverseActiveEnergy[self.type],2)
      self.TotalPower   = self.read_float(Total_ActiveEnergy[self.type],2)
      self.TotalActivePower = self.read_float(Total_ActivePower[self.type],2)
      self.TotalReactivePower = self.read_float(Total_ReactivePower[self.type],2)
      self.TotalApparentPower = self.read_float(Total_ApparentPower[self.type],2)
      self.TotalPF            = self.read_float(Total_PF[self.type],2)
      self.TotalActiveEnergy  = self.read_float(Total_ActiveEnergy[self.type],2)
      self.TotalForwardActiveEnergy = self.read_float(Total_ForwardActiveEnergy[self.type],2)
      self.TotalReverseActiveEnergy = self.read_float(Total_ReverseActiveEnergy[self.type],2)
      self.T1_TotalActiveEnergy     = self.read_float(T1_TotalActiveEnergy[self.type],2)
      self.T1_ForwardActiveEnergy   = self.read_float(T1_ForwardActiveEnergy[self.type],2)
      self.T1_ReverseActiveEnergy   = self.read_float(T1_ReverseActiveEnergy[self.type],2)
      self.T2_TotalActiveEnergy     = self.read_float(T2_TotalActiveEnergy[self.type],2)
      self.T2_ForwardActiveEnergy   = self.read_float(T2_ForwardActiveEnergy[self.type],2)
      self.T2_ReverseActiveEnergy   = self.read_float(T2_ReverseActiveEnergy[self.type],2)
      self.T3_TotalActiveEnergy     = self.read_float(T3_TotalActiveEnergy[self.type],2)
      self.T3_ForwardActiveEnergy   = self.read_float(T3_ForwardActiveEnergy[self.type],2)
      self.T3_ReverseActiveEnergy   = self.read_float(T3_ReverseActiveEnergy[self.type],2)
      self.T4_TotalActiveEnergy     = self.read_float(T4_TotalActiveEnergy[self.type],2)
      self.T4_ForwardActiveEnergy   = self.read_float(T4_ForwardActiveEnergy[self.type],2)
      self.T4_ReverseActiveEnergy   = self.read_float(T4_ReverseActiveEnergy[self.type],2)
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
      print(f"L1 Voltage                  {self.L1_voltage:.0f} V")
      print(f"L1 Frequency                {self.L1_frequency:.2f} Hz")
      print(f"L1 Current                  {self.L1_current:.3f} A")
      print(f"L1 Power                    {self.L1_power:.3f} W")
      print(f"L1 Active Power             {self.L1_APower:.3f} kW")
      print(f"L1 Reactive Power           {self.L1_RPower:.3f} kvar")
      print(f"L1 Apparent Power           {self.L1_ApPower:.3f} kva")
      print(f"L1 Power Factor             {self.L1_PF:.2f}")
      print(f"Total Power                 {self.TotalPower:.2f} kWh")
    elif self.type == 1:
      print(f"L1 Voltage                  {self.L1_voltage:.0f} V")
      print(f"L1 Frequency                {self.L1_frequency:.2f} Hz")
      print(f"L1 Current                  {self.L1_current:.3f} A")
      print(f"L1 Power                    {self.L1_power:.3f} W")
      print(f"L1 Active Power             {self.L1_APower:.3f} kW")
      print(f"L1 Reactive Power           {self.L1_RPower:.3f} kvar")
      print(f"L1 Apparent Power           {self.L1_ApPower:.3f} kva")
      print(f"L1 Power Factor             {self.L1_PF:.2f}")
      print(f"L1 Active Energy            {self.L1_AEnergy:.3f} kWh")
      print(f"L1 Forward Active Energy    {self.L1_FAEnergy:.3f} kWh")
      print(f"L1 Reverse Active Energy    {self.L1_RAEnergy:.3f} kWh")
      print(f"L2 Voltage                  {self.L2_voltage:.0f} V")
      print(f"L2 Frequency                {self.L2_frequency:.2f} Hz")
      print(f"L2 Current                  {self.L2_current:.3f} A")
      print(f"L2 Power                    {self.L2_power:.3f} W")
      print(f"L2 Active Power             {self.L2_APower:.3f} kW")
      print(f"L2 Reactive Power           {self.L2_RPower:.3f} kvar")
      print(f"L2 Apparent Power           {self.L2_ApPower:.3f} kva")
      print(f"L2 Power Factor             {self.L2_PF:.2f}")
      print(f"L2 Active Energy            {self.L2_AEnergy:.3f} kWh")
      print(f"L2 Forward Active Energy    {self.L2_FAEnergy:.3f} kWh")
      print(f"L2 Reverse Active Energy    {self.L2_RAEnergy:.3f} kWh")
      print(f"L3 Voltage                  {self.L3_voltage:.0f} V")
      print(f"L3 Frequency                {self.L3_frequency:.2f} Hz")
      print(f"L3 Current                  {self.L3_current:.3f} A")
      print(f"L3 Power                    {self.L3_power:.3f} W")
      print(f"L3 Active Power             {self.L3_APower:.3f} kW")
      print(f"L3 Reactive Power           {self.L3_RPower:.3f} kvar")
      print(f"L3 Apparent Power           {self.L3_ApPower:.3f} kva")
      print(f"L3 Power Factor             {self.L3_PF:.2f}")
      print(f"L3 Active Energy            {self.L3_AEnergy:.3f} kWh")
      print(f"L3 Forward Active Energy    {self.L3_FAEnergy:.3f} kWh")
      print(f"L3 Reverse Active Energy    {self.L3_RAEnergy:.3f} kWh")
      print(f"Grid Frequency              {self.L1_frequency:.2f} Hz")
      print(f"Total Active Power          {self.TotalActivePower:.2f} kWh")
      print(f"Total Reactive Power        {self.TotalReactivePower:.2f} kvar")
      print(f"Total Apparent Power        {self.TotalApparentPower:.2f} kva")
      print(f"Total PF                    {self.TotalPF:.2f} ")
      print(f"Total Active Energy         {self.TotalActiveEnergy:.2f} kWh")
      print(f"Total Forward Active Energy {self.TotalForwardActiveEnergy:.2f} kWh")
      print(f"Total Reverse Active Energy {self.TotalReverseActiveEnergy:.2f} kWh")
      print(f"T1 Total Active Energy      {self.T1_TotalActiveEnergy:.2f} kWh")
      print(f"T1 Forward Active Energy    {self.T1_ForwardActiveEnergy:.2f} kWh")
      print(f"T1 Reverse Active Energy    {self.T1_ReverseActiveEnergy:.2f} kWh")
      print(f"T2 Total Active Energy      {self.T2_TotalActiveEnergy:.2f} kWh")
      print(f"T2 Forward Active Energy    {self.T2_ForwardActiveEnergy:.2f} kWh")
      print(f"T2 Reverse Active Energy    {self.T2_ReverseActiveEnergy:.2f} kWh")
      print(f"T3 Total Active Energy      {self.T3_TotalActiveEnergy:.2f} kWh")
      print(f"T3 Forward Active Energy    {self.T3_ForwardActiveEnergy:.2f} kWh")
      print(f"T3 Reverse Active Energy    {self.T3_ReverseActiveEnergy:.2f} kWh")
      print(f"T4 Total Active Energy      {self.T4_TotalActiveEnergy:.2f} kWh")
      print(f"T4 Forward Active Energy    {self.T4_ForwardActiveEnergy:.2f} kWh")
      print(f"T4 Reverse Active Energy    {self.T4_ReverseActiveEnergy:.2f} kWh")

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
          self.mqtt_publish()
        t.sleep(self.polling_interval)
    else:
      for i in range(0,count):
        self.query()
        if self.useMQTT:
           self.mqtt_publish()
        t.sleep(self.polling_interval)
    if self.useMQTT:
          self.client.loop(0.10)
 
  def mqtt_prepareTopics(self, type=0):
    if type == 0:
      #self.mqtt_topic = f"{self.mqtt_topic}/WE-514"
      self.L1U  = f"{self.mqtt_topic}/L1_Voltage"
      self.L1F  = f"{self.mqtt_topic}/L1_Frequency"
      self.L1I  = f"{self.mqtt_topic}/L1_Current"
      self.L1P  = f"{self.mqtt_topic}/L1_Power"
      self.L1AP = f"{self.mqtt_topic}/L1_ActivePower"
      self.L1RP = f"{self.mqtt_topic}/L1_ReactivePower"
      self.L1ApP= f"{self.mqtt_topic}/L1_ApparentPower"
      self.L1PF = f"{self.mqtt_topic}/L1_PF"
      self.TP   = f"{self.mqtt_topic}/TotalPower"
    if type == 1:
      #self.mqtt_topic = f"{self.mqtt_topic}/WE-517"
      self.L1U   = f"{self.mqtt_topic}/L1_Voltage"
      self.L1F   = f"{self.mqtt_topic}/L1_Frequency"
      self.L1I   = f"{self.mqtt_topic}/L1_Current"
      self.L1P   = f"{self.mqtt_topic}/L1_Power"
      self.L1AP  = f"{self.mqtt_topic}/L1_ActivePower"
      self.L1RP  = f"{self.mqtt_topic}/L1_ReactivePower"
      self.L1ApP = f"{self.mqtt_topic}/L1_ApparentPower"
      self.L1PF  = f"{self.mqtt_topic}/L1_PF"
      self.L1AE  = f"{self.mqtt_topic}/L1_ActiveEnergy"
      self.L1FAE = f"{self.mqtt_topic}/L1_ForwardActiveEnergy"
      self.L1RAE = f"{self.mqtt_topic}/L1_ReverseActiveEnergy"
      self.L2U   = f"{self.mqtt_topic}/L2_Voltage"
      self.L2F   = f"{self.mqtt_topic}/L2_Frequency"
      self.L2I   = f"{self.mqtt_topic}/L2_Current"
      self.L2P   = f"{self.mqtt_topic}/L2_Power"
      self.L2AP  = f"{self.mqtt_topic}/L2_ActivePower"
      self.L2RP  = f"{self.mqtt_topic}/L2_ReactivePower"
      self.L2ApP = f"{self.mqtt_topic}/L2_ApparentPower"
      self.L2PF  = f"{self.mqtt_topic}/L2_PF"
      self.L2AE  = f"{self.mqtt_topic}/L2_ActiveEnergy"
      self.L2FAE = f"{self.mqtt_topic}/L2_ForwardActiveEnergy"
      self.L2RAE = f"{self.mqtt_topic}/L2_ReverseActiveEnergy"
      self.L3U   = f"{self.mqtt_topic}/L3_Voltage"
      self.L3F   = f"{self.mqtt_topic}/L3_Frequency"
      self.L3I   = f"{self.mqtt_topic}/L3_Current"
      self.L3P   = f"{self.mqtt_topic}/L3_Power"
      self.L3AP  = f"{self.mqtt_topic}/L3_ActivePower"
      self.L3RP  = f"{self.mqtt_topic}/L3_ReactivePower"
      self.L3ApP = f"{self.mqtt_topic}/L3_ApparentPower"
      self.L3PF  = f"{self.mqtt_topic}/L3_PF"
      self.L3AE  = f"{self.mqtt_topic}/L3_ActiveEnergy"
      self.L3FAE = f"{self.mqtt_topic}/L3_ForwardActiveEnergy"
      self.L3RAE = f"{self.mqtt_topic}/L3_ReverseActiveEnergy"
      self.GF    = f"{self.mqtt_topic}/GridFrequency"
      self.TAP   = f"{self.mqtt_topic}/Total_ActivePower"
      self.TRP   = f"{self.mqtt_topic}/Total_ReactivePower"
      self.TApP  = f"{self.mqtt_topic}/Total_ApparentPower"
      self.TPF   = f"{self.mqtt_topic}/Total_PF"
      self.TAE   = f"{self.mqtt_topic}/Total_ActiveEnergy"
      self.TFAE  = f"{self.mqtt_topic}/Total_ForwardActiveEnergy"
      self.TRAE  = f"{self.mqtt_topic}/Total_ReverseActiveEnergy"
      self.T1TAE = f"{self.mqtt_topic}/T1_Total_ActiveEnergy"
      self.T1TFAE= f"{self.mqtt_topic}/T1_Total_ForwardActiveEnergy"
      self.T1TRAE= f"{self.mqtt_topic}/T1_Total_ReverseActiveEnergy"
      self.T2TAE = f"{self.mqtt_topic}/T2_Total_ActiveEnergy"
      self.T2TFAE= f"{self.mqtt_topic}/T2_Total_ForwardActiveEnergy"
      self.T2TRAE= f"{self.mqtt_topic}/T2_Total_ReverseActiveEnergy"
      self.T3TAE = f"{self.mqtt_topic}/T3_Total_ActiveEnergy"
      self.T3TFAE= f"{self.mqtt_topic}/T3_Total_ForwardActiveEnergy"
      self.T3TRAE= f"{self.mqtt_topic}/T3_Total_ReverseActiveEnergy"
      self.T4TAE = f"{self.mqtt_topic}/T4_Total_ActiveEnergy"
      self.T4TFAE= f"{self.mqtt_topic}/T4_Total_ForwardActiveEnergy"
      self.T4TRAE= f"{self.mqtt_topic}/T4_Total_ReverseActiveEnergy"

  def mqtt_enable(self):
    #self.mqtt_client_id=f'ORNO-{random.randint(1000, 8000)}'
    if self.mqtt_client_id == "":
      self.mqtt_client_id=f"ORNO-{socket.gethostname()}-{random.randint(1000, 8000)}"
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
        if self.type == 0:
          self.client.publish(self.L1U, f"{self.L1_voltage}")
          self.client.publish(self.L1F, f"{self.L1_frequency}")
          self.client.publish(self.L1I, f"{self.L1_current}")
          self.client.publish(self.L1P, f"{self.L1_power}")
          self.client.publish(self.L1AP, f"{self.L1_APower}")
          self.client.publish(self.L1RP, f"{self.L1_RPower}")
          self.client.publish(self.L1ApP, f"{self.L1_ApPower}")
          self.client.publish(self.L1PF, f"{self.L1_PF}")
          self.client.publish(self.TP, f"{self.TotalPower}")
        if self.type == 1:
          self.client.publish(self.L1U, f"{self.L1_voltage}")
          self.client.publish(self.L1F, f"{self.L1_frequency}")  
          self.client.publish(self.L1I, f"{self.L1_current}")   
          self.client.publish(self.L1P, f"{self.L1_power}")   
          self.client.publish(self.L1AP, f"{self.L1_APower}")  
          self.client.publish(self.L1RP, f"{self.L1_RPower}")  
          self.client.publish(self.L1ApP, f"{self.L1_ApPower}") 
          self.client.publish(self.L1PF, f"{self.L1_PF}")  
          self.client.publish(self.L1AE, f"{self.L1_AEnergy}")  
          self.client.publish(self.L1FAE, f"{self.L1_FAEnergy}") 
          self.client.publish(self.L1RAE, f"{self.L1_RAEnergy}") 
          self.client.publish(self.L2U, f"{self.L2_voltage}")   
          self.client.publish(self.L2F, f"{self.L2_frequency}")   
          self.client.publish(self.L2I, f"{self.L2_current}")   
          self.client.publish(self.L2P, f"{self.L2_power}")   
          self.client.publish(self.L2AP, f"{self.L2_APower}")  
          self.client.publish(self.L2RP, f"{self.L2_RPower}")  
          self.client.publish(self.L2ApP, f"{self.L2_ApPower}") 
          self.client.publish(self.L2PF, f"{self.L2_PF}")  
          self.client.publish(self.L2AE, f"{self.L2_AEnergy}")  
          self.client.publish(self.L2FAE, f"{self.L2_FAEnergy}") 
          self.client.publish(self.L2RAE, f"{self.L2_RAEnergy}") 
          self.client.publish(self.L3U, f"{self.L3_voltage}")   
          self.client.publish(self.L3F, f"{self.L3_frequency}")   
          self.client.publish(self.L3I, f"{self.L3_current}")   
          self.client.publish(self.L3P, f"{self.L3_power}")   
          self.client.publish(self.L3AP, f"{self.L3_APower}")  
          self.client.publish(self.L3RP, f"{self.L3_RPower}")  
          self.client.publish(self.L3ApP, f"{self.L3_ApPower}") 
          self.client.publish(self.L3PF, f"{self.L3_PF}")  
          self.client.publish(self.L3AE, f"{self.L3_AEnergy}")  
          self.client.publish(self.L3FAE, f"{self.L3_FAEnergy}") 
          self.client.publish(self.L3RAE, f"{self.L3_RAEnergy}")
          self.client.publish(self.GF, f"{self.L1_frequency}")   
          self.client.publish(self.TAP, f"{self.TotalActivePower}")
          self.client.publish(self.TRP, f"{self.TotalReactivePower}")
          self.client.publish(self.TApP, f"{self.TotalApparentPower}")
          self.client.publish(self.TPF, f"{self.TotalPF}")
          self.client.publish(self.TAE, f"{self.TotalActiveEnergy}")
          self.client.publish(self.TFAE, f"{self.TotalForwardActiveEnergy}")
          self.client.publish(self.TRAE, f"{self.TotalReverseActiveEnergy}")
          self.client.publish(self.T1TAE, f"{self.T1_TotalActiveEnergy}")
          self.client.publish(self.T1TFAE, f"{self.T1_ForwardActiveEnergy}")
          self.client.publish(self.T1TRAE, f"{self.T1_ReverseActiveEnergy}")
          self.client.publish(self.T2TAE, f"{self.T2_TotalActiveEnergy}")
          self.client.publish(self.T2TFAE, f"{self.T2_ForwardActiveEnergy}")
          self.client.publish(self.T2TRAE, f"{self.T2_ReverseActiveEnergy}")
          self.client.publish(self.T3TAE, f"{self.T3_TotalActiveEnergy}")
          self.client.publish(self.T3TFAE, f"{self.T3_ForwardActiveEnergy}")
          self.client.publish(self.T3TRAE, f"{self.T3_ReverseActiveEnergy}")
          self.client.publish(self.T4TAE, f"{self.T4_TotalActiveEnergy}")
          self.client.publish(self.T4TFAE, f"{self.T4_ForwardActiveEnergy}")
          self.client.publish(self.T4TRAE, f"{self.T4_ReverseActiveEnergy}")
      else:
        self.logMessage(f"Publish Error: no connection")
        self.client.loop(0.01)
        self.mqtt_actual_connection_try = self.mqtt_actual_connection_try + 1
        if self.mqtt_actual_connection_try < self.mqtt_connect_retry_count:
          self.client.loop(0.01)
          t.sleep(10)
          self.logMessage(f"mqtt_publish(): Error - retry {self.mqtt_actual_connection_try}")
          self.mqtt_publish()
        else:
          raise
    except Exception as err:
        self.logMessage(f"mqtt_publish() ERROR: {err}")
        #self.mqtt_enable()
        #self.client.loop(0.01)

  def mqtt_on_disconnect(self, client, userdata, flags, rc=0):
    self.logMessage(f"mqtt_onDisconnect: DisConnected {flags}"+" result code: "+str(rc)+" {client_id}  ")
    if rc == 0:
     self.client.reconnect()
    if rc != 0:
     self.logMessage(f"mqtt_onDisconnect: Verbindung verloren!")
     client.connected_flag=False
     t.sleep(30)
     self.client.reconnect()

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
      self.client.loop_start()
    except Exception as err:
        self.logMessage(f"mqtt_connect() ERROR: {err}")
    return self.client
