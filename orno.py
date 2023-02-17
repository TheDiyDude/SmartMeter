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
SDM72DV2         = 2                                      # FC=04, Digits=2, Type=float

TotalPower                    = 0xA001,   -1,0x0034
Total_Voltage                 =     -1,   -1,0x0038
L1_Voltage                    = 0x131,0x000E,0x0000
L2_Voltage                    = 0x131,0x0010,0x0002
L3_Voltage                    = 0x131,0x0012,0x0004
L1_Frequency                  = 0x130,0x0014,0x0046
L2_Frequency                  = 0x130,0x0014,0x0046
L3_Frequency                  = 0x130,0x0014,0x0046
Total_Current                 =    -1,    -1,0x0030
L1_Current                    = 0x13A,0x0016,0x0006
L2_Current                    = 0x13A,0x0018,0x0008
L3_Current                    = 0x13A,0x001A,0x000A
Total_ActivePower             =    -1,0x001C,    -1
L1_ActivePower                = 0x141,0x001E,0x000C
L2_ActivePower                = 0x141,0x0020,0x000E
L3_ActivePower                = 0x141,0x0022,0x0010
Total_ReactivePower           =    -1,0x0024,0x0158
L1_ReactivePower              = 0x149,0x0026,0x0018
L2_ReactivePower              = 0x149,0x0028,0x001A
L3_ReactivePower              = 0x149,0x002A,0x001C
Total_ApparentPower           =    -1,0x002C,    -1
L1_ApparentPower              = 0x151,0x002E,0x0012
L2_ApparentPower              = 0x151,0x0030,0x0014
L3_ApparentPower              = 0x151,0x0032,0x0016
Total_PF                      =    -1,0x0034,0x003E
L1_PF                         = 0x158,0x0036,0x001E      # SDM72D: Positive refers to forward current, negative refers to reverse current.
L2_PF                         = 0x158,0x0038,0x0020      # SDM72D: Positive refers to forward current, negative refers to reverse current.
L3_PF                         = 0x158,0x003A,0x0022      # SDM72D: Positive refers to forward current, negative refers to reverse current.
Total_ActiveEnergy            =    -1,0x0100,0x0156      # SDM72D: Total active energy equals to import + export
L1_ActiveEnergy               =    -1,0x0102,    -1
L2_ActiveEnergy               =    -1,0x0104,    -1
L3_ActiveEnergy               =    -1,0x0106,    -1
Total_ForwardActiveEnergy     =    -1,0x0108,0x0048
L1_ForwardActiveEnergy        =    -1,0x010A,    -1
L2_ForwardActiveEnergy        =    -1,0x010C,    -1
L3_ForwardActiveEnergy        =    -1,0x010E,    -1
Total_ReverseActiveEnergy     =    -1,0x0110,0x004A
L1_ReverseActiveEnergy        =    -1,0x0112,    -1
L2_ReverseActiveEnergy        =    -1,0x0114,    -1
L3_ReverseActiveEnergy        =    -1,0x0116,    -1
T1_TotalActiveEnergy          =    -1,0x0130,    -1
T1_ForwardActiveEnergy        =    -1,0x0132,    -1
T1_ReverseActiveEnergy        =    -1,0x0134,    -1
T2_TotalActiveEnergy          =    -1,0x013C,    -1
T2_ForwardActiveEnergy        =    -1,0x013E,    -1
T2_ReverseActiveEnergy        =    -1,0x0140,    -1
T3_TotalActiveEnergy          =    -1,0x0148,    -1
T3_ForwardActiveEnergy        =    -1,0x014A,    -1
T3_ReverseActiveEnergy        =    -1,0x014C,    -1
T4_TotalActiveEnergy          =    -1,0x0154,    -1
T4_ForwardActiveEnergy        =    -1,0x0156,    -1
T4_ReverseActiveEnergy        =    -1,0x0158,    -1
GridFrequency                 = 0x130,0x0014,0x0046
Net_Power                     =    -1,    -1,0x018C             # Import-Export
Total_Import_Active_Power     =    -1,    -1,0x0500
Total_Export_Active_Power     =    -1,    -1,0x0502
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
    if type == WE514:
        self.fc=3
    elif type == WE516:
        self.fc=3
    elif type == WE517:
        self.fc=3
    elif type == SDM72DV2:
        self.fc=4
    else: 
        self.fc=3
    self.mqtt_actual_connection_try = 0
    self.mqtt_connect_retry_count = 60
    self.mqtt_connect_sleep_time  = 320
    self.mqtt_client_id=""
    self.smartmeter = minimalmodbus.Instrument(self.port, self.slave_id)
    self.smartmeter.serial.baudrate = 9600
    self.smartmeter.serial.bytesize = 8
    if type == SDM72DV2:
      self.smartmeter.serial.parity = serial.PARITY_NONE
    else:
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
    if register == 0 and self.type == SDM72DV2:
      self.L1_APower = self.smartmeter.read_register(L1_ActivePower[self.type],4,self.fc)
      self.L2_APower = self.smartmeter.read_register(L2_ActivePower[self.type],4,self.fc)
      self.L3_APower = self.smartmeter.read_register(L3_ActivePower[self.type],4,self.fc)
      self.L1_frequency = self.smartmeter.read_register(L1_Frequency[self.type],4,self.fc)
      print(f"F: {self.L1_frequency}")
      self.TotalActivePower = self.smartmeter.read_register(L1_ActivePower[self.type],4,self.fc)
      self.TotalForwardActiveEnergy = self.smartmeter.read_register(Total_ForwardActiveEnergy[self.type],4,self.fc)
      self.TotalReverseActiveEnergy = self.smartmeter.read_register(Total_ReverseActiveEnergy[self.type],4,self.fc)  
    if register == 0 and self.type == WE514:
      self.L1_frequency = self.smartmeter.read_register(L1_Frequency[self.type],2,self.fc)
      self.L1_voltage   = self.smartmeter.read_register(L1_Voltage[self.type],2,self.fc)
      self.L1_current   = self.smartmeter.read_register(L1_Current[self.type],3,self.fc)
      self.L1_power     = self.L1_voltage * self.L1_current
      self.L1_PF        = self.smartmeter.read_register(L1_PF[self.type],3,self.fc)
      self.TotalPower   = self.smartmeter.read_register(TotalPower[self.type],2,self.fc)
      self.L1_APower    = self.smartmeter.read_register(L1_ActivePower[self.type],3,self.fc)
      self.L1_RPower    = self.smartmeter.read_register(L1_ReactivePower[self.type],3,self.fc)
      self.L1_ApPower   = self.smartmeter.read_register(L1_ApparentPower[self.type],3,self.fc)
    elif register == 0 and self.type == WE517:
      self.L1_frequency = self.read_float(L1_Frequency[self.type],2,self.fc)
      self.L1_voltage   = self.read_float(L1_Voltage[self.type],2,self.fc)
      self.L1_current   = self.read_float(L1_Current[self.type],2,self.fc)
      self.L1_power     = self.L1_voltage * self.L1_current
      self.L1_PF        = self.read_float(L1_PF[self.type],2,self.fc)
      self.L1_APower    = self.read_float(L1_ActivePower[self.type],2,self.fc)
      self.L1_RPower    = self.read_float(L1_ReactivePower[self.type],2,self.fc)
      self.L1_ApPower   = self.read_float(L1_ApparentPower[self.type],2,self.fc)
      self.L1_AEnergy   = self.read_float(L1_ActiveEnergy[self.type],2,self.fc)
      self.L1_FAEnergy  = self.read_float(L1_ForwardActiveEnergy[self.type],2,self.fc)
      self.L1_RAEnergy  = self.read_float(L1_ReverseActiveEnergy[self.type],2,self.fc)
      self.L2_frequency = self.read_float(L1_Frequency[self.type],2,self.fc)
      self.L2_voltage   = self.read_float(L2_Voltage[self.type],2,self.fc)
      self.L2_current   = self.read_float(L2_Current[self.type],2,self.fc)
      self.L2_power     = self.L2_voltage * self.L2_current
      self.L2_PF        = self.read_float(L2_PF[self.type],2,self.fc)
      self.L2_APower    = self.read_float(L2_ActivePower[self.type],2,self.fc)
      self.L2_RPower    = self.read_float(L2_ReactivePower[self.type],2,self.fc)
      self.L2_ApPower   = self.read_float(L2_ApparentPower[self.type],2,self.fc)
      self.L2_AEnergy   = self.read_float(L2_ActiveEnergy[self.type],2,self.fc)
      self.L2_FAEnergy  = self.read_float(L2_ForwardActiveEnergy[self.type],2,self.fc)
      self.L2_RAEnergy  = self.read_float(L2_ReverseActiveEnergy[self.type],2,self.fc)
      self.L3_frequency = self.read_float(L3_Frequency[self.type],2,self.fc)
      self.L3_voltage   = self.read_float(L3_Voltage[self.type],2,self.fc)
      self.L3_current   = self.read_float(L3_Current[self.type],2,self.fc)
      self.L3_power     = self.L3_voltage * self.L3_current
      self.L3_PF        = self.read_float(L3_PF[self.type],2,self.fc)
      self.L3_APower    = self.read_float(L3_ActivePower[self.type],2,self.fc)
      self.L3_RPower    = self.read_float(L3_ReactivePower[self.type],2,self.fc)
      self.L3_ApPower   = self.read_float(L3_ApparentPower[self.type],2,self.fc)
      self.L3_AEnergy   = self.read_float(L3_ActiveEnergy[self.type],2,self.fc)
      self.L3_FAEnergy  = self.read_float(L3_ForwardActiveEnergy[self.type],2,self.fc)
      self.L3_RAEnergy  = self.read_float(L3_ReverseActiveEnergy[self.type],2,self.fc)
      self.TotalPower   = self.read_float(Total_ActiveEnergy[self.type],2,self.fc)
      self.TotalActivePower = self.read_float(Total_ActivePower[self.type],2,self.fc)
      self.TotalReactivePower = self.read_float(Total_ReactivePower[self.type],2,self.fc)
      self.TotalApparentPower = self.read_float(Total_ApparentPower[self.type],2,self.fc)
      self.TotalPF            = self.read_float(Total_PF[self.type],2,self.fc)
      self.TotalActiveEnergy  = self.read_float(Total_ActiveEnergy[self.type],2,self.fc)
      self.TotalForwardActiveEnergy = self.read_float(Total_ForwardActiveEnergy[self.type],2,self.fc)
      self.TotalReverseActiveEnergy = self.read_float(Total_ReverseActiveEnergy[self.type],2,self.fc)
      self.T1_TotalActiveEnergy     = self.read_float(T1_TotalActiveEnergy[self.type],2,self.fc)
      self.T1_ForwardActiveEnergy   = self.read_float(T1_ForwardActiveEnergy[self.type],2,self.fc)
      self.T1_ReverseActiveEnergy   = self.read_float(T1_ReverseActiveEnergy[self.type],2,self.fc)
      self.T2_TotalActiveEnergy     = self.read_float(T2_TotalActiveEnergy[self.type],2,self.fc)
      self.T2_ForwardActiveEnergy   = self.read_float(T2_ForwardActiveEnergy[self.type],2,self.fc)
      self.T2_ReverseActiveEnergy   = self.read_float(T2_ReverseActiveEnergy[self.type],2,self.fc)
      self.T3_TotalActiveEnergy     = self.read_float(T3_TotalActiveEnergy[self.type],2,self.fc)
      self.T3_ForwardActiveEnergy   = self.read_float(T3_ForwardActiveEnergy[self.type],2,self.fc)
      self.T3_ReverseActiveEnergy   = self.read_float(T3_ReverseActiveEnergy[self.type],2,self.fc)
      self.T4_TotalActiveEnergy     = self.read_float(T4_TotalActiveEnergy[self.type],2,self.fc)
      self.T4_ForwardActiveEnergy   = self.read_float(T4_ForwardActiveEnergy[self.type],2,self.fc)
      self.T4_ReverseActiveEnergy   = self.read_float(T4_ReverseActiveEnergy[self.type],2,self.fc)
    elif register == -1 and self.type == self.WE514:
      self.L1_voltage   = self.smartmeter.read_register(L1_Voltage[self.type],2,self.fc)
      self.L1_current   = self.smartmeter.read_register(L1_Current[self.type],3,self.fc)
      self.L1_power     = self.L1_voltage * self.L1_current
      return self.L1_power
    else:
      if self.type == 0:
        return self.smartmeter.read_register(register,decimals,self.fc)
      elif self.type == 1:
        return self.read_float(register,decimals,self.fc)

  def read_float(self, register=0, num=2, code=3, order=0):
      return self.smartmeter.read_float(register,code,num,order)
  
  def print(self):
    if self.type == SDM72DV2:
      print(f"L1 Power (Active)           {self.L1_APower:.3f} W")
      print(f"L2 Power (Active)           {self.L2_APower:.3f} W")
      print(f"L3 Power (Active)           {self.L3_APower:.3f} W")
      print(f"Frequency                   {self.L1_frequency:.3f} Hz")
      print(f"Net kWh (Import - Export)   {self.TotalActivePower:.3f} kWh")
      print(f"Total Import Power (Active) {self.TotalForwardActiveEnergy:.3f} W")
      print(f"Total Export Power (Active) {self.TotalReverseActiveEnergy:.3f} W")
    if self.type == WE514:
      print(f"L1 Voltage                  {self.L1_voltage:.0f} V")
      print(f"L1 Frequency                {self.L1_frequency:.2f} Hz")
      print(f"L1 Current                  {self.L1_current:.3f} A")
      print(f"L1 Power                    {self.L1_power:.3f} W")
      print(f"L1 Active Power             {self.L1_APower:.3f} kW")
      print(f"L1 Reactive Power           {self.L1_RPower:.3f} kvar")
      print(f"L1 Apparent Power           {self.L1_ApPower:.3f} kva")
      print(f"L1 Power Factor             {self.L1_PF:.2f}")
      print(f"Total Power                 {self.TotalPower:.2f} kWh")
    elif self.type == WE517:
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
    if type == WE514:
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
    if type == WE517:
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
        if self.type == WE514:
          self.client.publish(self.L1U, f"{self.L1_voltage}")
          self.client.publish(self.L1F, f"{self.L1_frequency}")
          self.client.publish(self.L1I, f"{self.L1_current}")
          self.client.publish(self.L1P, f"{self.L1_power}")
          self.client.publish(self.L1AP, f"{self.L1_APower}")
          self.client.publish(self.L1RP, f"{self.L1_RPower}")
          self.client.publish(self.L1ApP, f"{self.L1_ApPower}")
          self.client.publish(self.L1PF, f"{self.L1_PF}")
          self.client.publish(self.TP, f"{self.TotalPower}")
        if self.type == WE517:
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
