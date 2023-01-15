#!/usr/bin/python3
import minimalmodbus
import serial
import time

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
	time.sleep(2)
