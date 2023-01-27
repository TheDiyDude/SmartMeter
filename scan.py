#!/usr/bin/python3
#
# ModBus Address Scanner 
#
# This shows how to scann addresses via RS-485 ModBus protocol. 
#
# See REAME.MD for more information and hardware setup.
#
# Author: Marc-Oliver Blumenauer 
#         marc@l3c.de
#
# License: MIT
#
import orno
import time as t
from datetime import datetime
import sys

if len(sys.argv) == 1:
  print(f"Usage: {sys.argv[0]} StartAddress EndAddress\nusing default address range 0x0000 to 0x0360\n")

if len(sys.argv) < 2:
  addr  = int("0x0000",16)
  end   = int("0x0360",16)
else:
  addr  = int(sys.argv[1],16)
  end   = int(sys.argv[2],16)

instrument=orno.orno('/dev/ttyUSB0',type=orno.WE517)



print(f"Scanning SLAVE ID 1: from address {addr} to address {end}")

while addr <= end:
  try:
    time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print(f"{time} -- Address 0x{addr:04x}: {instrument.query(addr,2)}")  
  except Exception as ex:
    #print(f"Addr {addr}: error")
    err = ex
  t.sleep(0.01)  
  addr = addr + 1
