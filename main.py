#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Matomo pipeline components """
import os
import socket
import sys
import time
import logging

import pdb
from nuevoProceso import ProcesoManage
from utils import LogReg
from cartero import Cartero
logger=LogReg()
pid = str(os.getpid())
pidfile = "/tmp/matomoSNRD.pid"

if os.path.isfile(pidfile):
    print ("%s el archivo existe, el proceso se esta ejecutando. Si estas seguro de que no se esta corriendo borra el archivo." % pidfile)
    logger.error("Se quizo iniciar un nuevo proceso!!!")
    sys.exit()

file(pidfile, 'w').write(pid)

try:
    
    sesion=ProcesoManage(logger)
    
    #cartero=Cartero()       
    #cartero.sendMail(procResult)

    print ("enviando mail...")
    
    
finally:
    os.unlink(pidfile)


