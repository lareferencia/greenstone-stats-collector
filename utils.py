#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Matomo pipeline components """
from datetime import datetime, timedelta, date
import logging
from  fileTextManage import FileTextManage, CustomError, Error
import pdb
class LogReg:
    """Clase que se encarga de registrar en el log"""
    logger=""

    def __init__(self):
        self.logger = logging.getLogger('MATOMO_SRND_UPDATE')
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler('debug.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.mensaje('iniciando proceso de actualizacion del motomo srnd')
        
    def mensaje(self,texto):
        self.logger.info(texto)
        pass

    def error(self,texto):
        self.logger.error(texto)
        pass
    def cuidado(self,texto):
        self.logger.warning(texto)
        pass
    def dameFecha(self):
        fecha=datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
        return fecha


class SeguimientoFile:
    """Esta clase gestiona un archivo de texto donde se guardan los datos entre una actualización y la siguient
        su objetivo es marcar hasta donde llegó el proceso anterior y desde donde debe actualizarse el nuevo proceso"""

    def __init__(self,log):
        archivo = FileTextManage(log,'seguimiento_rsnd.txt',CustomError)
        self.logF=log
        self.miSegFile=archivo.creaArchivo()        

    def dameUltimoRow(self):
        valor=""
        lines = self.miSegFile.readlines()
        for line in lines:
            valor=line
        return valor

    
    def escribir(self, texto):
        self.miSegFile.truncate(0)
        self.miSegFile.write(texto)

    def cierraArchivo(self):
        try:
            self.miSegFile.close()
        except:
            self.logF.mensaje("Ojota: el archivo seguimiento_rsnd.txt no pudo cerrarse !!!!")
            return False
        return True



class Utils:
    def __init__(self):
        pass

    def fechaReglada(self,fech):
        """devuelve un  diccionario con las claves: Y,M,D,H,M,S 
        o la fecha completa en string en la clave: formateado"""
        feIN =fech

        xdi=feIN.day
        xmes=feIN.month
        xhora=feIN.hour
        xminuto=feIN.minute

        try:
            xseg=feIN.second
            if xseg==0:
                xseg="00"
            if xseg<10:
                xseg="0{}".format(str(xseg))
        except:
            xseg="00"


        if xhora==0:
            xhora="00"
        elif xhora<10:
            xhora="0{}".format(str(xhora))

        if xminuto==0:
            xminuto="00"
        elif xminuto<10:
            xminuto="0{}".format(str(xminuto))


        if xdi<10:
            xdi="0"+str(xdi)
        if xmes<10:
            xmes="0"+str(xmes)

        params = dict()
        params['Y'] = str(feIN.year)
        params['M'] = str(xmes)
        params['D'] = str(xdi)
        params['H'] = str(xhora)
        params['Min'] = str(xminuto)
        params['S'] = str(xseg)
        params['formateado']="{}-{}-{} {}:{}:{}".format(params['Y'],params['M'],params['D'],params['H'],params['Min'],params['S'])
        

        return params