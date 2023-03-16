#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path
from os import path



class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class CustomError(Error):
    """Exception raised for errors in the input.

        Attributes:
        expr -- input expression in which the error occurred
        msg  -- explanation of the error
    """

    def __init__(self,  msg):
        self.msg = msg
        
    def getMsg(self):
        return self.msg


class FileTextManage:
    """gestiona un archivo de textos"""
    def __init__(self,logM,filename,CustomError):
        self.logF=logM
        self.filename=filename

    def creaArchivo(self):
        #rutaActual=os.path.abspath(os.getcwd())
        try:
            self.miFile=open(self.filename, "a+")
        except:
            self.logF.error("el archivo {} no pudo crearse. verifique los permisos".format(self.filename))
            raise CustomError

        return self.miFile
        
    def escribir(self, texto):        
        self.miFile.write(texto)

    def cierraArchivo(self):
        try:
            self.miFile.close()
        except:
            self.logF.mensaje("el archivo {} no pudo cerrarse !!!!".format(self.filename))
            return False

        self.logF.mensaje("el archivo {} fue cerrado".format(self.filename))