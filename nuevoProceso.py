#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Matomo pipeline components """
import __main__

from datetime import datetime, timedelta, date
import re
import random
import json
import urllib
import urllib2
import math
from  fileTextManage import FileTextManage, CustomError, Error
from dbManager import DBC
import requests
import pdb
from utils import Utils,SeguimientoFile
from config import PATH_GREEN_OUT,GREEN_OUT_FILE_NAME,MATOMO_TRACKER_URL, MATOMO_TOKEN,MATOMO_BACH_SIZE


__main__.__file__ = 'matomoSNRD'






class ProcesoManage:
    """ Gestor de proceso y punto de inicio """
    miLog       = ""
    dbEventos   = ""
    segInitRang = ""
    docIdsTitulos=""
    requestMatomo=""
    rowsPorRequest=MATOMO_BACH_SIZE

    def __init__(self,logger):
        print ("nuevo Proceso!!!!!" )
        now = datetime.now()
        fechaInicial = now.strftime("%Y-%m-%d %H:%M:%S")
        self.tools=Utils()
        self.sesion=dict()

        """diccionario que tiene los datos del seguimiento"""

        self.sesion['fechaInicio']  =fechaInicia
        self.sesion['fechaFin']     =''
        self.sesion['proc_rangoIni']=''        
        self.sesion['proc_rangoFin']='' 
        self.sesion['lastIdRow']    =''        
        self.miLog=logger
        
        
        ############################

        #Descargo el archivo que contiene los ids y los títulos de los documentos

        ############################
        descargarUltimosIds=self.getFileFromURL(PATH_GREEN_OUT)
        self.miLog.mensaje("Iniciando la descarga del archivo de IDs y Titulos")
        if descargarUltimosIds[0]:
            self.miLog.mensaje("Descarga completa.")
        else:
            self.miLog.error("Error en la descarga: {}").format(descargarUltimosIds[1])
            raise Error("no se pudo leer la salida Greeenstone")
            return ""

        try:
            #leo el archivo de seguimiento
            segFile=SeguimientoFile(self.miLog)

            datosSeguimiento=segFile.dameUltimoRow()
            self.sesion['proc_rangoIni'] =datosSeguimiento.split('|')[0]
            self.sesion['lastIdRow']     =datosSeguimiento.split('|')[1]
            self.sesion['proc_rangoFin'] =datosSeguimiento.split('|')[2].rstrip()
            self.sesion['registrosProcesados']=0
            
            if self.sesion['proc_rangoFin']!='0000-00-00 00:00:00':
                """Hay procesos que terminar!!!"""
                self.miLog.mensaje("Se detectó un proceso sin terminar....")
                #esNuevoProceso=False
            else:
                self.sesion['proc_rangoFin']=fechaInicial

        except CustomError:
            self.miLog.error('Error abriendo el archivo de seguimiento: no se cargar una fecha para el incio de la consulta')
        
        feIN =datetime.strptime(self.sesion['proc_rangoIni'],'%Y-%m-%d %H:%M:%S')
        feOut=datetime.strptime(self.sesion['proc_rangoFin'],'%Y-%m-%d %H:%M:%S')


        ############################

        #Genero un listado con cada dia del rango para hacer
        #la búsqueda en la base por dichas fechas

        ############################

        rangoDiasQ=self.gen_days(feIN,feOut)

        #-----------------------
        #-Solo ejecuto el primer dia del rango oar
        # rangoDiasQ=[rangoDiasQ[0]]
        #-----------------------


        
        try:
            self.dbEventos=DBC(self.miLog)
            self.miLog.mensaje("Conectado con la base eventos.")
        except:
            self.miLog.error("No se pudo conctar con la DB de Eventos.")
            self.miLog.mensaje("No hay más pasos. Proceso terminado.")
            #raise Error('Problemas conectando con la base')
            raise CustomError('Problemas conectando con la base')
        
        
        
        ############################

        #Lee el Archivo con los IDS y Titulos

        ############################

        try:
            mbf=FileTextManage(self.miLog,GREEN_OUT_FILE_NAME,CustomError)
            baseFile=mbf.creaArchivo()
            self.listaDatosDocTxt=[]
            self.miLog.mensaje('Ya se cargaron los IDS a procesar')
            print( "Ya se cargaron los IDS a procesar")
        except:
            self.miLog.error('No fue posible leer una lista de IDS. No se proceso nada. Fin de la tarea')
            raise CustomError('No fue posible leer una lista de IDS. Fin de la tarea')       
        
        
        try:
            flineas = baseFile.readlines()
            for xlinea in flineas:
                if xlinea.find("MX biblovuf")==-1:
                    #este condicional es para obviar un error que viene en el archivo
                    #if xlinea != "MX biblovuf lw=100000 fmtl=100000 pft=@/usr/local/etc/vufind/pftvuf/matomo/biblomatomo.pft now":
                    tmpr=xlinea.split('|')
                    self.listaDatosDocTxt.append({'idDoc':tmpr[1],'idParaRuta':tmpr[0],'titulo':tmpr[2]})
                    
        except:
            print ("Error linea a liena")

        try:
            ftm = FileTextManage(self.miLog,'matomoRequest.txt', CustomError)
            self.requestMatomo =ftm.creaArchivo()
        except:
            raise  CustomError("que lio  con el archivo!!!")

        ############################

        #Selecciono todos los rows por fecha
        
        ############################

        EMPAQUETA=[]
        
        for dia in rangoDiasQ:
            fech=dia
            litadoDocAenviar=self.procesaDia(fech)

            if len(litadoDocAenviar)==0:
                continue

            xRows = [] 
            completeRow=0            
            #Aca se guardan rows consolidados entre la base y los documentos
            for tmpQ in litadoDocAenviar:
                if 'idRow' in tmpQ.keys():
                    completeRow=self.dbEventos.dameRowById(tmpQ['idRow'])
                    #merge los dos diccionarios
                    z = completeRow[0].copy()
                    z.update(tmpQ)
                    xRows.append(z)
                else:
                    print( "el parametro idRow no existe en el objeto")
            print(str(len(xRows)))

            xRows     = xRows[:10]
            paginas   = math.ceil(len(xRows)/(self.rowsPorRequest+0.0))
            #paginas   = 1
            nroPagina = 0
            self.rrii = 0
            iniVuelta = 0-self.rowsPorRequest
            indice    =-1
            bufferEventos = []
            

            for pflag in range(int(paginas)):
                indice=indice+1
                self.rrii = self.rrii+self.rowsPorRequest
                iniVuelta = iniVuelta+self.rowsPorRequest
                subVuelta = 0
                for mm in xRows[iniVuelta:iniVuelta+self.rowsPorRequest]:
                    esDescarga=False
                    lstDescTmp=mm['document'].split('/')
                    #tmpR=rowdb['id']
                    if len(lstDescTmp)>2:
                        #DOCDESCARGA = lstDescTmp[1]
                        esDescarga=True
                    else:
                        lstDescTmp[1][lstDescTmp[1].rfind('=')+1:120]

                    params = dict()
                    params['idsite']= '50'
                    params['rec']   = '1'
                    params['action_name'] = mm["titulo"]
                    #row['document']
                    #params['_id']   = mm['_id']
                    #lo teniamos seteado al id del row de la base
                    params['_id']   = ''
                    lastIdRow       = mm['idRow']
                    params['rand']  = random.randint(1e5,1e6)
                    params['apiv']  = 1
                    params['urlref']= mm['urlref']
                    params['ua']    = mm['ua']
                    oaipmhID        = 'oai.example.com:snrd:{}'.format(mm["idDoc"])
                    params['cvar']  = json.dumps({"1": ["oaipmhID", oaipmhID],"2": ["repositoryID","1341"],"3": ["countryID","AR"] })
                    if esDescarga:
                        params['download'] = 'http://www.example.com/{}'.format(mm['document'])

                    params['url']   = 'http://www.example.com/{}'.format(mm['document'])
                    params['token_auth']   = MATOMO_TOKEN
                    params['cip']   = mm.get('cip', '0.0.0.0')
                    params['cdt']   = mm['cdt']
                    prarams         = dict()                    
                    prarams['_matomoRequest'] = '?' + urllib.urlencode(params)
                    subVuelta=subVuelta+1
                    bufferEventos.append(prarams['_matomoRequest'])

                resultFlush=self.flush(bufferEventos,(ftm, prarams['_matomoRequest'],params['cdt'] ))
                if resultFlush[0] == 'error':
                    print ("se precipitó un error, vea el log")
                    print (resultFlush[1])
                    txtSeguimiento ='{}|{}|{}'.format(params['cdt'], lastIdRow ,self.sesion['proc_rangoFin'])
                    print ("guardando archivo de seguimiento")
                    segFile.escribir(txtSeguimiento)
                    raise ValueError('Error al enviar los datos a MATOMO. Vea el log.')
                else:
                    nroPagina = nroPagina + 1
        



        ############################
        # terminando el proceso
        ############################


        txtSeguimiento ='{}|{}|{}'.format(params['cdt'], lastIdRow ,'0000-00-00 00:00:00')
        segFile.escribir(txtSeguimiento)
        self.miLog.mensaje('::FIN DEL PROCESO:: registros:{}'.format(self.sesion['registrosProcesados']))
        baseFile.close()
        
    def getDameProcesados(self):
        return self.sesion['registrosProcesados']
        
    def procesaDia(self,fech):
        """Busco en la base todos los registros del dia.
           Recorro los datos de GreenstonOut  para completar titulo y id del doc
           Devuelvo en una lista de objetos
            {'idDoc':'idParaRuta':'titulo':'idRow'}"""

        feIN=self.tools.fechaReglada(fech)

        diaRangoManana=feIN['formateado']
        diaRangoNoche="{}-{}-{} 23:59:59".format(feIN['Y'],feIN['M'],feIN['D'])

        self.miLog.mensaje("Dia a procesar {}".format(diaRangoManana))
        listadoHits = self.dbEventos.dameIdsPorRangoFecha(diaRangoManana,diaRangoNoche)

        
        tmpLista=[]
        if len(listadoHits)==0:
            return []

        for mysqlId in listadoHits:
            flagExist=False
            for tmpDoc in self.listaDatosDocTxt:
                if tmpDoc['idParaRuta'] == mysqlId[0]:
                    newO={'idDoc':tmpDoc['idDoc'],
                        'idParaRuta':tmpDoc['idParaRuta'],
                        'titulo':tmpDoc['titulo'],
                        'idRow':mysqlId[1]}
                    tmpLista.append(newO);

        #elimino esta variable
        
        del listadoHits

        
        if len(tmpLista)>0:
            return tmpLista
        else:
            return []

    def gen_days(self, st_date, en_date):
        #start_date=datetime( st_date.year, st_date.month, st_date.day)
        #end_date=datetime( en_date.year, en_date.month, en_date.day )
        start_date=st_date
        end_date=en_date
        d=start_date
        dates=[ start_date ]
        
        #return [start_date,en_date]
        while d < end_date:
            d += timedelta(days=1)
            dates.append( d )

        return dates
    

    def getFileFromURL(self,t_url):
        try:
            response = urllib2.urlopen(t_url)
        except urllib2.HTTPError as e:
            return (False,e.code)
        except urllib2.URLError as e:
            return (False,e.args)
        
        data = response.read()
        
        txt_str = str(data[:-1])
        lines = txt_str.split("\\n")
        des_url = GREEN_OUT_FILE_NAME
        fx = open(des_url,"w")

        for line in lines:
            fx.write(line)

        fx.close()
        return (True,"")

    def flush(self,eventos,compi):       
        
        num_events = len(eventos)
        
        url =  MATOMO_TRACKER_URL        
        data_dict=dict( requests = eventos, token_auth = MATOMO_TOKEN)



        #'http://httpbin.org/post   '
        #'utlima                    '
        self._lastTimestamp = compi[2]
  
        
        compi[0].escribir(json.dumps( data_dict))
        compi[0].escribir("\n")
        


        try:          
            
            
            #http_response = requests.post(url, data = json.dumps(data_dict))
            #data_dict = dict(requests=eventos, token_auth="4351bbananas5a8f3",cdt=compi[2])

            http_response = requests.post(url, data = json.dumps(data_dict))
            
            http_response.raise_for_status()
            json_response = json.loads(http_response.text)
            self.miLog.mensaje("LaReferencia responde: {}".format(json_response['status']))

            if json_response['status'] != "success" or json_response['invalid'] != 0 :
                self.miLog.error('Pareciera que el mensaj de respuesta de matomo no fue SUCCES...')
                return ('error',http_response.text)
                #raise ValueError(http_response.text)
                
        except requests.exceptions.HTTPError as err:
            self.miLog.errosegFiler('HTTP error occurred: {}'.format(err))
            return ('error','HTTP error occurred: {}'.format(err))
            #raise
        except:
            self.miLog.error('Error while posting events to tracker. URL: {}. Data: {}'.format(url, data_dict))
            return ('error','Error while posting events to tracker. URL')
            #raise
        self.sesion['registrosProcesados']= self.sesion['registrosProcesados']+len(eventos)

        self.miLog.mensaje('{} events sent to tracker'.format(num_events))
        self.miLog.mensaje('Local time for last sent event: {}'.format(self._lastTimestamp))
        #self._configContext.save_last_tracked_timestamp(self._lastTimestamp)
        self._requestsBuffer = []
        self._lastTimestamp = None
        return ('ok','buffer vacio')
    


def main():
    try:
        # run with pid locking mecanism
        with pid.PidFile(pidname='/tmp/matomoSNRD.pid') as p:
            ProcesoManage()
    
    # PidFileError captures locking problems or already running instances
    except pid.PidFileError as e:
        logger.error("El matomoSNRD esta en uso. El error es: %s" % e)
    
