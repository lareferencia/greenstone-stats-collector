#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import pdb
from utils import Utils
from config import DB_URL,DB_PUERTO,DB_U_NAME ,DB_U_PASS,DB_NAME    
""" Conecta a la base de datos y gestiona las consultas a la tabla MySQLdb.connections.Connection"""
class DBC:    
    miLog=""

    def __init__(self,myLog):        
        self.miLog=myLog
        self.tools=Utils()
        conctado=self.conectar()
        

    def cerrarConeccion(self):
        try:
            self.con.close()
        except:
            print("ocurrió un error al cerrar la base.")
            return False

        print("la base se cerró exitosamente.")


    def dameIdsPorRangoFecha(self, fechaIni , fechaFin):
        """Devuleve un listado de row.document y row.id dada una fecha...
           SELECT document FROM accepted_records WHERE time_stamp>"2020-06-01 00:00:00" AND time_stamp<"2020-06-01 23:59:59" 
        """

        cur = self.con
        listado=[]
        
        try:
            print ("SELECT document,id FROM accepted_records WHERE time_stamp>='{}' and time_stamp<='{}'".format(fechaIni,fechaFin))
            cur.execute("SELECT document,id FROM accepted_records WHERE time_stamp>='{}' and time_stamp<='{}'".format(fechaIni,fechaFin))            
        except MySQLdb.OperationalError as error:
            self.miLog.error("Error al realizar la consulta: (%s)" % error )
            return False
        records = cur.fetchall()
        
        

        for rowA in records:
            crudo=""

            idrow=rowA[1]
            if rowA[0][0:1]=='/':
                row=rowA[0][1:100]
            else:
                row=rowA[0]
                
            if (row.find('.pdf') != -1):
                #es un pdf
                crudo=row.split("/")[1]
            else:
                crudo=row[row.find("&d=")+3:]

            listado.append((crudo,idrow))

        return listado

    def dameTotalDocs(self,idDoc,idParaRuta,rangInicial,fechaIni,fechaFin):
       
        total=0
        cur = self.con        
        print ("-------------")
        print ("try mysql count para id: {}".format(idDoc))        
        try:
            cur.execute("SELECT COUNT(*) FROM accepted_records WHERE (document like '%d={}' OR document like '%/{}/%') AND (time_stamp>='{}' and time_stamp<='{}')".format(idDoc,idParaRuta,fechaIni,fechaFin))
            total=cur.fetchone()[0]
        except MySQLdb.OperationalError as error:
            self.miLog.error("Error al realizar la consulta: (%s)" % error )
            return False
             
        
        #self.miLog.mensaje('... La consulta por {} y {} devolvio: {} documentos'.format(idDoc,idParaRuta,total))     
        print ('devolvio: {} documentos'.format(total))
        print ("-------------")
        return total

    def dameRowById(self,idRow):
        """Devuelvo el row completo dado un id de la tabla """
        cur = self.con
        try:
            cur.execute("SELECT process_uuid,referer,user_agent,remote_host,time_stamp,document,id FROM accepted_records WHERE id ={}".format(idRow))
            total=cur.fetchall()
        except MySQLdb.OperationalError as error:
            self.miLog.error("Error al realizar la consulta: (%s)" % error )
            return False

        result=[]
        
        for row in total:
            




            result.append({
                '_id':"",
                'urlref':row[1],
                'ua':row[2],
                'cip':row[3],
                'cdt':self.tools.fechaReglada(row[4])['formateado'],
                'document':row[5],
                'id':int(row[6])})
        return result

    def generaConsulta(self,idDoc,idParaRuta,rangInicial,cantRowsRango=50,fechaIni='',fechaFin=''):
        """ genera la consulta para un documento por su idDoc(documento de descarga) y idParaRuta (documento consultado)
            los paramtreos son idDoc=Jpr12  y idParaRuta=pr.12 y el ademas es necesario enviar 
            el rangInicial que es la ROW a partir de la cual queremos hacer la consulta.
        """
        self.miLog.mensaje('Generando consulta para {} y {}, iniciando desde {}...'.format(idDoc,idParaRuta,rangInicial))
        result=[]

        try:
            cur = self.con
            #strA="SELECT process_uuid,referer,user_agent,remote_host,time_stamp,document,id FROM accepted_records WHERE (document like '%d={}' OR document like '%/{}/%') AND  (time_stamp between '{}' and '{}') ORDER BY id LIMIT {},{}".format(idDoc,idParaRuta,fechaIni,fechaFin,rangInicial,cantRowsRango)
            cur.execute("SELECT process_uuid,referer,user_agent,remote_host,time_stamp,document,id FROM accepted_records WHERE (document like '%d={}' OR document like '%/{}/%') AND  (time_stamp between '{}' and '{}') ORDER BY id LIMIT {},{}".format(idDoc,idParaRuta,fechaIni,fechaFin,rangInicial,cantRowsRango))
        except:            
            self.miLog.error('....Hubo un error al generar la consulta')
            return False

            
        # print all the first cell of all the rows
        for row in cur.fetchall():
            result.append({
                '_id':row[0],
                'urlref':row[1],
                'ua':row[2],
                'cip':row[3],
                'cdt':row[4],
                'document':row[5],
                'id':int(row[6])})
            
        #self.cerrarConeccion()
        
        return result



    def conectar(self):
        url     =DB_URL 
        porto   = DB_PUERTO
        username=DB_U_NAME
        password=DB_U_PASS

        db_name =DB_NAME 

        print( "intentando enlazar la base de datos...")
    
        try:
            db= MySQLdb.connect(host=url, port=porto, user=username, passwd=password, db=db_name,connect_timeout = 2)
            self.con = db.cursor()
        except MySQLdb.Error as e:
            print ("Error al conectarse con la base {}".format(e))
            raise

        print ("conexion exitosa!")
        return True