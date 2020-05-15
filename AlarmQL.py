# -*- coding: utf-8 -*-
import arcpy
from arcpy import env
import math
import string
import time,datetime
import logging
import os
import pyodbc
import httplib, urllib, json
import getpass

spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\logs\\qlAlarm.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)
shapefieldname = "SHAPE"
FidFieldName = "OBJECTID"



if __name__ == '__main__':
    try:
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        DataSourcePath = "WangYouGis_Oracle97.sde"
        GISDBPath = "WangYouGis_Site15.sde"
        intable = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.GIS_ALARM_VIEW"
        alarmPoints = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouSite.DBO.GIS_ALARM_QL"
        alarmFields=arcpy.ListFields(alarmPoints)
        
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        dbConn=pyodbc.connect('DRIVER={SQL Server};SERVER=10.209.13.15;DATABASE=WangYouSite;UID=sa;PWD=Nsn_sj3Yd!Jf8')
        deleteSql = "delete from [WangYouSite].[dbo].[GIS_ALARM_QL]"
        cursor =dbConn.cursor()
        cursor.execute(deleteSql)
        dbConn.commit()
        dbConn.close()
        print "delete success!~"
        del dbConn
        print "search begin!"
        searchCursor=arcpy.SearchCursor(intable,"ALARM_STATUS = 1")
        print "search complete!"
        print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        alarmInsertCur=arcpy.InsertCursor(alarmPoints)
        nextAlarmRow=searchCursor.next()
        while nextAlarmRow:
            #print nextAlarmRow.getValue("NE_NAME")
            p_lon = nextAlarmRow.getValue("LONGITUDE")
            p_lat = nextAlarmRow.getValue("LATITUDE")
            if(p_lon is not None and p_lat is not None and p_lon>110 and p_lon<130 and p_lat>20 and p_lat<30):
                alarmPnt = arcpy.Point(p_lon, p_lat)
                resultRow=alarmInsertCur.newRow()
                resultRow.shape=alarmPnt
                for alarmField in alarmFields:
                    XQFieldName = alarmField.name
                    if(XQFieldName!="OBJECTID" and XQFieldName!=shapefieldname):
                        resultRow.setValue(XQFieldName,nextAlarmRow.getValue(XQFieldName))
                alarmInsertCur.insertRow(resultRow)
            nextAlarmRow=searchCursor.next()
        del searchCursor,alarmInsertCur,nextAlarmRow,resultRow
        print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0) 
