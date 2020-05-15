# -*- coding: utf-8 -*-
import arcpy
from arcpy import env
import math
import string
import time,datetime
import logging
import os
import pyodbc

spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "D:\\GisPython\\create_event.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)

    
if __name__ == '__main__':
    try:
        print "creat VOLTE table start..."
        logging.info("creat VOLTE table start...")
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        logging.info(time.strftime('%Y-%m-%d %H:%M:%S',startTime))
        
        yestday = datetime.datetime.now() + datetime.timedelta(days=+4)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        
        ArcCatalogPath = "C:\Users\Administrator\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog"
        GISDBPath = "Connection to sde.sde"
        gisFeatureOrg = ArcCatalogPath+"\\"+GISDBPath+"\\SDE.GEO_AgrInsuData_"+yestday.strftime('%Y%m%d')
        gisTemp = ArcCatalogPath+"\\"+GISDBPath+"\\SDE.GEO_AIDate"

        print "create feature-->"+gisFeatureOrg
        logging.info("create feature-->"+gisFeatureOrg)
        if(arcpy.Exists(gisFeatureOrg)):
            arcpy.Delete_management(gisFeatureOrg)
        gisFeatureOrg = arcpy.CreateFeatureclass_management(os.path.dirname(gisFeatureOrg),os.path.basename(gisFeatureOrg),"Polygon",gisTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        
        print "creat table end ..."
        logging.info("creat table end...")
        
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0) 
