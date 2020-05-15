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
LOG_FILE_NAME = "E:\\GisPython\\APP\\logs\\creatAppPntTable.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)

    
if __name__ == '__main__':
    try:
        print "creat appPnt table start..."
        logging.info("creat appPnt table start...")
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        logging.info(time.strftime('%Y-%m-%d %H:%M:%S',startTime))
        
        yestday = datetime.datetime.now() + datetime.timedelta(days=+1)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        GISDBPath = "PDB_PMSDB.sde"
        gisFeatureOrg = ArcCatalogPath+"\\"+GISDBPath+"\\SDE.gis_app_measured_pnt"+yestday.strftime('%Y%m%d')
        gisTemp = ArcCatalogPath+"\\"+GISDBPath+"\\SDE.gis_app_measured_pnt"

        print "create feature-->"+gisFeatureOrg
        logging.info("create feature-->"+gisFeatureOrg)
        if(arcpy.Exists(gisFeatureOrg)):
            arcpy.Delete_management(gisFeatureOrg)
        gisFeatureOrg = arcpy.CreateFeatureclass_management(os.path.dirname(gisFeatureOrg),os.path.basename(gisFeatureOrg),"Point",gisTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        
        print "creat table end ..."
        logging.info("creat table end...")
        
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0) 
