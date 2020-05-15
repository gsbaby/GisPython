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
LOG_FILE_NAME = "E:\\GisPython\\APP\\logs\\creatAppTable.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)
startTime = time.localtime(time.time())
curDay = startTime.tm_mday
lastMonthEnd = datetime.datetime.now()-datetime.timedelta(days=curDay)

    
if __name__ == '__main__':
    try:
        print "creat table start..."
        logging.info("creat table start...")
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        logging.info(time.strftime('%Y-%m-%d %H:%M:%S',startTime))
        
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        GISDBPath = "PDB_PMSDB.sde"
        gisFeatureOrg = ArcCatalogPath+"\\"+GISDBPath+"\\SDE.gis_app_measured_data"+lastMonthEnd.strftime('%Y%m')
        gisTemp = ArcCatalogPath+"\\"+GISDBPath+"\\SDE.gis_app_measured_data"

        print "create feature-->"+gisFeatureOrg
        logging.info("create feature-->"+gisFeatureOrg)
        if(arcpy.Exists(gisFeatureOrg)):
            arcpy.Delete_management(gisFeatureOrg)
        gisFeatureOrg = arcpy.CreateFeatureclass_management(os.path.dirname(gisFeatureOrg),os.path.basename(gisFeatureOrg),"Polygon",gisTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        arcpy.DeleteField_management (gisFeatureOrg, "SHAPE_STArea__") 
        arcpy.DeleteField_management (gisFeatureOrg, "SHAPE_STLength__")
        
        print "creat table end ..."
        logging.info("creat table end...")
        
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0) 
