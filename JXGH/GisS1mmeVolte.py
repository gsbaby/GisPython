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
import cx_Oracle

spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\JXGH\\logs\\GisS1mmeVolte.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)
startTime = time.localtime(time.time())
curDay = startTime.tm_mday
lastMonthEnd = datetime.datetime.now()-datetime.timedelta(days=curDay)

def excuteSql():
    print "excute sql......."
    logging.info("excute sql.......")
    dbConnOra=cx_Oracle.connect('sde/sde_nsn2015@10.48.186.102:1521/pdb_pmsdb')
    cursorOra = dbConnOra.cursor()
    logging.info("db connect success~")
    getGridShapeSql = "INSERT INTO SDE.GIS_S1MME_VOLTE_GRID"+yestday.strftime('%Y%m%d')      
    getGridShapeSql += "           (objectid                 "
    getGridShapeSql += "           ,dateid                   "
    getGridShapeSql += "           ,grid_city                "
    getGridShapeSql += "           ,grid_county              "
    getGridShapeSql += "           ,lon_grid                 "
    getGridShapeSql += "           ,lat_grid                 "    
    getGridShapeSql += "           ,grid_id                  "
    getGridShapeSql += "           ,grid                     "
    getGridShapeSql += "           ,cityid                   "
    getGridShapeSql += "           ,partid                   "
    getGridShapeSql += "           ,lte_users                "
    getGridShapeSql += "           ,volte_users              "
    getGridShapeSql += "           ,dots_ltescrsrp_avg,flag  "
    getGridShapeSql += "           ,shape)                   "
    getGridShapeSql += "SELECT rownum AS OBJECTID,a.dateid   "
    getGridShapeSql += "      ,a.grid_city                   "
    getGridShapeSql += "      ,a.grid_county                 "
    getGridShapeSql += "      ,a.lon_grid                    "
    getGridShapeSql += "      ,a.lat_grid                    "
    getGridShapeSql += "      ,a.grid_id                     "
    getGridShapeSql += "      ,a.grid                        "
    getGridShapeSql += "      ,a.cityid                      "
    getGridShapeSql += "      ,a.partid                      "
    getGridShapeSql += "      ,a.lte_users                   "
    getGridShapeSql += "      ,a.volte_users                 "
    getGridShapeSql += "      ,a.dots_ltescrsrp_avg,a.flag   "
    getGridShapeSql += "      ,b.shape                       "
    getGridShapeSql += "FROM SDE.mroid_s1mme_volte_day a ,SDE.gis_cover_grids_jxgh_all b"
    getGridShapeSql+= " where a.dateid = '"+yestday.strftime('%Y%m%d')+"' and a.grid_id = b.OBJECTID"
    print getGridShapeSql
    cursorOra.execute(getGridShapeSql)
    dbConnOra.commit()
    dbConnOra.close()


if __name__ == '__main__':
    try:
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        DataSourcePath = "PDB_PMSDB.sde"
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        yestday = datetime.datetime.now() + datetime.timedelta(days=-34)
        print yestday.strftime('%Y%m%d')
        
        tmpS1mmeVolte = ArcCatalogPath+"\\"+DataSourcePath+"\\SDE.GIS_S1MME_VOLTE_GRID"
        GisS1mmeVolte = ArcCatalogPath+"\\"+DataSourcePath+"\\SDE.GIS_S1MME_VOLTE_GRID"+yestday.strftime('%Y%m%d')

        if(arcpy.Exists(GisS1mmeVolte)):
            arcpy.Delete_management(GisS1mmeVolte)
        GisS1mmeVolte = arcpy.CreateFeatureclass_management(os.path.dirname(GisS1mmeVolte),os.path.basename(GisS1mmeVolte),"Polygon",tmpS1mmeVolte,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        arcpy.DeleteField_management (GisS1mmeVolte, "SHAPE_STArea__") 
        arcpy.DeleteField_management (GisS1mmeVolte, "SHAPE_STLength__")
        excuteSql()
        
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0) 
