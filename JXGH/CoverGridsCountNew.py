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
LOG_FILE_NAME = "E:\\GisPython\\JXGH\\logs\\coverGridsCount.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)


def excuteSql():
    print "excute sql......."
    logging.info("excute sql.......")
    dbConn=pyodbc.connect('DRIVER={SQL Server};SERVER=10.48.186.82;DATABASE=JZXN_GIS;UID=sa;PWD=!!Fj@nsn123')
    cursor =dbConn.cursor()
    logging.info("db connect success~")
    
    getGridShapeSql = "INSERT INTO [dbo].GIS_COVER_GRIDS_COUNT"+yestday.strftime('%Y%m%d')      
    getGridShapeSql += "           ([OBJECTID]                                                              "
    getGridShapeSql += "           ,[VDATE_D]                                                               "
    getGridShapeSql += "           ,[city]                                                                  "
    getGridShapeSql += "           ,[grid_id]                                                               "
    getGridShapeSql += "           ,[subtotal_weak]                                                         "
    getGridShapeSql += "           ,[subtotal_cdfg]                                                         "
    getGridShapeSql += "           ,[subtotal_gfg]                                                          "
    getGridShapeSql += "           ,[weakcover_rate]                                                        "
    getGridShapeSql += "           ,[cdfg_rate]                                                             "
    getGridShapeSql += "           ,[gfg_rate]                                                              "
    getGridShapeSql += "           ,[totcnt]                                                                "
    getGridShapeSql += "           ,[rsrp_avg]                                                              "
    getGridShapeSql += "           ,[SCORE]                                                                 "
    getGridShapeSql += "           ,[POI],[POI_TYPE],[SCORE_WEAK],[SCORE_CDFG],[SCORE_GFG]                  "
    getGridShapeSql += "           ,[SHAPE])                                                                "
    getGridShapeSql += "SELECT ROW_NUMBER() OVER( order by a.VDATE_D desc ) AS OBJECTID,a.[VDATE_D]         "
    getGridShapeSql += "      ,a.[city]                                                                     "
    getGridShapeSql += "      ,a.[grid_id]                                                                  "
    getGridShapeSql += "      ,a.[subtotal_weak]                                                            "
    getGridShapeSql += "      ,a.[subtotal_cdfg]                                                            "
    getGridShapeSql += "      ,a.[subtotal_gfg]                                                             "
    getGridShapeSql += "      ,a.[weakcover_rate]                                                           "
    getGridShapeSql += "      ,a.[cdfg_rate]                                                                "
    getGridShapeSql += "      ,a.[gfg_rate]                                                                 "
    getGridShapeSql += "      ,a.[totcnt]                                                                   "
    getGridShapeSql += "      ,a.[rsrp_avg]                                                                 "
    getGridShapeSql += "      ,a.[SCORE]                                                                    "
    getGridShapeSql += "      ,a.[POI],a.[POI_TYPE],a.[SCORE_WEAK],a.[SCORE_CDFG],a.[SCORE_GFG]             "
    getGridShapeSql += "      ,b.shape                                                                      "
    getGridShapeSql += "FROM [JZXN_GIS].[dbo].[cover] a ,[JZXN_GIS].[dbo].GIS_MGRS_JXGH_FW b                     "
    getGridShapeSql += " where a.RSRP_LEVEL=110 and a.VDATE_D = '"+yestday.strftime('%Y-%m-%d')+"' and a.GRID_ID = b.MGRS"
    cursor.execute(getGridShapeSql)
    dbConn.commit()    
    dbConn.close()


    
if __name__ == '__main__':
    try:
        print "coverGridsCount start..."
        logging.info("coverGridsCount start...")
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        logging.info(time.strftime('%Y-%m-%d %H:%M:%S',startTime))
        
        yestday = datetime.datetime.now() + datetime.timedelta(days=-15)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.2\\ArcCatalog"
        GISDBPath = "JZXNGis82.sde"
        env.workspace = ArcCatalogPath+"\\"+GISDBPath
        infc = ArcCatalogPath+"\\"+GISDBPath+"\\JZXN_GIS.dbo.cover"
        gisFeature110 = ArcCatalogPath+"\\"+GISDBPath+"\\JZXN_GIS.DBO.GIS_COVER_GRIDS_COUNT"+yestday.strftime('%Y%m%d')
        gisTemp = ArcCatalogPath+"\\"+GISDBPath+"\\JZXN_GIS.DBO.GIS_COVER_GRIDS_COUNT_TEMP"

        print "create feature110-->"+gisFeature110
        logging.info("create feature110-->"+gisFeature110)
        if(arcpy.Exists(gisFeature110)):
            arcpy.Delete_management(gisFeature110)
        gisFeature110 = arcpy.CreateFeatureclass_management(os.path.dirname(gisFeature110),os.path.basename(gisFeature110),"Polygon",gisTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        arcpy.DeleteField_management (gisFeature110, "SHAPE_STArea__") 
        arcpy.DeleteField_management (gisFeature110, "SHAPE_STLength__")
        
        excuteSql()
        
        print "AddSpatialIndex ..."
        logging.info("AddSpatialIndex...")
        
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0) 
