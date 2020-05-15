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
import ConversionUtils
# Define local variables
VDATE_D = ConversionUtils.gp.GetParameterAsText(0)

spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\JXGH\\logs\\coverGridsCount.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)
#VDATE_D="20161206"


def excuteSql():
    print "excute sql......."
    logging.info("excute sql.......")
    dbConn=pyodbc.connect('DRIVER={SQL Server};SERVER=10.48.177.4;DATABASE=JZXN_GIS;UID=sa;PWD=!!Fj@nsn123')
    logging.info("db connect success~")
    getGridShapeSql = "INSERT INTO [dbo].GIS_COVER_GRIDS_COUNT"+VDATE_D      
    getGridShapeSql += "           ([OBJECTID]                                                           "
    getGridShapeSql += "           ,[VDATE_D]                                                              "
    getGridShapeSql += "           ,[city]                                                               "
    getGridShapeSql += "           ,[grid_id]                                                            "
    getGridShapeSql += "           ,[subtotal_weak]                                                      "
    getGridShapeSql += "           ,[subtotal_cdfg]                                                      "
    getGridShapeSql += "           ,[subtotal_gfg]                                                       "
    getGridShapeSql += "           ,[weakcover_rate]                                                     "
    getGridShapeSql += "           ,[cdfg_rate]                                                          "
    getGridShapeSql += "           ,[gfg_rate]                                                           "
    getGridShapeSql += "           ,[totcnt]                                                             "
    getGridShapeSql += "           ,[rsrp_avg]                                                           "
    getGridShapeSql += "           ,[SCORE]                                                           "
    getGridShapeSql += "           ,[POI],[POI_TYPE],[SCORE_WEAK],[SCORE_CDFG],[SCORE_GFG]"
    getGridShapeSql += "           ,[SHAPE])                                                             "
    getGridShapeSql += "SELECT ROW_NUMBER() OVER( order by a.VDATE_D desc ) AS OBJECTID,a.[VDATE_D]          "
    getGridShapeSql += "      ,a.[city]                                                                  "
    getGridShapeSql += "      ,a.[grid_id]                                                               "
    getGridShapeSql += "      ,a.[subtotal_weak]                                                         "
    getGridShapeSql += "      ,a.[subtotal_cdfg]                                                         "
    getGridShapeSql += "      ,a.[subtotal_gfg]                                                          "
    getGridShapeSql += "      ,a.[weakcover_rate]                                                        "
    getGridShapeSql += "      ,a.[cdfg_rate]                                                             "
    getGridShapeSql += "      ,a.[gfg_rate]                                                              "
    getGridShapeSql += "      ,a.[totcnt]                                                                "
    getGridShapeSql += "      ,a.[rsrp_avg]                                                              "
    getGridShapeSql += "      ,a.[SCORE]                                                              "
    getGridShapeSql += "      ,b.Name,a.[POI_TYPE],a.[SCORE_WEAK],a.[SCORE_CDFG],a.[SCORE_GFG]                                                              "
    getGridShapeSql += "      ,b.shape                                                              "
    getGridShapeSql += "FROM [JZXN_GIS].[dbo].[cover] a ,[JZXN_GIS].[dbo].FUJIANCHENGQUGRIDSNEW_1 b      "
    getGridShapeSql+= " where VDATE_D = '"+VDATE_D+"' and a.GRID_ID = b.OBJECTID"
    cursor =dbConn.cursor()
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
        
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        GISDBPath = "JZXNGis.sde"
        env.workspace = ArcCatalogPath+"\\"+GISDBPath
        infc = ArcCatalogPath+"\\"+GISDBPath+"\\JZXN_GIS.dbo.cover"
        gisFeatureOrg = ArcCatalogPath+"\\"+GISDBPath+"\\JZXN_GIS.DBO.GIS_COVER_GRIDS_COUNT"+VDATE_D
        gisTemp = ArcCatalogPath+"\\"+GISDBPath+"\\JZXN_GIS.DBO.GIS_COVER_GRIDS_COUNT_TEMP"

        print "create feature-->"+gisFeatureOrg
        logging.info("create feature-->"+gisFeatureOrg)
        if(arcpy.Exists(gisFeatureOrg)):
            arcpy.Delete_management(gisFeatureOrg)
        gisFeatureOrg = arcpy.CreateFeatureclass_management(os.path.dirname(gisFeatureOrg),os.path.basename(gisFeatureOrg),"Polygon",gisTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        arcpy.DeleteField_management (gisFeatureOrg, "SHAPE_STArea__") 
        arcpy.DeleteField_management (gisFeatureOrg, "SHAPE_STLength__")
        excuteSql()
        print "AddSpatialIndex ..."
        logging.info("AddSpatialIndex...")
        
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0) 
