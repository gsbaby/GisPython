# -*- coding: utf-8 -*-
import arcpy
import time,datetime
import os
import cx_Oracle
import logging

spatialReference = arcpy.SpatialReference(4326)
ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
LOG_FILE_NAME = "E:\\GisPython\\logs\\moveLteSxToOracle.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)
    
if __name__ == '__main__':
    try:
        print "moveLteSxToOracle start..."
        logging.info("moveLteSxToOracle start...")
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        yestday = datetime.datetime.now() + datetime.timedelta(days=-2)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        logging.info(yestday.strftime('%Y-%m-%d 00:00:00'))
        SqlGISDBPath = "WangYouCellThiess60.sde"
        OracleGISDBPath = "PDB_PMSDB.sde"
        gisSqlTable = ArcCatalogPath+"\\"+SqlGISDBPath+"\\WangYouCellThiess.DBO.GIS_OBJECT_LTESX"+yestday.strftime('%Y%m%d')
        gisOracleTable = ArcCatalogPath+"\\"+OracleGISDBPath+"\\SDE.GIS_OBJECT_LTESX"
        if(arcpy.Exists(gisSqlTable)):
            if(arcpy.Exists(gisOracleTable)):
                print "delete feature-->"+gisOracleTable
                logging.info("delete feature-->"+gisOracleTable)
                arcpy.Delete_management(gisOracleTable)
            print "create feature-->"+gisOracleTable
            logging.info("create feature-->"+gisOracleTable)
            arcpy.FeatureClassToFeatureClass_conversion(gisSqlTable,ArcCatalogPath+"\\"+OracleGISDBPath,"GIS_OBJECT_LTESX","1=1")
            arcpy.DeleteField_management (gisOracleTable, "SHAPE_STArea__") 
            arcpy.DeleteField_management (gisOracleTable, "SHAPE_STLength__")
        print "moveLteSxToOracle   END ..."
        logging.info("moveLteSxToOracle  END...")
        
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0) 
