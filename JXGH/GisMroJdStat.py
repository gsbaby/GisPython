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
LOG_FILE_NAME = "E:\\GisPython\\JXGH\\logs\\GisMroJdStat.log"
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
    getGridShapeSql = "INSERT INTO SDE.GIS_MRO_GRID_JD_STAT"+lastMonthEnd.strftime('201706')      
    getGridShapeSql += "           (objectid                 "
    getGridShapeSql += "           ,date_id                  "
    getGridShapeSql += "           ,city_name                "
    getGridShapeSql += "           ,lon_grid                 "
    getGridShapeSql += "           ,lat_grid                 "
    getGridShapeSql += "           ,city                     "
    getGridShapeSql += "           ,county                   "
    getGridShapeSql += "           ,grid_id                  "
    getGridShapeSql += "           ,grid                     "
    getGridShapeSql += "           ,poi                      "
    getGridShapeSql += "           ,poi_type                 "
    getGridShapeSql += "           ,dots_sc_weak             "
    getGridShapeSql += "           ,dots_sc                  "
    getGridShapeSql += "           ,dots_sc_weak_pct,dots_dx_nc_weak,dots_dx_nc,dots_dx_nc_weak_pct,dots_lt_nc_weak,dots_lt_nc,dots_lt_nc_weak_pct,ave_ltescrsrp,grid_class,date_str"
    getGridShapeSql += "           ,shape)                                                             "
    getGridShapeSql += "SELECT rownum AS OBJECTID,a.date_id  "
    getGridShapeSql += "      ,a.city_name                   "
    getGridShapeSql += "      ,a.lon_grid                    "
    getGridShapeSql += "      ,a.lat_grid                    "
    getGridShapeSql += "      ,a.city                        "
    getGridShapeSql += "      ,a.county                      "
    getGridShapeSql += "      ,a.mgrs                        "
    getGridShapeSql += "      ,a.grid                        "
    getGridShapeSql += "      ,a.poi                         "
    getGridShapeSql += "      ,a.poi_type                    "
    getGridShapeSql += "      ,a.dots_sc_weak                "
    getGridShapeSql += "      ,a.dots_sc                     "
    getGridShapeSql += "      ,a.dots_sc_weak_pct,a.dots_dx_nc_weak,a.dots_dx_nc,a.dots_dx_nc_weak_pct,a.dots_lt_nc_weak,a.dots_lt_nc,a.dots_lt_nc_weak_pct,a.ave_ltescrsrp,a.grid_class,a.date_str"
    getGridShapeSql += "      ,b.shape                                                              "
    getGridShapeSql += "FROM SDE.MRO_GRID_JD_STAT a ,SDE.GIS_MGRS_50 b      "
    getGridShapeSql+= " where a.date_id = '"+lastMonthEnd.strftime('201706')+"' and a.MGRS=b.MGRS"
    print getGridShapeSql
    cursorOra.execute(getGridShapeSql)
    dbConnOra.commit()
    getGridPntSql = "INSERT INTO SDE.GIS_MRO_GRID_JD_PNT"+lastMonthEnd.strftime('201706')      
    getGridPntSql += "           (objectid                 "
    getGridPntSql += "           ,date_id                  "
    getGridPntSql += "           ,city_name                "
    getGridPntSql += "           ,lon_grid                 "
    getGridPntSql += "           ,lat_grid                 "
    getGridPntSql += "           ,city                     "
    getGridPntSql += "           ,county                   "
    getGridPntSql += "           ,grid_id                  "
    getGridPntSql += "           ,grid                     "
    getGridPntSql += "           ,poi                      "
    getGridPntSql += "           ,poi_type                 "
    getGridPntSql += "           ,dots_sc_weak             "
    getGridPntSql += "           ,dots_sc                  "
    getGridPntSql += "           ,dots_sc_weak_pct,dots_dx_nc_weak,dots_dx_nc,dots_dx_nc_weak_pct,dots_lt_nc_weak,dots_lt_nc,dots_lt_nc_weak_pct,ave_ltescrsrp,grid_class,date_str"
    getGridPntSql += "           ,shape)                                                             "
    getGridPntSql += "SELECT OBJECTID,date_id  "
    getGridPntSql += "      ,city_name                   "
    getGridPntSql += "      ,lon_grid                    "
    getGridPntSql += "      ,lat_grid                    "
    getGridPntSql += "      ,city                        "
    getGridPntSql += "      ,county                      "
    getGridPntSql += "      ,grid_id                     "
    getGridPntSql += "      ,grid                        "
    getGridPntSql += "      ,poi                         "
    getGridPntSql += "      ,poi_type                    "
    getGridPntSql += "      ,dots_sc_weak                "
    getGridPntSql += "      ,dots_sc                     "
    getGridPntSql += "      ,dots_sc_weak_pct,dots_dx_nc_weak,dots_dx_nc,dots_dx_nc_weak_pct,dots_lt_nc_weak,dots_lt_nc,dots_lt_nc_weak_pct,ave_ltescrsrp,grid_class,date_str"
    getGridPntSql += "      ,st_centroid(shape)                                                              "
    getGridPntSql += "FROM SDE.GIS_MRO_GRID_JD_STAT"+lastMonthEnd.strftime('201706')
    getGridPntSql+= " where GRID_CLASS=1 or GRID_CLASS=4"
    print getGridPntSql
    cursorOra.execute(getGridPntSql)
    dbConnOra.commit()
    dbConnOra.close()


if __name__ == '__main__':
    try:
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        DataSourcePath = "PDB_PMSDB.sde"
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        #yestday = datetime.datetime.now() + datetime.timedelta(days=-10)
        print lastMonthEnd.strftime('201706')
        
        tmpJdGis = ArcCatalogPath+"\\"+DataSourcePath+"\\SDE.GIS_MRO_GRID_JD_STAT"
        tmpPntGis = ArcCatalogPath+"\\"+DataSourcePath+"\\SDE.GIS_MRO_GRID_JD_PNT"
        GisMroJdStat = ArcCatalogPath+"\\"+DataSourcePath+"\\SDE.GIS_MRO_GRID_JD_STAT"+lastMonthEnd.strftime('201706')
        GisMroJdPnt = ArcCatalogPath+"\\"+DataSourcePath+"\\SDE.GIS_MRO_GRID_JD_PNT"+lastMonthEnd.strftime('201706')

        if(arcpy.Exists(GisMroJdStat)):
            arcpy.Delete_management(GisMroJdStat)
        GisMroJdStat = arcpy.CreateFeatureclass_management(os.path.dirname(GisMroJdStat),os.path.basename(GisMroJdStat),"Polygon",tmpJdGis,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        arcpy.DeleteField_management (GisMroJdStat, "SHAPE_STArea__") 
        arcpy.DeleteField_management (GisMroJdStat, "SHAPE_STLength__")
        if(arcpy.Exists(GisMroJdPnt)):
            arcpy.Delete_management(GisMroJdPnt)
        GisMroJdPnt = arcpy.CreateFeatureclass_management(os.path.dirname(GisMroJdPnt),os.path.basename(GisMroJdPnt),"Point",tmpPntGis,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        excuteSql()
        
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0) 
