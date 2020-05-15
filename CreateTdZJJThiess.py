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
LOG_FILE_NAME = "E:\\GisPython\\logs\\CreateTDZJJThiess.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)

if __name__ == '__main__':
    try:
        # Local variables:
        logging.info("-----------------------start--------------------")
        print "Create TD  ZJJ Data"
        logging.info("Create TD ZJJ Data")
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        logging.info(time.strftime('%Y-%m-%d %H:%M:%S',startTime))
        yestday = datetime.datetime.now() + datetime.timedelta(days=-2)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        currentDoTimeStr = yestday.strftime('%Y-%m-%d 00:00:00')
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        DataSourcePath = "WangYouGis_Oracle97.sde"
        GISDBPath = "WangYouCellThiess60.sde"
        JGYHDbPath = "JGYH_SQL97.sde"
        InputTable = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.DBO.GIS_CELL_TSSPOLY_TD"+yestday.strftime('%Y%m%d')
        ZJJ_DETAIL = ArcCatalogPath+"\\"+JGYHDbPath+"\\JGYH.DBO.GIS_CELL_TSSPOLY_TD_D"+yestday.strftime('%Y%m%d')
        rows = arcpy.SearchCursor(InputTable)   
        row = rows.next()   
        if row:
            print "delete exists ZJJ_DETAIL"
            logging.info("delete exists ZJJ_DETAIL")
            if(arcpy.Exists(ZJJ_DETAIL)):
                arcpy.Delete_management(ZJJ_DETAIL)
            print "PolygonNeighbors_analysis begin"
            logging.info("PolygonNeighbors_analysis begin")
            # Process: 面邻域
            arcpy.PolygonNeighbors_analysis(InputTable, ZJJ_DETAIL, "OBJECTID;CELL_NAME;CI;LATITUDE;LONGITUDE;SITE_NAME;CITY_NAME", "NO_AREA_OVERLAP", "BOTH_SIDES", "", "METERS", "SQUARE_MILES")
            print "PolygonNeighbors_analysis success"
            logging.info("PolygonNeighbors_analysis success")
            # Process: 添加字段
            arcpy.AddField_management(ZJJ_DETAIL, "DISTANCE", "DOUBLE", "12", "2", "", "DISTANCE", "NULLABLE", "NON_REQUIRED", "")
            logging.info("AddField_management success")
            dbConn=pyodbc.connect('DRIVER={SQL Server};SERVER=10.48.186.12;DATABASE=JGYH;UID=sa;PWD=!Passw0rd@')
            cursor =dbConn.cursor()
            updateThiessZjjSql=" update GIS_CELL_TSSPOLY_TD_D"+yestday.strftime('%Y%m%d')+" set DISTANCE =(dbo.fnGetDistance(src_LATITUDE,src_LONGITUDE,nbr_LATITUDE,nbr_LONGITUDE))*1000"
            cursor.execute(updateThiessZjjSql)
            deleteZjjSql = "delete GIS_ZJJ_TD"
            cursor.execute(deleteZjjSql)
            createZjjThiessSql = " insert into GIS_ZJJ_TD select t.src_CITY_NAME as CITY_NAME,t.src_CI as CI ,avg(DISTANCE) as ZJJ  from GIS_CELL_TSSPOLY_TD_D"+yestday.strftime('%Y%m%d')+" t"
            createZjjThiessSql += " where t.DISTANCE>0 and t.src_SITE_NAME != t.nbr_SITE_NAME "
            createZjjThiessSql +="group by t.src_CI,t.src_CITY_NAME" 
            cursor.execute(createZjjThiessSql)
            dbConn.commit()
            dbConn.close()
            endTime = time.localtime(time.time())
            print "开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime)
            logging.info("开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime))
        else:
            logging.info("数据为空跳过执行!~~")
            endTime = time.localtime(time.time())
            print "开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime)
            logging.info("开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime))
        del rows,row
        logging.info("-----------------------end--------------------")
        os._exit(0)
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0)
