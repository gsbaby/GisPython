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
LOG_FILE_NAME = "E:\\GisPython\\logs\\CreateTdZJJFwj.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)

if __name__ == '__main__':
    try:
        # Local variables:
        logging.info("-----------------------start--------------------")
        print "Create TD ZJJ FWJ Data"
        logging.info("Create TD ZJJ FWJ Data")
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        yestday = datetime.datetime.now() + datetime.timedelta(days=-2)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        logging.info(yestday.strftime('%Y-%m-%d 00:00:00'))
        currentDoTimeStr = yestday.strftime('%Y-%m-%d 00:00:00')
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        DataSourcePath = "WangYouGis_Oracle97.sde"
        GISDBPath = "WangYouCellThiess60.sde"
        JGYHDbPath = "JGYH_SQL97.sde"
        InputData = ArcCatalogPath+"\\"+JGYHDbPath+"\\JGYH.DBO.GIS_OBJECT_TDPNT_ZJJ_FWJ"
        ZJJ_FWJ_DETAIL = ArcCatalogPath+"\\"+JGYHDbPath+"\\JGYH.DBO.GIS_TD_ZJJ_FWJ_DETAIL"
        print "delete exists ZJJ_FWJ_DETAIL"
        logging.info("delete exists ZJJ_FWJ_DETAIL")
        if(arcpy.Exists(ZJJ_FWJ_DETAIL)):
            arcpy.Delete_management(ZJJ_FWJ_DETAIL)
            
        print "GenerateNearTable_analysis begin"
        logging.info("GenerateNearTable_analysis begin")
        # Process: 生成近邻表
        arcpy.GenerateNearTable_analysis(InputData,InputData, ZJJ_FWJ_DETAIL, "3000 Meters", "LOCATION", "ANGLE", "ALL", "50")

        print "GenerateNearTable_analysis end"
        logging.info("GenerateNearTable_analysis end")
        logging.info("AddField_management")
        # Process: 添加字段FLAG
        arcpy.AddField_management(ZJJ_FWJ_DETAIL, "FLAG", "SHORT", "2", "", "", "FLAG", "NULLABLE", "NON_REQUIRED", "")

        # Process: 添加字段 B_ANGEL
        arcpy.AddField_management(ZJJ_FWJ_DETAIL, "B_ANGEL", "DOUBLE", "8", "2", "", "B_ANGEL", "NULLABLE", "NON_REQUIRED", "")

        # Process: 添加字段 DISTANCE
        arcpy.AddField_management(ZJJ_FWJ_DETAIL, "DISTANCE", "DOUBLE", "12", "2", "", "DISTANCE", "NULLABLE", "NON_REQUIRED", "")

        # Process: 添加字段 NEAR_CI
        arcpy.AddField_management(ZJJ_FWJ_DETAIL, "NEAR_CI", "TEXT", "", "", "100", "NEAR_CI", "NULLABLE", "NON_REQUIRED", "")
		
	# Process: 添加字段 NEAR_CITY_NAME
        arcpy.AddField_management(ZJJ_FWJ_DETAIL, "NEAR_CITY_NAME", "TEXT", "", "", "100", "NEAR_CITY_NAME", "NULLABLE", "NON_REQUIRED", "")

        # Process: 添加字段 NEAR_SITE_NAME
        arcpy.AddField_management(ZJJ_FWJ_DETAIL, "NEAR_SITE_NAME", "TEXT", "", "", "300", "NEAR_SITE_NAME", "NULLABLE", "NON_REQUIRED", "")

        logging.info("ZJJ DATA UPDATE BEGIN...")
        dbConn=pyodbc.connect('DRIVER={SQL Server};SERVER=10.48.186.12;DATABASE=JGYH;UID=sa;PWD=!Passw0rd@')
        cursor =dbConn.cursor()
        print "update b_angel"
        logging.info("update b_angel")
        updateBAngel1 = "  update GIS_TD_ZJJ_FWJ_DETAIL set B_ANGEL = 450-NEAR_ANGLE   where [NEAR_ANGLE]>90 and [NEAR_ANGLE]<=180;"
        cursor.execute(updateBAngel1)
        updateBAngel2 = "  update GIS_TD_ZJJ_FWJ_DETAIL set B_ANGEL = 90-NEAR_ANGLE   where [NEAR_ANGLE]>=-180 and [NEAR_ANGLE]<=90;"
        cursor.execute(updateBAngel2)
        print "update distance"
        logging.info("update distance")
        updateDistanceSql = "update a set a.DISTANCE =dbo.fnGetDistance(a.[NEAR_Y],a.[NEAR_X],b.[LATITUDE],b.[LONGITUDE])*1000  "
        updateDistanceSql +="from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateDistanceSql +=" where a.[IN_FID] = b .OBJECTID"
        cursor.execute(updateDistanceSql)
        print "update near info"
        logging.info("update near info")
        updateNearInfoSql = "update a set a.NEAR_CI =b.CI,a.NEAR_CITY_NAME = b.CITY_NAME,a.NEAR_SITE_NAME = b.SITE_NAME "
        updateNearInfoSql +=" from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b where a.NEAR_FID  = b.OBJECTID"
        cursor.execute(updateNearInfoSql)
        dbConn.commit()
        print "update flag1"
        logging.info("update flag1")
        updateFlag1Sql = "update a set a.FLAG=1 "
        updateFlag1Sql += "from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateFlag1Sql += "where a.[IN_FID] = b.OBJECTID and  b.ANT_DIRCT_ANGLE>=60 and  b.ANT_DIRCT_ANGLE<=300 and a.B_ANGEL>=b.ANT_DIRCT_ANGLE-60 "
        updateFlag1Sql += "and a.B_ANGEL<=b.ANT_DIRCT_ANGLE+60   and a.DISTANCE>10 and a.DISTANCE<=1000; "
        cursor.execute(updateFlag1Sql)
        updateFlag12Sql = "update a set a.FLAG=1 "
        updateFlag12Sql +=" from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateFlag12Sql +="where a.[IN_FID] = b.OBJECTID and  b.ANT_DIRCT_ANGLE>=0 and b.ANT_DIRCT_ANGLE<60 "
        updateFlag12Sql +="and ((a.B_ANGEL>=300+b.ANT_DIRCT_ANGLE and  a.B_ANGEL<360) "
        updateFlag12Sql +="or (a.B_ANGEL>0 and a.B_ANGEL<=b.ANT_DIRCT_ANGLE+60)) and a.DISTANCE>10 and a.DISTANCE<=1000;"
        cursor.execute(updateFlag12Sql)
        updateFlag13Sql = "update a set a.FLAG=1 "
        updateFlag13Sql += "from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateFlag13Sql += "where a.[IN_FID] = b.OBJECTID and  b.ANT_DIRCT_ANGLE>300    and b.ANT_DIRCT_ANGLE<=360 "
        updateFlag13Sql += "and ((a.B_ANGEL>=b.ANT_DIRCT_ANGLE-60 and  a.B_ANGEL<360) "
        updateFlag13Sql += "or (a.B_ANGEL>0 and a.B_ANGEL<=b.ANT_DIRCT_ANGLE-300)) and a.DISTANCE>10 and a.DISTANCE<=1000;"
        cursor.execute(updateFlag13Sql)
        print "update flag2"
        logging.info("update flag2")
        updateFlag2Sql = "update a set a.FLAG=2 "
        updateFlag2Sql += "from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateFlag2Sql += "where a.[IN_FID] = b.OBJECTID and  b.ANT_DIRCT_ANGLE>=60 and  b.ANT_DIRCT_ANGLE<=300 and a.B_ANGEL>=b.ANT_DIRCT_ANGLE-60 "
        updateFlag2Sql += "and a.B_ANGEL<=b.ANT_DIRCT_ANGLE+60   and a.DISTANCE>1000 and a.DISTANCE<=2000; "
        cursor.execute(updateFlag2Sql)
        updateFlag22Sql = "update a set a.FLAG=2 "
        updateFlag22Sql +=" from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateFlag22Sql +="where a.[IN_FID] = b.OBJECTID and  b.ANT_DIRCT_ANGLE>=0 and b.ANT_DIRCT_ANGLE<60 "
        updateFlag22Sql +="and ((a.B_ANGEL>=300+b.ANT_DIRCT_ANGLE and  a.B_ANGEL<360) "
        updateFlag22Sql +="or (a.B_ANGEL>0 and a.B_ANGEL<=b.ANT_DIRCT_ANGLE+60)) and a.DISTANCE>1000 and a.DISTANCE<=2000;"
        cursor.execute(updateFlag22Sql)
        updateFlag23Sql = "update a set a.FLAG=2 "
        updateFlag23Sql += "from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateFlag23Sql += "where a.[IN_FID] = b.OBJECTID and  b.ANT_DIRCT_ANGLE>300    and b.ANT_DIRCT_ANGLE<=360 "
        updateFlag23Sql += "and ((a.B_ANGEL>=b.ANT_DIRCT_ANGLE-60 and  a.B_ANGEL<360) "
        updateFlag23Sql += "or (a.B_ANGEL>0 and a.B_ANGEL<=b.ANT_DIRCT_ANGLE-300)) and a.DISTANCE>1000 and a.DISTANCE<=2000;"
        cursor.execute(updateFlag23Sql)
        print "update flag3"
        logging.info("update flag3")
        updateFlag3Sql = "update a set a.FLAG=3 "
        updateFlag3Sql += "from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateFlag3Sql += "where a.[IN_FID] = b.OBJECTID and  b.ANT_DIRCT_ANGLE>=60 and  b.ANT_DIRCT_ANGLE<=300 and a.B_ANGEL>=b.ANT_DIRCT_ANGLE-60 "
        updateFlag3Sql += "and a.B_ANGEL<=b.ANT_DIRCT_ANGLE+60   and a.DISTANCE>2000 ; "
        cursor.execute(updateFlag3Sql)
        updateFlag32Sql = "update a set a.FLAG=3 "
        updateFlag32Sql +=" from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateFlag32Sql +="where a.[IN_FID] = b.OBJECTID and  b.ANT_DIRCT_ANGLE>=0 and b.ANT_DIRCT_ANGLE<60 "
        updateFlag32Sql +="and ((a.B_ANGEL>=300+b.ANT_DIRCT_ANGLE and  a.B_ANGEL<360) "
        updateFlag32Sql +="or (a.B_ANGEL>0 and a.B_ANGEL<=b.ANT_DIRCT_ANGLE+60)) and a.DISTANCE>2000 ;"
        cursor.execute(updateFlag32Sql)
        updateFlag33Sql = "update a set a.FLAG=3 "
        updateFlag33Sql += "from GIS_TD_ZJJ_FWJ_DETAIL a ,GIS_OBJECT_TDPNT_ZJJ_FWJ b "
        updateFlag33Sql += "where a.[IN_FID] = b.OBJECTID and  b.ANT_DIRCT_ANGLE>300    and b.ANT_DIRCT_ANGLE<=360 "
        updateFlag33Sql += "and ((a.B_ANGEL>=b.ANT_DIRCT_ANGLE-60 and  a.B_ANGEL<360) "
        updateFlag33Sql += "or (a.B_ANGEL>0 and a.B_ANGEL<=b.ANT_DIRCT_ANGLE-300)) and a.DISTANCE>2000;"
        cursor.execute(updateFlag33Sql)
        dbConn.commit()
        print "delete data o3"
        logging.info("delete data o3")
        deleteO3Sql = "delete GIS_OBJECT_TDZJJ_FWJ_O3"
        cursor.execute(deleteO3Sql)
        print "insert o3"
        logging.info("insert o3")
        insertO3Sql = "insert into GIS_OBJECT_TDZJJ_FWJ_O3   select IN_FID,NEAR_SITE_NAME,DISTANCE,ORDER_ID,FLAG from ( "
        insertO3Sql += "select a.IN_FID,a.NEAR_SITE_NAME,a.DISTANCE,a.FLAG,row_number() over (partition by IN_FID order by DISTANCE)  order_id "
        insertO3Sql += "from GIS_TD_ZJJ_FWJ_DETAIL a where a.FLAG is not null group by IN_FID,NEAR_SITE_NAME,DISTANCE,FLAG ) t WHERE ORDER_ID <4 "
        cursor.execute(insertO3Sql)
        dbConn.commit()
        print "delete data final detail"
        logging.info("delete data final detail")
        deleteFinalDetail ="delete GIS_OBJECT_TDZJJ_FINA_DETAIL"
        cursor.execute(deleteFinalDetail)
        print "insert final detail"
        logging.info("insert final detail")
        insertFinalDetail = "insert into GIS_OBJECT_TDZJJ_FINA_DETAIL select a.IN_FID,a.NEAR_CI,a.NEAR_CITY_NAME,a.NEAR_SITE_NAME,a.DISTANCE,b.ORDER_ID,b.FLAG from GIS_TD_ZJJ_FWJ_DETAIL a,GIS_OBJECT_TDZJJ_FWJ_O3 b "
        insertFinalDetail +="where a.IN_FID=b.IN_FID and a.NEAR_SITE_NAME = b.NEAR_SITE_NAME "
        cursor.execute(insertFinalDetail)
        dbConn.commit()
        logging.info("delete GIS_OBJECT_TDZJJ_FWJ")
        print "delete GIS_OBJECT_TDZJJ_FWJ"
        deleteFwjZjjSql = "delete GIS_OBJECT_TDZJJ_FWJ"
        cursor.execute(deleteFwjZjjSql)
        print "insert fwjzjj "
        logging.info("insert fwjzjj")
        insertFwjZjj1 = "insert into  GIS_OBJECT_TDZJJ_FWJ  select a.IN_FID,avg(DISTANCE) DISTANCE,'','',1 from [GIS_OBJECT_TDZJJ_FINA_DETAIL] a "
        insertFwjZjj1 += "where a.FLAG=1  group by a.IN_FID "
        cursor.execute(insertFwjZjj1)
        insertFwjZjj2 = "insert into  GIS_OBJECT_TDZJJ_FWJ  select a.IN_FID,avg(DISTANCE) DISTANCE,'','',2 from [GIS_OBJECT_TDZJJ_FINA_DETAIL] a "
        insertFwjZjj2 += "where a.FLAG=2 and a.IN_FID not in (select IN_FID from  GIS_OBJECT_TDZJJ_FWJ)  group by a.IN_FID "
        cursor.execute(insertFwjZjj2)
        insertFwjZjj3 = "insert into  GIS_OBJECT_TDZJJ_FWJ  select a.IN_FID,avg(DISTANCE) DISTANCE,'','',3 from [GIS_OBJECT_TDZJJ_FINA_DETAIL] a "
        insertFwjZjj3 += "where a.FLAG=3 and a.IN_FID not in (select IN_FID from  GIS_OBJECT_TDZJJ_FWJ)  group by a.IN_FID "
        cursor.execute(insertFwjZjj3)
        dbConn.commit()
        logging.info("update fwjzjj ci")
        print "update fwjzjj ci"
        updateFwjZjjSql = "update a set a.CI=b.CI,a.CITY_NAME=b.CITY_NAME from GIS_OBJECT_TDZJJ_FWJ a,GIS_OBJECT_TDPNT_ZJJ_FWJ b     where a.IN_FID = b.OBJECTID "
        cursor.execute(updateFwjZjjSql)
        dbConn.commit()
        dbConn.close()
        logging.info("-----------------------end--------------------")
        os._exit(0)
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0)
