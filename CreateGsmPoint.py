#coding=utf-8
import arcpy
from arcpy import env
import math
import string
import time,datetime
import logging
import os
import logging
import pyodbc


shapefieldname = "SHAPE"
FidFieldName = "OBJECTID"
spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\logs\\CreateGSMPnt.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)

def createFeatureFromXY(featureTable,lonField,latField,featurePath,XiaoQuFields):
    resultFeatcur = arcpy.InsertCursor(featurePath)
    featRows = arcpy.SearchCursor(featureTable)
    for CellRow in featRows:
        p_lon = CellRow.getValue(lonField)
        p_lat = CellRow.getValue(latField)
        resultRow = resultFeatcur.newRow()
        featurePoint = arcpy.Point(p_lon, p_lat)
        resultRow.shape = featurePoint
        for XiaoQuField in XiaoQuFields:
            XQFieldName = XiaoQuField.name
            if(XQFieldName!="OBJECTID" and XQFieldName!=shapefieldname):
                resultRow.setValue(XQFieldName,CellRow.getValue(XQFieldName))
        resultFeatcur.insertRow(resultRow)
    del resultFeatcur,featRows


if __name__ == '__main__':
    try:
        print "Create GSM POINT"
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        yestday = datetime.datetime.now() + datetime.timedelta(days=-1)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        currentDoTimeStr = yestday.strftime('%Y-%m-%d 00:00:00')
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        DataSourcePath = "WangYouGis_Oracle97.sde"
        GISDBPath = "WangYouCellThiess60.sde"
        JGYHDbPath = "JGYH_SQL97.sde"
        
        infc = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.GIS_OBJECT_CELL_GSM"
        PointTemplate = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.DBO.GIS_OBJECT_GSMPNT_TEMP"
        PointInCache = ArcCatalogPath+"\\"+JGYHDbPath+"\\JGYH.DBO.GIS_OBJECT_GSMPNT_ZJJ_FWJ"

        FuJianSheng = "D:\Map\GaoDe\FuJian20140312.gdb\福建省面"
        
        print "desc"
        shapefieldname = "SHAPE"
        print "delete exists PointInCache"
        logging.info("delete exists PointInCache")
        if(arcpy.Exists(PointInCache)):
            arcpy.Delete_management(PointInCache, "FeatureClass")
        XiaoQuFields = arcpy.ListFields(infc)
        PointInCache = arcpy.CreateFeatureclass_management(os.path.dirname(PointInCache),os.path.basename(PointInCache),"Point",PointTemplate,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        print "筛选室外站生成要素"
        ShiWaiCell_GSM_ALL = arcpy.TableSelect_analysis(infc, "in_memory/ShiWaiCell_GSM_ALL", "HONEYCOMB_TYPE='室外' and TIME_STAMP = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
        createFeatureFromXY(ShiWaiCell_GSM_ALL,"LONGITUDE","LATITUDE",PointInCache,XiaoQuFields)
       
        endTime = time.localtime(time.time())
        print "开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime)
        logging.info("开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime))
        os._exit(0)
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0)
