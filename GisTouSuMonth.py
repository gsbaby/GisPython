# -*- coding: utf-8 -*-
import arcpy
import time,datetime
import os
import cx_Oracle
import logging

spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\logs\\GisTouSuMonth.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)
spatialReference = arcpy.SpatialReference(4326)


def createGisTable(resultFeature,tableTemp):
    #获取所需所有字段名
    dataFields = arcpy.ListFields(tableTemp)
    
    print "delete exists resultFeature"
    if(arcpy.Exists(resultFeature)):
        arcpy.Delete_management(resultFeature, "FeatureClass")
    resultFeature = arcpy.CreateFeatureclass_management(os.path.dirname(resultFeature),os.path.basename(resultFeature),"Point",tableTemp,"DISABLED","DISABLED",spatialReference)


if __name__ == '__main__':
    startTime = time.localtime(time.time()) 
    print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
    ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
    DataSourcePath = "WangYouGis_Oracle97.sde"
    GISDBPath = "WangYouTouSu60.sde"
    infc = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.GIS_TOUSU_ALL"
    GisTouSuWeekTemp = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouTouSu.DBO.GIS_TOUSU"
    ComplainSheetPoint = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouTouSu.DBO.GIS_TOUSU_MONTH"
    
    createGisTable(ComplainSheetPoint,GisTouSuWeekTemp)

    curDay = startTime.tm_mday
    lastMonthEnd = datetime.datetime.now()-datetime.timedelta(days=curDay)
    lastMonthStart = datetime.date(lastMonthEnd.year,lastMonthEnd.month,1)
    print lastMonthStart.strftime('%Y-%m-%d 00:00:00')
    print lastMonthEnd.strftime('%Y-%m-%d 00:00:00')
    complainSheetCursor=arcpy.SearchCursor(infc,"TIME>=TO_DATE('"+lastMonthStart.strftime('%Y-%m-%d 00:00:00')+"','YYYY-MM-DD HH24:MI:SS') and TIME<=TO_DATE('"+lastMonthEnd.strftime('%Y-%m-%d 23:59:59')+"','YYYY-MM-DD HH24:MI:SS')")
    CompalinFinalInsertCur=arcpy.InsertCursor(ComplainSheetPoint)
    ComplainFields=arcpy.ListFields(infc)
    nextComplainRow=complainSheetCursor.next()
    while nextComplainRow:
        p_lon = nextComplainRow.getValue("LON")
        p_lat = nextComplainRow.getValue("LAT")
        print p_lon,p_lat
        if(p_lon is not None and p_lat is not None and p_lon>110 and p_lon<130 and p_lat>20 and p_lat<30):
            complainPnt = arcpy.Point(p_lon, p_lat)
            resultRow=CompalinFinalInsertCur.newRow()
            resultRow.shape=complainPnt
            for ComplainField in ComplainFields:
                XQFieldName = ComplainField.name
                if(XQFieldName!="OBJECTID" and XQFieldName!="ORDER_ID"):
                    resultRow.setValue(XQFieldName,nextComplainRow.getValue(XQFieldName))
            CompalinFinalInsertCur.insertRow(resultRow)
        nextComplainRow=complainSheetCursor.next()
    logging.info("................end.............")
    del complainSheetCursor,CompalinFinalInsertCur,nextComplainRow,resultRow
                
                    
