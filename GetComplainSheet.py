# -*- coding: utf-8 -*-
import arcpy
import time,datetime
import os
import cx_Oracle
import logging

spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\logs\\GetComplainSheet.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)

if __name__ == '__main__':
    startTime = time.localtime(time.time()) 
    print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
    yestday = datetime.datetime.now() + datetime.timedelta(days=-1)
    logging.info(yestday.strftime('%Y-%m-%d 00:00:00'))
    print yestday.strftime('%Y-%m-%d 00:00:00')
    ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
    DataSourcePath = "WangYouGis_Oracle97.sde"
    GISDBPath = "WangYouTouSu60.sde"
    infc = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.GIS_TOUSU_ALL"
    ComplainSheetPoint = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouTouSu.DBO.GIS_TOUSU"
    complainSheetCursor=arcpy.SearchCursor(infc,"TIME>=TO_DATE('"+yestday.strftime('%Y-%m-%d 00:00:00')+"','YYYY-MM-DD HH24:MI:SS')")
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
                
                    
