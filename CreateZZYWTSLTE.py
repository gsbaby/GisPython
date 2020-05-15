# -*- coding: utf-8 -*-

import arcpy
import time,datetime
import os
import random
import copy
import threading
import httplib, urllib, json
import logging


spatialReference = arcpy.SpatialReference(4326)
shapefieldname = "SHAPE"
cellname = "CELL_NAME"
totalincome = "TOTAL_INCOME"
ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
GisSourcePath = "WangYouZZYW15.sde"
DataSourcePath = "WangYouGis_Oracle97.sde"
startTime = time.localtime(time.time())
yestday = datetime.datetime.now() + datetime.timedelta(days=-2)
curDay = startTime.tm_mday
lastMonthEnd = datetime.datetime.now()-datetime.timedelta(days=curDay)
lastMonthStart = datetime.date(lastMonthEnd.year,lastMonthEnd.month,1)
LOG_FILE_NAME = "E:\\GisPython\\ZZYW\\logs\\zzywTSLTE"+time.strftime('%Y%m%d',startTime)+".log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)



def createThiessCellByTable(resultFeature,datasArray,cellShapes,dataFields):
    resultThiessCellCurs = arcpy.InsertCursor(resultFeature)
    dataLen = len(datasArray)
    for cellThiess in cellShapes:
        resultThiessRow = resultThiessCellCurs.newRow()
        resultThiessRow.shape = cellThiess["shape"]
        resultThiessRow.CELL_NAME = cellThiess["CELL_NAME"]
        flag = False
        for i in range(dataLen):
            CurrentDataRow = datasArray[i]
            if(CurrentDataRow["CI"]==cellThiess["CI"]):
                print "CI:::"+str(i)+"#######"+CurrentDataRow["CI"]+"==:::::=="+cellThiess["CI"]
                print "REGION_NAME:::"+str(i)+"#######"+CurrentDataRow["REGION_NAME"]+"==:::::=="+cellThiess["CITY_NAME"]
                flag = True
                for dataField in dataFields:
                    dataFieldName = dataField.name
                    if(dataFieldName!="TDL_ZDYWFB_1001" and dataFieldName!="OBJECTID" and dataFieldName!=shapefieldname  and dataFieldName!=cellname and dataFieldName!=totalincome and dataFieldName!="SHAPE.STArea()" and dataFieldName!="SHAPE.STLength()"):
                        resultThiessRow.setValue(dataFieldName,CurrentDataRow[dataFieldName])
                resultThiessRow.TOTAL_INCOME = CurrentDataRow["TDL_ZDYWFB_1029"]+CurrentDataRow["TDL_ZDYWFB_1030"]
                resultThiessRow.TDL_ZDYWFB_1001 = int(CurrentDataRow["TDL_ZDYWFB_1001"])
                resultThiessCellCurs.insertRow(resultThiessRow)      
                break
        if(flag==True):
            continue
        else:
            for dataField in dataFields:
                dataFieldName = dataField.name
                if(dataField.type=="Double"  and dataFieldName!="SHAPE.STArea()" and dataFieldName!="SHAPE.STLength()"):
                    resultThiessRow.setValue(dataFieldName,0)
                    continue
                if(dataFieldName.upper()=="REGION_NAME"):
                    resultThiessRow.setValue(dataFieldName,cellThiess["CITY_NAME"])
                    continue
                if(dataFieldName.upper()=="CI"):
                    resultThiessRow.setValue(dataFieldName,cellThiess["CI"])
                    continue
            resultThiessCellCurs.insertRow(resultThiessRow)



def dataTable2Array(dataTable,dataFields,filterSQL):
    datasArray = []
    CurrentDataCursors = arcpy.SearchCursor(dataTable,filterSQL)
    CurrentDataRow=CurrentDataCursors.next()
    while CurrentDataRow:
        dataRowObj = {}
        for dataField in dataFields:
            dataFieldName = dataField.name
            if(dataFieldName!="OBJECTID" and dataFieldName!=shapefieldname and dataFieldName!=totalincome and dataFieldName!="SHAPE.STArea()" and dataFieldName!="SHAPE.STLength()" and dataFieldName!=cellname):
                dataRowObj[dataFieldName]=CurrentDataRow.getValue(dataFieldName)
        datasArray.append(dataRowObj)
        CurrentDataRow=CurrentDataCursors.next()
    return datasArray




def createGisTable(resultFeature,tableTemp,dataTable,cellThiesses,filterSQL):
    #获取所需所有字段名
    dataFields = arcpy.ListFields(tableTemp)
    #按条件查询出所需要的数据放入数组
    datasArray = dataTable2Array(dataTable,dataFields,filterSQL)
    print "data length:",len(datasArray)
    if(len(datasArray)>0):
        print "delete exists resultFeature"
        if(arcpy.Exists(resultFeature)):
            arcpy.Delete_management(resultFeature, "FeatureClass")
        resultFeature = arcpy.CreateFeatureclass_management(os.path.dirname(resultFeature),os.path.basename(resultFeature),"Polygon",tableTemp,"DISABLED","DISABLED",spatialReference)
        createThiessCellByTable(resultFeature,datasArray,cellThiesses,dataFields)




def createGisTableZZYWLTE(cellThiesses):
    filterSQL = "TIME = TO_DATE('"+lastMonthStart.strftime('%Y-%m-%d 00:00:00')+"','YYYY-MM-DD HH24:MI:SS')"
    print filterSQL
    ZZYWLTETemp = ArcCatalogPath+"\\"+GisSourcePath+"\\WangYouZZYW.DBO.GIS_THIESS_ZZYW_LTE_TEMP"
    currentDataTableZZYWLTE= ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.GIS_JF_LTE"
    resultZZYWLTE = ArcCatalogPath+"\\"+GisSourcePath+"\\WangYouZZYW.DBO.GIS_THIESS_ZZYW_LTE"+lastMonthEnd.strftime('%Y%m')
    if(arcpy.Exists(currentDataTableZZYWLTE)):
        logging.info("do create Gis table")
        createGisTable(resultZZYWLTE,ZZYWLTETemp,currentDataTableZZYWLTE,cellThiesses,filterSQL)
    else:
        logging.info("no current data")





# Script start
if __name__ == "__main__":
    try:
        print "create ZZYW  TSLTE"
        logging.info(time.strftime('%Y-%m-%d %H:%M:%S',startTime))
        logging.info("create ZZYM TSLTE table")
        CellThiessSource = "WangYouCellThiess60.sde"
        cellThiessFeatures = ArcCatalogPath+"\\"+CellThiessSource+"\\WangYouCellThiess.DBO.GIS_CELL_TSSPOLY_LTE"+yestday.strftime('%Y%m%d')
        print cellThiessFeatures
        cellThiessCurs = arcpy.SearchCursor(cellThiessFeatures)
        cellShapes = []
        for cellThiess in cellThiessCurs:
            cellShpe = {}
            cellShpe["CI"] = cellThiess.CI
            cellShpe["CELL_NAME"] = cellThiess.CELL_NAME
            cellShpe["CITY_NAME"] = cellThiess.CITY_NAME
            cellShpe["shape"]= cellThiess.shape
            cellShapes.append(cellShpe)
        print "======================================"
        print "||    create ZZYW TSLTE         ||"
        print "======================================"    
        createGisTableZZYWLTE(cellShapes)
    except Exception,e:
        print e
        logging.error(e)
