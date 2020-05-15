# -*- coding: utf-8 -*-
import arcpy
import time,datetime
import os
import cx_Oracle
import logging

spatialReference = arcpy.SpatialReference(4326)
ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
DataSourcePath = "WangYouGis_Oracle97.sde"
GISDBPath = "WangYouGis15.sde"
startTime = time.localtime(time.time())
yestday = datetime.datetime.now() + datetime.timedelta(days=-3)
LOG_FILE_NAME = "E:\\GisPython\\logs\\RLYJDay.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)


def createRLYJCellByTable(resultFeature,datasArray,dataFields):
    resultRowCellCurs = arcpy.InsertCursor(resultFeature)
    dataLen = len(datasArray)
    for i in range(dataLen):
        resultRow = resultRowCellCurs.newRow()
        CurrentDataRow = datasArray[i]
        for dataField in dataFields:
            dataFieldName = dataField.name
            if(dataFieldName!="OBJECTID" and dataFieldName!="SHAPE" and dataFieldName!="PROB_TYPE"):
                resultRow.setValue(dataFieldName,CurrentDataRow[dataFieldName])
        if(CurrentDataRow["HIGHUSED"]>50 or CurrentDataRow["HIGHUSED1"]>50):
            resultRow.PROB_TYPE="1"
        elif(CurrentDataRow["HIGHRRC"]>50):
            resultRow.PROB_TYPE="2"
        elif(CurrentDataRow["HIGHFLOW"]>10):
            resultRow.PROB_TYPE="3"
        p_lon = CurrentDataRow["LONGITUDE"]
        p_lat = CurrentDataRow["LATITUDE"]
        if(p_lon is not None and p_lat is not None and p_lon>110 and p_lon<130 and p_lat>20 and p_lat<30):
            complainPnt = arcpy.Point(p_lon, p_lat)
            resultRow.shape=complainPnt
            resultRowCellCurs.insertRow(resultRow)

            

def dataTable2Array(dataTable,dataFields,filterSQL):
    datasArray = []
    CurrentDataCursors = arcpy.SearchCursor(dataTable,filterSQL)
    CurrentDataRow=CurrentDataCursors.next()
    while CurrentDataRow:
        dataRowObj = {}
        for dataField in dataFields:
            dataFieldName = dataField.name
            if(dataFieldName!="OBJECTID" and dataFieldName!="SHAPE" and dataFieldName!="PROB_TYPE"):
                dataRowObj[dataFieldName]=CurrentDataRow.getValue(dataFieldName)
        datasArray.append(dataRowObj)
        CurrentDataRow=CurrentDataCursors.next()
    return datasArray


def createGisTable(resultFeature,tempTable,dataTable,filterSQL):
    #获取所需所有字段名
    dataFields = arcpy.ListFields(tempTable)
    #按条件查询出所需要的数据放入数组
    datasArray = dataTable2Array(dataTable,dataFields,filterSQL)
    print "data length:",len(datasArray)
    if(len(datasArray)>0):
        if(arcpy.Exists(resultFeature)):
            print "delete exists resultFeature"
            arcpy.Delete_management(resultFeature, "FeatureClass")
        resultFeature = arcpy.CreateFeatureclass_management(os.path.dirname(resultFeature),os.path.basename(resultFeature),"Point",tempTable,"DISABLED","DISABLED",spatialReference)
        createRLYJCellByTable(resultFeature,datasArray,dataFields)


def createGisTableRLYJ():
    filterSQL = "TIME='"+yestday.strftime('%Y-%m-%d')+"' and TIMETYPE='D' and (HIGHFLOW >10 or HIGHRRC >50 or HIGHUSED > 50 or HIGHUSED1 > 50)"
    print filterSQL
    RLYJTemp = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouGis.DBO.TDL_PM_KR_CELL_TEMP"
    currentDataTable= ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.TDL_PM_KR_CELL"
    resultGisTable = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouGis.DBO.TDL_PM_KR_CELL_DAY"+yestday.strftime('%Y%m%d')
    if(arcpy.Exists(RLYJTemp)):
        logging.info("do create Gis table")
        createGisTable(resultGisTable,RLYJTemp,currentDataTable,filterSQL)
    else:
        logging.info("no current data")
        


if __name__ == '__main__':
    logging.info(time.strftime('%Y-%m-%d %H:%M:%S',startTime))
    logging.info(yestday.strftime('%Y-%m-%d 00:00:00'))
    createGisTableRLYJ();
