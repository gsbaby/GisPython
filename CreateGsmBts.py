# -*- coding: utf-8 -*-
import arcpy
import time,datetime
import os
import random
import copy
import logging

ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
GISDBPath = "WangYouGis_Site15.sde"
startTime = time.localtime(time.time())
yestday = datetime.datetime.now() + datetime.timedelta(days=-2)
currentDoTimeStr = yestday.strftime('%Y-%m-%d 00:00:00')
shapefieldname = "SHAPE"
FidFieldName = "OBJECTID"
spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\logs\\createSite.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)

def createFeatureFromXY(featureTable,lonField,latField,featurePath,dataFields):
    resultFeatcur = arcpy.InsertCursor(featurePath)
    featRows = arcpy.SearchCursor(featureTable)
    for CellRow in featRows:
        p_lon = CellRow.getValue(lonField)
        p_lat = CellRow.getValue(latField)
        #print p_lon,p_lat
        if(p_lon is not None and p_lat is not None and p_lon>90 and p_lon<140 and p_lat>20 and p_lat<40):
            resultRow = resultFeatcur.newRow()
            featurePoint = arcpy.Point(p_lon, p_lat)
            resultRow.shape = featurePoint
            for dataField in dataFields:
                dataFieldName = dataField.name
                if(dataFieldName!="OBJECTID" and dataFieldName!=shapefieldname):
                    resultRow.setValue(dataFieldName,CellRow.getValue(dataFieldName))
            resultFeatcur.insertRow(resultRow)
    del resultFeatcur,featRows

def createGsmBts():
    sourceGsmBtsTable = ArcCatalogPath+"\\WangYouGis_Oracle97.sde\\WANGYOU.GIS_GSM_CM_BTS"
    targetGsmTemp = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouSite.DBO.GIS_GSM_BTS_TEMP"
    targetGsmBtsFeature = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouSite.DBO.GIS_GSM_BTS"+yestday.strftime('%Y%m%d')
    print "delete exists targetGsmBtsFeature"
    if(arcpy.Exists(targetGsmBtsFeature)):
        arcpy.Delete_management(targetGsmBtsFeature, "FeatureClass")
    targetGsmBtsFeature = arcpy.CreateFeatureclass_management(os.path.dirname(targetGsmBtsFeature),os.path.basename(targetGsmBtsFeature),"Point",targetGsmTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
    print "筛选当天的GSM基站生成要素"
    logging.info("筛选当天的GSM基站生成要素")
    gsmBtsFields = arcpy.ListFields(sourceGsmBtsTable)
    GSM_BTS_ALL = arcpy.TableSelect_analysis(sourceGsmBtsTable, "in_memory/GSM_BTS_ALL", "TIME = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
    createFeatureFromXY(GSM_BTS_ALL,"LONGITUDE","LATITUDE",targetGsmBtsFeature,gsmBtsFields)

def createTdsNodeb():
    sourceTdsNodebTable = ArcCatalogPath+"\\WangYouGis_Oracle97.sde\\WANGYOU.GIS_TDS_CM_NODEB"
    targetTdsTemp = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouSite.DBO.GIS_TD_NODEB_TEMP"
    targetTdsNodebFeature = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouSite.DBO.GIS_TD_NODEB"+yestday.strftime('%Y%m%d')
    print "delete exists targetTdsNodebFeature"
    if(arcpy.Exists(targetTdsNodebFeature)):
        arcpy.Delete_management(targetTdsNodebFeature, "FeatureClass")
    targetTdsNodebFeature = arcpy.CreateFeatureclass_management(os.path.dirname(targetTdsNodebFeature),os.path.basename(targetTdsNodebFeature),"Point",targetTdsTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
    print "筛选当天的TD基站生成要素"
    logging.info("筛选当天的TD基站生成要素")
    tdNobedFields = arcpy.ListFields(sourceTdsNodebTable)
    TD_NOBED_ALL = arcpy.TableSelect_analysis(sourceTdsNodebTable, "in_memory/TD_NOBED_ALL", "TIME = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
    createFeatureFromXY(TD_NOBED_ALL,"NBAB08","NBAB07",targetTdsNodebFeature,tdNobedFields)

def createTdlEnodeb():
    sourceTdlEnodebTable = ArcCatalogPath+"\\WangYouGis_Oracle97.sde\\WANGYOU.GIS_TDL_CM_ENODEB"
    targetTdlTemp = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouSite.DBO.GIS_LTE_ENODEB_TEMP"
    targetTdlEnodebFeature = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouSite.DBO.GIS_LTE_ENODEB"+yestday.strftime('%Y%m%d')
    print "delete exists targetTdlEnodebFeature"
    if(arcpy.Exists(targetTdlEnodebFeature)):
        arcpy.Delete_management(targetTdlEnodebFeature, "FeatureClass")
    targetTdlEnodebFeature = arcpy.CreateFeatureclass_management(os.path.dirname(targetTdlEnodebFeature),os.path.basename(targetTdlEnodebFeature),"Point",targetTdlTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
    print "筛选当天的LTE基站生成要素"
    logging.info("筛选当天的LTE基站生成要素")
    lteEnobedFields = arcpy.ListFields(sourceTdlEnodebTable)
    LTE_ENOBED_ALL = arcpy.TableSelect_analysis(sourceTdlEnodebTable, "in_memory/LTE_ENOBED_ALL", "TIME = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
    createFeatureFromXY(LTE_ENOBED_ALL,"LONGITUDE","LATITUDE",targetTdlEnodebFeature,lteEnobedFields)
    
if __name__ == '__main__':
    try:
        print "Create GSM ShanXing"
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        logging.info(time.strftime('%Y-%m-%d %H:%M:%S',startTime))
        print "创建GSM基站要素类"
        logging.info("创建GSM基站要素类")
        createGsmBts()
        print "创建TD基站要素类"
        logging.info("创建TD基站要素类")
        createTdsNodeb()
        logging.info("创建LTE基站要素类")
        print "创建LTE基站要素类"
        createTdlEnodeb()
    except Exception,e:
        print e
        logging.error(e)    
