# -*- coding: utf-8 -*-
import arcpy
from arcpy import env
import math
import string
import time,datetime
import logging
import os
import logging
import pyodbc


ShanXingR = 0.12
circleR =0.05
ShanXingAngel = 50
ShanXingPointNum = 50
CirclePointNum = 360
rad = math.pi/180
CutRad = 2.5
shapefieldname = "SHAPE"
FidFieldName = "OBJECTID"
spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\logs\\CreateShanXingGSM.log"
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
        
def createShanXing(featureTable,lonField,latField,inOutField,CellShanXingPath,angleFieldName,XiaoQuFields):
    resultcurFAN = arcpy.InsertCursor(CellShanXingPath)
    try:
        cellRows = arcpy.SearchCursor(featureTable)
        for CellRow in cellRows:
            print CellRow.getValue(angleFieldName),CellRow.getValue(lonField),CellRow.getValue(latField)
            inOrOut = CellRow.getValue(inOutField)
            p_lon = CellRow.getValue(lonField)
            p_lat = CellRow.getValue(latField)
            if(p_lon is not None and p_lat is not None and p_lon>90 and p_lon<140 and p_lat>20 and p_lat<40):
                if(inOrOut=="室内"):
                    createIn(CellRow,resultcurFAN,p_lon,p_lat,XiaoQuFields)
                else:
                    createOut(CellRow,resultcurFAN,p_lon,p_lat,angleFieldName,XiaoQuFields)
            else:
                logging.error(str(CellRow.getValue("CI"))+"经纬度有问题!~~")
    except Exception,e:
        print e
        logging.error(e)
    del resultcurFAN,cellRows,CellRow






def createOut(CellRow,_ShanXingInsertCursor,p_lon,p_lat,angleFieldName,XiaoQuFields):
    doShanXingAngel = ShanXingAngel
    doShanXingR = ShanXingR
    if(CellRow.getValue("FLAG_1800")==1):
        print "draw 1800 CELL"
        doShanXingAngel = ShanXingAngel*0.7
        doShanXingR = ShanXingR*1.5
    ShanXingPerPointAngel = float(doShanXingAngel)/float(ShanXingPointNum)
    cellPolygon = _ShanXingInsertCursor.newRow()
    angel = CellRow.getValue(angleFieldName)
    #p_lon = CellRow.getValFidFieldNameue(lonField)
    #p_lat = CellRow.getValue(latField)
    rdloncos= 111*math.cos(p_lat*rad)
    pointArray = arcpy.Array()
    point0 = arcpy.Point(p_lon, p_lat)
    pointArray.add(point0)
    for i in range(ShanXingPointNum+1):
        if(angel is None):
            angel=0
        currentAngel = angel-doShanXingAngel/2+ShanXingPerPointAngel*i
        if(currentAngel>360):
            currentAngel = currentAngel-360
        rslonX = doShanXingR*math.sin(currentAngel*rad)
        rslatX = doShanXingR*math.cos(currentAngel*rad)
        lonX = p_lon+(rslonX/rdloncos)
        latX = p_lat+rslatX/111    
        pointX = arcpy.Point(lonX, latX)
        pointArray.add(pointX)
    pointArray.add(point0)
    cellPolygon.shape=pointArray
    for XiaoQuField in XiaoQuFields:
        XQFieldName = XiaoQuField.name
        if(XQFieldName!="OBJECTID" and XQFieldName!=shapefieldname):
            cellPolygon.setValue(XQFieldName,CellRow.getValue(XQFieldName))
    _ShanXingInsertCursor.insertRow(cellPolygon)

def createIn(CellRow,_ShanXingInsertCursor,p_lon,p_lat,XiaoQuFields):
    cellPolygon = _ShanXingInsertCursor.newRow()
    #p_lon = CellRow.getValue(lonField)
    #p_lat = CellRow.getValue(latField)
    CirclePerPointAngel = 360.0/float(CirclePointNum)
    rdloncos= 111*math.cos(p_lat*rad)
    pointArray = arcpy.Array()
    _angel = 0
    for i in range(CirclePointNum+1):
        _angel = CirclePerPointAngel*i
        if(_angel==360):
            _angel=0
        rslonX = circleR*math.sin(_angel*rad)
        rslatX = circleR*math.cos(_angel*rad)
        lonX = p_lon+(rslonX/rdloncos)
        latX = p_lat+rslatX/111
        pointX = arcpy.Point(lonX, latX)
        pointArray.add(pointX)
    cellPolygon.shape=pointArray
    for XiaoQuField in XiaoQuFields:
        XQFieldName = XiaoQuField.name
        if(XQFieldName!="OBJECTID"):
            cellPolygon.setValue(XQFieldName,CellRow.getValue(XQFieldName))
    _ShanXingInsertCursor.insertRow(cellPolygon)


def createThiessPolygonFast(CellShanXingPois,cellThiessFeature,cellThiessFinal,inOutField,ShengBianJie):
    print "过滤室外站生成泰森多边形"
    logging.info("过滤室外站生成泰森多边形")
    CellsOutPointsGsm=arcpy.Select_analysis(CellShanXingPois, "in_memory/CellsOutPointsGsm", inOutField+"='室外'");
    print "创建小区泰森多边形"
    logging.info("创建小区泰森多边形")
    cellThiessFeatureCacheGsm = arcpy.CreateThiessenPolygons_analysis(CellsOutPointsGsm, "in_memory/cellThiessFeatureCacheGsm", "ALL")
    print "将泰森多边形进行省边界切割"
    logging.info("将泰森多边形进行省边界切割")
    arcpy.Clip_analysis(cellThiessFeatureCacheGsm, ShengBianJie, cellThiessFeature, "")
    cellsThiessPolygonCurs = arcpy.SearchCursor(cellThiessFeature)
    cellThiessFinalInsertCur = arcpy.InsertCursor(cellThiessFinal)
    row = cellsThiessPolygonCurs.next()
    while row:
        currentThiessenPolygon = row.getValue(shapefieldname)
        finalCellThiessRow = cellThiessFinalInsertCur.newRow()
        if(row.getValue("Shape.STArea()")>0.0006 or row.getValue("Shape.STLength()")>0.05):
            print "面积大于0.001或长度大于0.1，执行裁剪"
            ThiessenFID = row.ORIG_FID
            CurrentCellPointCur = arcpy.SearchCursor(CellShanXingPois,"ORIG_FID="+repr(ThiessenFID))
            CurrentCellPoinRow = CurrentCellPointCur.next()
            CenterPoint = CurrentCellPoinRow.getValue(shapefieldname)
            CenterPointPnt = CenterPoint.getPart()
            rdloncos= 111*math.cos(CenterPointPnt.Y*rad)
            pointSingleArray = arcpy.Array()
            for si in range(5):
                singleAngle = 90*si
                if(singleAngle==360):
                    singleAngle=0
                rslonEndSingle = CutRad*math.sin(singleAngle*rad)
                rslatEndSingle = CutRad*math.cos(singleAngle*rad)
                lonEndSingle = CenterPointPnt.X+(rslonEndSingle/rdloncos)
                latEndSingle= CenterPointPnt.Y+rslatEndSingle/111
                pointEndSingle = arcpy.Point(lonEndSingle, latEndSingle)
                pointSingleArray.add(pointEndSingle)
            XiaoQuShanXingSingle = arcpy.Polygon(pointSingleArray,spatialReference)
            CELL_TSDBX_SINGLE = currentThiessenPolygon.intersect(XiaoQuShanXingSingle,4)
            finalCellThiessRow.shape=CELL_TSDBX_SINGLE
            for XiaoQuField in XiaoQuFields:
                XQFieldName = XiaoQuField.name
                if(XQFieldName!="OBJECTID"):
                    finalCellThiessRow.setValue(XQFieldName,row.getValue(XQFieldName))
            finalCellThiessRow.Input_FID = row.Input_FID
            print "CI:"+str(row.CI),row.CITY_NAME
            cellThiessFinalInsertCur.insertRow(finalCellThiessRow)
        else:
            finalCellThiessRow.shape=currentThiessenPolygon
            for XiaoQuField in XiaoQuFields:
                XQFieldName = XiaoQuField.name
                if(XQFieldName!="OBJECTID"):
                    finalCellThiessRow.setValue(XQFieldName,row.getValue(XQFieldName))
            finalCellThiessRow.Input_FID = row.Input_FID
            print "CI:"+str(row.CI),row.CITY_NAME
            cellThiessFinalInsertCur.insertRow(finalCellThiessRow)
        row = cellsThiessPolygonCurs.next()
    print "删除变量~~~~"
    del cellsThiessPolygonCurs,cellThiessFinalInsertCur,CellsOutPointsGsm,cellThiessFeatureCacheGsm,row

        
if __name__ == '__main__':
    try:
        print "Create GSM ShanXing"
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        logging.info(time.strftime('%Y-%m-%d %H:%M:%S',startTime))
        yestday = datetime.datetime.now() + datetime.timedelta(days=-1)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        currentDoTimeStr = yestday.strftime('%Y-%m-%d 00:00:00')
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        DataSourcePath = "WangYouGis_Oracle97.sde"
        GISDBPath = "WangYouCellThiess60.sde"
        infc = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.GIS_OBJECT_CELL_GSM"
        zhiBiaoTableOrg = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.HF_PM_CELL_DAY_2G"
        zhiBiaoTableGis = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_ZHIBIAO_GSM_DAY"+yestday.strftime('%Y%m%d')
        template = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_OBJECT_CELL_GSM_TEMPLATE"
        CellShanXing = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_OBJECT_GSMSX"+yestday.strftime('%Y%m%d')
        CellShanXingPoints = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_OBJECT_GSMSX_PT"+yestday.strftime('%Y%m%d')
        #CellThiessTemp = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouGis.dbo.GIS_CELL_THIESS_GSM_TEMP"
        CellThiessCache = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GSM_CELL_THIESSPOLYGON_CACHE"
        CellThiessFinalTemp = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_CELL_TSSPOLY_GSM"
        CellThiessFinal = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_CELL_TSSPOLY_GSM"+yestday.strftime('%Y%m%d')
        PointTemplate = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.DBO.GIS_OBJECT_GSMPNT_TEMP"
        PointInCache = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.DBO.GIS_OBJECT_GSMPNT_CACHE"
        FuJianSheng = "E:\MAP\GaoDe\FuJian20140312.gdb\福建省面"
    
        print "delete exists CellShanXingPoints"
        logging.info("delete exists CellShanXingPoints")
        if(arcpy.Exists(CellShanXingPoints)):
            arcpy.Delete_management(CellShanXingPoints, "FeatureClass")
        print("delete exists CellShanXing")
        logging.info("delete exists CellShanXing")
        if(arcpy.Exists(CellShanXing)):
            arcpy.Delete_management(CellShanXing, "FeatureClass")
        print "delete exists CellThiessCache"
        logging.info("delete exists CellThiessCache")
        if(arcpy.Exists(CellThiessCache)):
            arcpy.Delete_management(CellThiessCache, "FeatureClass")
        print "delete exists PointInCache"
        logging.info("delete exists PointInCache")
        if(arcpy.Exists(PointInCache)):
            arcpy.Delete_management(PointInCache, "FeatureClass")
        ##print "delete exists zhiBiaoTableGis"
        ##logging.info("delete exists zhiBiaoTableGis")
        ##if(arcpy.Exists(zhiBiaoTableGis)):
            ##arcpy.Delete_management(zhiBiaoTableGis)
        print "delete exists CellThiessFinal"
        logging.info("delete exists CellThiessFinal")
        if(arcpy.Exists(CellThiessFinal)):
            arcpy.Delete_management(CellThiessFinal, "FeatureClass")
        #CellShanXing = arcpy.CreateFeatureclass_management(os.path.dirname(CellShanXing),os.path.basename(CellShanXing),"Polygon",template,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        PointInCache = arcpy.CreateFeatureclass_management(os.path.dirname(PointInCache),os.path.basename(PointInCache),"Point",PointTemplate,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        #CellThiessFinal = arcpy.CreateFeatureclass_management(os.path.dirname(CellThiessFinal),os.path.basename(CellThiessFinal),"Polygon",CellThiessTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
        XiaoQuFields = arcpy.ListFields(infc)
        CellThiessFinal = arcpy.CreateFeatureclass_management(os.path.dirname(CellThiessFinal),os.path.basename(CellThiessFinal),"Polygon",CellThiessFinalTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))


        arcpy.AddIndex_management(CellThiessFinal, "CI;CITY_NAME", "CCNGSMIndex", "NON_UNIQUE", "NON_ASCENDING")
        arcpy.AddIndex_management(CellThiessFinal, "TIME_STAMP", "TIME_INDEX_GSM", "NON_UNIQUE", "NON_ASCENDING")

        
        print "筛选室内站生成要素"
        ShiNeiCell_GSM_ALL = arcpy.TableSelect_analysis(infc, "in_memory/ShiNeiCell_GSM_ALL", "HONEYCOMB_TYPE='室内' and TIME_STAMP = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
        createFeatureFromXY(ShiNeiCell_GSM_ALL,"LONGITUDE","LATITUDE",PointInCache,XiaoQuFields)
        print "生成室内站圆形"
        arcpy.Buffer_analysis(PointInCache, CellShanXing, "30 Meters", "FULL", "ROUND", "NONE", "")
        

        print "筛选当天室外站的数据"
        logging.info("筛选当天室外站的数据")
        ShiWai_GSM_ALL = arcpy.TableSelect_analysis(infc, "in_memory/ShiWaiCell_GSM_ALL", "HONEYCOMB_TYPE!='室内' and TIME_STAMP = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
        createShanXing(ShiWai_GSM_ALL,"LONGITUDE","LATITUDE","HONEYCOMB_TYPE",CellShanXing,"ANT_DIRCT_ANGLE",XiaoQuFields)

        #arcpy.AddIndex_management(CellShanXing, "CITY_CI", "CCNGSMSXIndex", "NON_UNIQUE", "NON_ASCENDING")
        #arcpy.AddIndex_management(CellShanXing, "TIME_STAMP", "TIME_INDEX_SXGSM", "NON_UNIQUE", "NON_ASCENDING")

        print "create label points"
        logging.info("create label points")
        arcpy.FeatureToPoint_management(CellShanXing, CellShanXingPoints, "CENTROID")
        ##print "筛选当天的指标数据"
        ##arcpy.TableSelect_analysis(zhiBiaoTableOrg, zhiBiaoTableGis, "TIME_STAMP = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
        ##ZhiBiaoTable = "GIS_ZHIBIAO_GSM_DAY"+yestday.strftime('%Y%m%d')
        ##print "ZhiBiaoTable:"+ZhiBiaoTable
        print "使用快速模式创建泰森多边形"
        createThiessPolygonFast(CellShanXingPoints,CellThiessCache,CellThiessFinal,"HONEYCOMB_TYPE",FuJianSheng)

        endTime = time.localtime(time.time())
        print "开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime)
        logging.info("开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime))
        os._exit(0)
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0)
