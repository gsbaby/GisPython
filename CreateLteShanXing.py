#coding=utf-8
import arcpy
from arcpy import env
import math
import string
import time,datetime
import logging
import os

ShanXingR = 0.1
circleR =0.05
ShanXingAngel = 50
ShanXingPointNum = 50
CirclePointNum = 360
rad = math.pi/180
CutRad = 2.5
shapefieldname = "SHAPE"
FidFieldName = "OBJECTID"
spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\logs\\CreateShanXingLTE.log"
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
        print "循环所有数据...."
        for CellRow in cellRows:
            print CellRow.getValue(angleFieldName),CellRow.getValue(lonField),CellRow.getValue(latField)
            inOrOut = CellRow.getValue(inOutField)
            p_lon = CellRow.getValue(lonField)
            p_lat = CellRow.getValue(latField)
            if(p_lon!=None and p_lat!=None and p_lon>90 and p_lon<140 and p_lat>20 and p_lat<40):
                if(inOrOut=="室内"):
                    createIn(CellRow,resultcurFAN,p_lon,p_lat,XiaoQuFields)
                else:
                    createOut(CellRow,resultcurFAN,p_lon,p_lat,angleFieldName,XiaoQuFields)
            else:
                logging.error(str(CellRow.getValue("CI"))+"经纬度有问题!~~")
        del resultcurFAN,cellRows,CellRow
    except Exception,e:
        print e
        logging.error(e)








def createOut(_CellRow,_ShanXingInsertCursor,p_lon,p_lat,angleFieldName,XiaoQuFields):
    ShanXingPerPointAngel = float(ShanXingAngel)/float(ShanXingPointNum)

    cellPolygon = _ShanXingInsertCursor.newRow()
    angel = _CellRow.getValue(angleFieldName)
    #p_lon = CellRow.getValue(lonField)
    #p_lat = CellRow.getValue(latField)
    rdloncos= 111*math.cos(p_lat*rad)
    pointArray = arcpy.Array()
    point0 = arcpy.Point(p_lon, p_lat)
    pointArray.add(point0)
    for i in range(ShanXingPointNum+1):
        if(angel == None):
            angel=0
        currentAngel = angel-ShanXingAngel/2+ShanXingPerPointAngel*i
        if(currentAngel>360):
            currentAngel = currentAngel-360
        rslonX = ShanXingR*math.sin(currentAngel*rad)
        rslatX = ShanXingR*math.cos(currentAngel*rad)
        lonX = p_lon+(rslonX/rdloncos)
        latX = p_lat+rslatX/111    
        pointX = arcpy.Point(lonX, latX)
        pointArray.add(pointX)
    pointArray.add(point0)
    cellPolygon.shape=pointArray
    for XiaoQuField in XiaoQuFields:
        XQFieldName = XiaoQuField.name
        if(XQFieldName!="OBJECTID" and XQFieldName!=shapefieldname):
            cellPolygon.setValue(XQFieldName,_CellRow.getValue(XQFieldName))
    _ShanXingInsertCursor.insertRow(cellPolygon)

def createIn(_CellRow,_ShanXingInsertCursor,p_lon,p_lat,XiaoQuFields):
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
            cellPolygon.setValue(XQFieldName,_CellRow.getValue(XQFieldName))
    _ShanXingInsertCursor.insertRow(cellPolygon)

def createThiessPolygonFast(CellShanXingPois,cellThiessFeature,cellThiessFinal,inOutField,ShengBianJie):
    print "过滤室外站生成泰森多边形"
    logging.info("过滤室外站生成泰森多边形")
    CellsOutPointsLTE=arcpy.Select_analysis(CellShanXingPois, "in_memory/CellsOutPointsLTE", inOutField+"='室外'")
    print "创建小区泰森多边形"
    logging.info("创建小区泰森多边形")
    cellThiessFeatureCacheLTE = arcpy.CreateThiessenPolygons_analysis(CellsOutPointsLTE, "in_memory/cellThiessFeatureCacheLTE", "ALL")
    print "将泰森多边形进行省边界切割"
    logging.info("将泰森多边形进行省边界切割")
    arcpy.Clip_analysis(cellThiessFeatureCacheLTE, ShengBianJie, cellThiessFeature, "")

    cellsThiessPolygonCurs = arcpy.SearchCursor(cellThiessFeature)
    cellThiessFinalInsertCur = arcpy.InsertCursor(cellThiessFinal)
    row = cellsThiessPolygonCurs.next()
    while row:
        currentThiessenPolygon = row.getValue(shapefieldname)
        finalCellThiessRow = cellThiessFinalInsertCur.newRow()
        if(row.getValue("Shape.STArea()")>0.0006 or row.getValue("Shape.STLength()")>0.05):
            print "面积大于0.001，执行裁剪"
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
    del cellsThiessPolygonCurs,cellThiessFinalInsertCur,CellsOutPointsLTE,cellThiessFeatureCacheLTE,row



if __name__ == '__main__':
    try:
        print "Create LTE ShanXing"
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        yestday = datetime.datetime.now() + datetime.timedelta(days=-1)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        currentDoTimeStr = yestday.strftime('%Y-%m-%d 00:00:00')
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        DataSourcePath = "WangYouGis_Oracle97.sde"
        GISDBPath = "WangYouCellThiess60.sde"
        infc = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.GIS_OBJECT_CELL_LTE"
        template = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_OBJECT_CELL_LTE_TEMPLATE"
        CellThiessCache = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.LTE_CELL_THIESSPOLYGON_CACHE"+yestday.strftime('%Y%m%d')
        CellThiessFinalTemp = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_CELL_TSSPOLY_LTE"
        CellThiessFinal = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_CELL_TSSPOLY_LTE"+yestday.strftime('%Y%m%d')

        CellShanXing = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_OBJECT_LTESX"+yestday.strftime('%Y%m%d')
        CellShanXingPoints = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_OBJECT_LTESX_PT"+yestday.strftime('%Y%m%d')

        PointTemplate = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.DBO.GIS_OBJECT_LTEPNT_TEMP"
        PointInCache = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.DBO.GIS_OBJECT_LTEPNTSX_CACHE"+yestday.strftime('%Y%m%d')
        shapefieldname = "SHAPE"
        
        FuJianSheng = "E:\MAP\GaoDe\FuJian20140312.gdb\福建省面"
        hasDone = True
        while(hasDone):
            tryTime = time.localtime(time.time())
            print time.strftime('%Y-%m-%d %H:%M:%S',tryTime)
            inrows = arcpy.SearchCursor(infc,"TIME_STAMP = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
            inrow = inrows.next()
            if(inrow):
                hasDone = False
                print("delete exists CellShanXing")
                logging.info("delete exists CellShanXing")
                if(arcpy.Exists(CellShanXing)):
                    arcpy.Delete_management(CellShanXing, "FeatureClass")
                print "delete exists CellShanXingPoints"
                logging.info("delete exists CellShanXingPoints")
                if(arcpy.Exists(CellShanXingPoints)):
                    arcpy.Delete_management(CellShanXingPoints, "FeatureClass")
                print "delete exists CellThiessFinal"
                logging.info("delete exists CellThiessFinal")
                if(arcpy.Exists(CellThiessFinal)):
                    arcpy.Delete_management(CellThiessFinal, "FeatureClass")
                print "delete exists CellThiessCache"
                logging.info("delete exists CellThiessCache")
                if(arcpy.Exists(CellThiessCache)):
                    arcpy.Delete_management(CellThiessCache, "FeatureClass")
                print "delete exists PointInCache"
                logging.info("delete exists PointInCache")
                if(arcpy.Exists(PointInCache)):
                    arcpy.Delete_management(PointInCache, "FeatureClass")
                XiaoQuFields = arcpy.ListFields(infc)
                PointInCache = arcpy.CreateFeatureclass_management(os.path.dirname(PointInCache),os.path.basename(PointInCache),"Point",PointTemplate,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
                print "筛选室内站生成要素"
                ShiNeiCell_LTE_ALL = arcpy.TableSelect_analysis(infc, "in_memory/ShiNeiCell_LTE_ALL", "HONEYCOMB_TYPE='室内' and TIME_STAMP = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
                createFeatureFromXY(ShiNeiCell_LTE_ALL,"LONGITUDE","LATITUDE",PointInCache,XiaoQuFields)
                print "生成室内站圆形"
                arcpy.Buffer_analysis(PointInCache, CellShanXing, "30 Meters", "FULL", "ROUND", "NONE", "")
                CellThiessFinal = arcpy.CreateFeatureclass_management(os.path.dirname(CellThiessFinal),os.path.basename(CellThiessFinal),"Polygon",CellThiessFinalTemp,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
                arcpy.AddIndex_management(CellThiessFinal, "CI;CITY_NAME", "CCNIndex", "NON_UNIQUE", "NON_ASCENDING")
                arcpy.AddIndex_management(CellThiessFinal, "TIME_STAMP", "TIME_INDEX_LTE", "NON_UNIQUE", "NON_ASCENDING")
                #CellShanXing = arcpy.CreateFeatureclass_management(os.path.dirname(CellShanXing),os.path.basename(CellShanXing),"Polygon",template,"DISABLED","DISABLED",spatialReference)
                print "筛选当天室外站的数据"
                logging.info("筛选当天室外站的数据")
                ShiWai_LTE_ALL = arcpy.TableSelect_analysis(infc, "in_memory/ShiWai_LTE_ALL", "HONEYCOMB_TYPE!='室内' and TIME_STAMP = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
                createShanXing(ShiWai_LTE_ALL,"LONGITUDE","LATITUDE","HONEYCOMB_TYPE",CellShanXing,"ANT_DIRCT_ANGLE",XiaoQuFields)

                print "create label points"
                logging.info("create label points")
                arcpy.FeatureToPoint_management(CellShanXing, CellShanXingPoints, "CENTROID")

                print "使用快速模式创建泰森多边形"
                createThiessPolygonFast(CellShanXingPoints,CellThiessCache,CellThiessFinal,"HONEYCOMB_TYPE",FuJianSheng)
            else:
                print "无数据等待..."
                logging.info("无数据等待...")
                time.sleep(600)
        print "finall delete exists PointInCache"
        logging.info("delete exists PointInCache")
        if(arcpy.Exists(PointInCache)):
            arcpy.Delete_management(PointInCache, "FeatureClass")
        print "delete exists CellThiessCache"
        logging.info("delete exists CellThiessCache")
        if(arcpy.Exists(CellThiessCache)):
            arcpy.Delete_management(CellThiessCache, "FeatureClass")
        endTime = time.localtime(time.time())
        print "开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime)
        logging.info("开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime))
        os._exit(0)
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0)
