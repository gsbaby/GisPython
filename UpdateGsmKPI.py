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
import httplib, urllib, json
import getpass

spatialReference = arcpy.SpatialReference(4326)
LOG_FILE_NAME = "E:\\GisPython\\logs\\UpdateGsmKPI.log"
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)

username = "arcadmin"
password = "Passw0rd"
serverName = "10.48.186.10"
serverPort = 6080

serviceFolder = "/arcgis/admin/services/DayCell/"

def updateKPI():
    print "将指标值Update到泰森多边形"
    logging.info("将指标值Update到泰森多边形")
    print "connec to WangYouGis DB..."
    dbConn=pyodbc.connect('DRIVER={SQL Server};SERVER=10.48.186.60;DATABASE=WangYouCellThiess;UID=sa;PWD=R00t.1929ncs')

    updateSqlDefault = "UPDATE GIS_CELL_TSSPOLY_GSM"+yestday.strftime('%Y%m%d')+" set VOICE_TOTAL_TRAFFIC =0,UL_DL_FLOW=0 "
    cursor =dbConn.cursor()
    logging.info("更新指标值 默认0")
    cursor.execute(updateSqlDefault)    
    dbConn.commit()
    updateSql = "UPDATE a set a.[VOICE_TOTAL_TRAFFIC] = b.[VOICE_TOTAL_TRAFFIC],a.[UL_DL_FLOW]=b.[UL_DL_FLOW] "
    updateSql+=" from GIS_CELL_TSSPOLY_GSM"+yestday.strftime('%Y%m%d')+"  a,"
    updateSql+="GIS_ZHIBIAO_GSM_DAY"+yestday.strftime('%Y%m%d')+" b where a.CI=b.CI and a.CITY_NAME=b.CITY_NAME and a.TIME_STAMP = b.TIME_STAMP"
    print "更新指标值"
    logging.info("更新指标值")
    cursor.execute(updateSql)
    updateMSql = "update a set a.M_VTT = b.VOICE_TOTAL_TRAFFIC,a.M_UDF = b.UL_DL_FLOW"
    updateMSql+= " from GIS_CELL_TSSPOLY_GSM"+yestday.strftime('%Y%m%d')+"  a,HF_PM_CELL_DAY_2G_M b "
    updateMSql+= " where a.CI=b.CI and a.CITY_NAME = b.CITY_NAME;"
    cursor.execute(updateMSql)
    updateWSql = "update a set a.W_VTT = b.VOICE_TOTAL_TRAFFIC,a.W_UDF = b.UL_DL_FLOW"
    updateWSql+= " from GIS_CELL_TSSPOLY_GSM"+yestday.strftime('%Y%m%d')+"  a,HF_PM_CELL_DAY_2G_W b "
    updateWSql+= " where a.CI=b.CI and a.CITY_NAME = b.CITY_NAME;"    
    cursor.execute(updateWSql)
    updateWsType = "UPDATE a  set  a.WS_TYPE = b.WS_TYPE  from GIS_CELL_TSSPOLY_GSM"+yestday.strftime('%Y%m%d')+" a,GIS_WS_NAME_TYPE b where a.WS_NAME = b.WS_NAME "
    cursor.execute(updateWsType)
    print updateWsType
    dbConn.commit()
    logging.info("更新扇形指标值")
    updateSXSql = "UPDATE a set a.[VOICE_TOTAL_TRAFFIC] = b.[VOICE_TOTAL_TRAFFIC],a.[UL_DL_FLOW]=b.[UL_DL_FLOW] "
    updateSXSql+=" from GIS_OBJECT_GSMSX"+yestday.strftime('%Y%m%d')+"  a,"
    updateSXSql+="GIS_ZHIBIAO_GSM_DAY"+yestday.strftime('%Y%m%d')+" b where a.CI=b.CI and a.CITY_NAME=b.CITY_NAME and a.TIME_STAMP = b.TIME_STAMP"
    cursor.execute(updateSXSql)
    updateSXMSql = "update a set a.M_VTT = b.VOICE_TOTAL_TRAFFIC,a.M_UDF = b.UL_DL_FLOW"
    updateSXMSql+= " from GIS_OBJECT_GSMSX"+yestday.strftime('%Y%m%d')+"  a,HF_PM_CELL_DAY_2G_M b "
    updateSXMSql+= " where a.CI=b.CI and a.CITY_NAME = b.CITY_NAME;"
    cursor.execute(updateSXMSql)
    updateSXWSql = "update a set a.W_VTT = b.VOICE_TOTAL_TRAFFIC,a.W_UDF = b.UL_DL_FLOW"
    updateSXWSql+= " from GIS_OBJECT_GSMSX"+yestday.strftime('%Y%m%d')+"  a,HF_PM_CELL_DAY_2G_W b "
    updateSXWSql+= " where a.CI=b.CI and a.CITY_NAME = b.CITY_NAME;"    
    cursor.execute(updateSXWSql)
    updateSXWsType = "UPDATE a  set  a.WS_TYPE = b.WS_TYPE  from GIS_OBJECT_GSMSX"+yestday.strftime('%Y%m%d')+" a,GIS_WS_NAME_TYPE b where a.WS_NAME = b.WS_NAME "
    cursor.execute(updateSXWsType)
    print updateSXWsType
    dbConn.commit()
    dbConn.close()

def createYun():
    StopOrStartService(serviceFolder,"DynamicGSMRasterHW","stop")
    StopOrStartService(serviceFolder,"DynamicGSMRasterLL","stop")
    if(arcpy.Exists(CellThiessYunHW)):
        arcpy.Delete_management(CellThiessYunHW)
    if(arcpy.Exists(CellThiessYunLL)):
        arcpy.Delete_management(CellThiessYunLL)
    logging.info("PolygonToRaster_conversion HW")
    print "面转栅格"
    # Process: 面转栅格
    arcpy.PolygonToRaster_conversion(CellThiessFinal, "VOICE_TOTAL_TRAFFIC", CellThiessYunHW, "CELL_CENTER", "NONE", ".01")
    logging.info("CalculateStatistics_management HW")
    print "计算统计数据"
    # Process: 计算统计数据
    arcpy.CalculateStatistics_management(CellThiessYunHW, "1", "1", "", "OVERWRITE","")
    logging.info("PolygonToRaster_conversion LL")
    arcpy.PolygonToRaster_conversion(CellThiessFinal, "UL_DL_FLOW", CellThiessYunLL, "CELL_CENTER", "NONE", ".01")
    logging.info("CalculateStatistics_management LL")
    print "计算统计数据"
    # Process: 计算统计数据
    arcpy.CalculateStatistics_management(CellThiessYunLL, "1", "1", "", "OVERWRITE","")
    StopOrStartService(serviceFolder,"DynamicGSMRasterHW","start")
    StopOrStartService(serviceFolder,"DynamicGSMRasterLL","start")


def StopOrStartService(serviceFolder,serviceName,stopOrStart):
    token = getToken(username, password, serverName, serverPort)
    if token == "":
        print "Could not generate a token with the username and password provided."
        return
    # This request only needs the token and the response formatting parameter 
    params = urllib.urlencode({'token': token, 'f': 'json'})
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    # Connect to URL and post parameters    
    httpConn = httplib.HTTPConnection(serverName, serverPort)
    # Get a token
    fullSvcName = serviceName + "." + "MapServer"
    stopOrStartURL = serviceFolder+fullSvcName+"/" + stopOrStart
    httpConn.request("POST", stopOrStartURL, params, headers)
    # Read stop or start response
    stopStartResponse = httpConn.getresponse()
    if (stopStartResponse.status != 200):
        httpConn.close()
        print "Error while executing stop or start. Please check the URL and try again."
        return
    else:
        stopStartData = stopStartResponse.read()
        
        # Check that data returned is not an error object
        if not assertJsonSuccess(stopStartData):
            if str.upper(stopOrStart) == "START":
                print "Error returned when starting service " + fullSvcName + "."
            else:
                print "Error returned when stopping service " + fullSvcName + "."

            print str(stopStartData)
            
        else:
            print "Service " + fullSvcName + " processed successfully."

    httpConn.close()           
    return

# A function to generate a token given username, password and the adminURL.
def getToken(username, password, serverName, serverPort):
    # Token URL is typically http://server[:port]/arcgis/admin/generateToken
    tokenURL = "/arcgis/admin/generateToken"
    
    params = urllib.urlencode({'username': username, 'password': password, 'client': 'requestip', 'f': 'json'})
    
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    
    # Connect to URL and post parameters
    httpConn = httplib.HTTPConnection(serverName, serverPort)
    httpConn.request("POST", tokenURL, params, headers)
    
    # Read response
    response = httpConn.getresponse()
    if (response.status != 200):
        httpConn.close()
        print "Error while fetching tokens from admin URL. Please check the URL and try again."
        return
    else:
        data = response.read()
        httpConn.close()
        
        # Check that data returned is not an error object
        if not assertJsonSuccess(data):            
            return
        
        # Extract the token from it
        token = json.loads(data)        
        return token['token']            
        

# A function that checks that the input JSON object 
#  is not an error object.
def assertJsonSuccess(data):
    obj = json.loads(data)
    if 'status' in obj and obj['status'] == "error":
        print "Error: JSON object returns an error. " + str(obj)
        return False
    else:
        return True
    
if __name__ == '__main__':
    try:
        print "update GSM KPI"
        startTime = time.localtime(time.time())
        print time.strftime('%Y-%m-%d %H:%M:%S',startTime)
        yestday = datetime.datetime.now() + datetime.timedelta(days=-2)
        print yestday.strftime('%Y-%m-%d 00:00:00')
        currentDoTimeStr = yestday.strftime('%Y-%m-%d 00:00:00')
        curWeekDay = datetime.datetime.weekday(datetime.datetime.now())
        lastWeekDateStart = datetime.datetime.now() + datetime.timedelta(days=-(7+curWeekDay))
        lastWeekDateEnd = datetime.datetime.now() + datetime.timedelta(days=-(1+curWeekDay))
        curDay = startTime.tm_mday
        lastMonthEnd = datetime.datetime.now()-datetime.timedelta(days=curDay)
        
        ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
        DataSourcePath = "WangYouGis_Oracle97.sde"
        GISDBPath = "WangYouCellThiess60.sde"
        SITEDBPath = "WangYouGis_Site15.sde"
        zhiBiaoTableOrg = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.HF_PM_CELL_DAY_2G"
        zhiBiaoTableGis = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_ZHIBIAO_GSM_DAY"+yestday.strftime('%Y%m%d')
        CellThiessCache = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GSM_CELL_THIESSPOLYGON_CACHE"
        CellThiessFinalTemp = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_CELL_TSSPOLY_GSM"
        CellThiessFinal = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_CELL_TSSPOLY_GSM"+yestday.strftime('%Y%m%d')
        CellThiessYunHW = ArcCatalogPath+"\\"+SITEDBPath+"\\WangYouSite.dbo.GIS_RASTER_TSS_GSM_HW"
        CellThiessYunLL = ArcCatalogPath+"\\"+SITEDBPath+"\\WangYouSite.dbo.GIS_RASTER_TSS_GSM_LL"
        CellShanXing = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.GIS_OBJECT_GSMSX"+yestday.strftime('%Y%m%d')
        zhiBiaoTableMOrg = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.HF_PM_CELL_DAY_2G_M"
        zhiBiaoTableWOrg = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.HF_PM_CELL_DAY_2G_W"
        zhiBiaoTableGisM = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.HF_PM_CELL_DAY_2G_M"
        zhiBiaoTableGisW = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouCellThiess.dbo.HF_PM_CELL_DAY_2G_W"

        print "delete exists zhiBiaoTableGis"
        logging.info("delete exists zhiBiaoTableGis")
        if(arcpy.Exists(zhiBiaoTableGis)):
            arcpy.Delete_management(zhiBiaoTableGis)           
        print "筛选当天的指标数据"
        arcpy.TableSelect_analysis(zhiBiaoTableOrg, zhiBiaoTableGis, "TIME_STAMP = TO_DATE('"+currentDoTimeStr+"','YYYY-MM-DD HH24:MI:SS')")
        if(curDay==3):    
            print "生成月平均"
            print "delete exists zhiBiaoTableGisM"
            logging.info("delete exists zhiBiaoTableGisM")
            if(arcpy.Exists(zhiBiaoTableGisM)):
                arcpy.Delete_management(zhiBiaoTableGisM)            
            arcpy.TableSelect_analysis(zhiBiaoTableMOrg, zhiBiaoTableGisM, "TIME_STAMP = TO_DATE('"+lastMonthEnd.strftime('%Y-%m-01 00:00:00')+"','YYYY-MM-DD HH24:MI:SS')")
        if(curWeekDay==3):
            print "生成周平均"
            print "delete exists zhiBiaoTableGisW"
            logging.info("delete exists zhiBiaoTableGisW")
            if(arcpy.Exists(zhiBiaoTableGisW)):
                arcpy.Delete_management(zhiBiaoTableGisW)            
            arcpy.TableSelect_analysis(zhiBiaoTableWOrg, zhiBiaoTableGisW, "TIME_STAMP = TO_DATE('"+lastWeekDateStart.strftime('%Y-%m-%d 00:00:00')+"','YYYY-MM-DD HH24:MI:SS')")

        print "更新指标值...."
        updateKPI()
        logging.info("create Yun")
        createYun()
        arcpy.AddIndex_management(CellThiessFinal, "WS_TYPE", "gsmwsTypeIndex", "NON_UNIQUE", "NON_ASCENDING")
        arcpy.AddIndex_management(CellShanXing, "WS_TYPE", "gsmSXWSTypeIndex", "NON_UNIQUE", "NON_ASCENDING")        
        endTime = time.localtime(time.time())
        print "开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime)
        logging.info("开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',startTime)+" 结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',endTime))
        os._exit(0)
    except Exception,e:
        print e
        logging.error(e)
        os._exit(0)   
