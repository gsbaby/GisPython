# -*- coding: utf-8 -*-
import arcpy
import time,datetime
import os
import cx_Oracle
import logging
import pyodbc
import getpass
import httplib, urllib, json

spatialReference = arcpy.SpatialReference(4326)
username = "arcadmin"
password = "Passw0rd"
serverName = "10.48.186.10"
serverPort = 6080
servicesFolder = "/arcgis/admin/services/Other/"


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
    LOG_FILE_NAME = "E:\\GisPython\\logs\\TFDZJobs.log"
    logging.basicConfig(filename=LOG_FILE_NAME,level=logging.INFO)
    logging.basicConfig(filename=LOG_FILE_NAME,level=logging.ERROR)
    ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
    GISDBPath = "WangYouGis60.sde"
    DataSourcePath = "WangYouGis_Oracle97.sde"
    infc_now_cell = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.F_TFDZ_NOW_CELL"
    infc_minus_cell = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.F_TFDZ_MINUS_CELL"
    out_now_cell =  ArcCatalogPath+"\\"+GISDBPath+"\\WANGYOUGIS.DBO.F_TFDZ_NOW_CELL"
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    logging.info("开始时间:"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

    StopOrStartService(servicesFolder,"SSTFGJJK","stop")
    #WANGYOU.F_TFDZ_NOW_CELL  WANGYOU.F_TFDZ_MINUS_CELL
    print "create F_TFDZ_NOW_CELL"
    logging.info("create F_TFDZ_NOW_CELL")
    if(arcpy.Exists(out_now_cell)):
        arcpy.Delete_management(out_now_cell)
    infc_now_today = arcpy.TableSelect_analysis(infc_now_cell, "in_memory/infc_now_today", "(WEEKDAY = case when (to_char(sysdate,'d')-1)='0' then '7' else to_char(to_char(sysdate,'d')-1) end) and LONGITUDE is not null")
    array = arcpy.da.TableToNumPyArray(infc_now_today, ["CITY_NAME","SEC_NAME","NET_TYPE","VENDOR_NAME","STATION_NAME","ROOM_NAME","TOWN_NAME","ENODEBID","CI","LONGITUDE","LATITUDE","IP_ADDR","IS_SQGB","GRADE_NAME"],null_value={})
    print len(array)
    arcpy.da.NumPyArrayToFeatureClass(array, out_now_cell, ("LONGITUDE", "LATITUDE"),arcpy.SpatialReference('WGS 1984'))
    StopOrStartService(servicesFolder,"SSTFGJJK","start")
    #dbo.updateDzAllType
    print "update updateDzAllType"
    logging.info("update updateDzAllType")
    dbConnUpdate=pyodbc.connect('DRIVER={SQL Server};SERVER=10.48.186.60;DATABASE=WANGYOUGIS;UID=sa;PWD=R00t.1929ncs')
    cursorUpdate =dbConnUpdate.cursor()
    updateSql = "exec updateDzAllType"
    cursorUpdate.execute(updateSql)
    dbConnUpdate.commit()
    dbConnUpdate.close()
    logging.info("结束时间:"+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
