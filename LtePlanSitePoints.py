#coding=utf-8
import arcpy
from arcpy import env
import math
import string
import time,datetime
import logging
import os
import httplib, urllib, json
import getpass

spatialReference = arcpy.SpatialReference(4326)
shapefieldname = "SHAPE"
username = "arcadmin"
password = "Passw0rd"
serverName = "10.48.186.82"
serverPort = 6080
WangYouFolder = "/arcgis/admin/services/WangYou/"
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
        siteType = CellRow.getValue("SITE_TYPE")
        if(siteType=="宏站BBU+RRU" or siteType=="宏站RRU"):
            resultRow.setValue("IN_OUT","宏站")
        else:
            resultRow.setValue("IN_OUT","室分")
        resultFeatcur.insertRow(resultRow)
    del resultFeatcur,featRows

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
            print "Service " + fullSvcName + " processed "+stopOrStart+" successfully."

    httpConn.close()           
    return

if __name__ == '__main__':
    ArcCatalogPath = "C:\\Users\\Administrator\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog"
    DataSourcePath = "WangYouGis_Oracle97.sde"
    GISDBPath = "WangYouGis15.sde"
    infc = ArcCatalogPath+"\\"+DataSourcePath+"\\WANGYOU.QSMZQ_ZDK_TDL"
    template = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouGis.DBO.LTE_PLANSITE_TEMP"
    resultFeature = ArcCatalogPath+"\\"+GISDBPath+"\\WangYouGis.DBO.LTE_PLANSITE_POINTS"
    print "start"
    StopOrStartService(WangYouFolder,"PlanSite","stop")
    StopOrStartService(WangYouFolder,"LteEquipment","stop")
    print "delete exists resultFeature"
    if(arcpy.Exists(resultFeature)):
        arcpy.Delete_management(resultFeature, "FeatureClass")
    resultFeature = arcpy.CreateFeatureclass_management(os.path.dirname(resultFeature),os.path.basename(resultFeature),"Point",template,"DISABLED","DISABLED",arcpy.SpatialReference(4326))
    XiaoQuFields = arcpy.ListFields(infc)
    print "Create feature!begin!~"
    createFeatureFromXY(infc,"LONGITUDE","LATITUDE",resultFeature,XiaoQuFields)
    print "Create feature!end!~"
    StopOrStartService(WangYouFolder,"PlanSite","start")
    StopOrStartService(WangYouFolder,"LteEquipment","start")
