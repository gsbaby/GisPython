# -*- coding: utf-8 -*-
"""
Tool name: deleteTable
Source: deleteTable.py
Author: ESRI
"""
import arcpy
import time,datetime
import os
import cx_Oracle

spatialReference = arcpy.SpatialReference(4326)

def delete_table(tableName):
    if arcpy.Exists(tableName):
        arcpy.Delete_management(tableName)
    
if __name__ == "__main__":
    delete_table(arcpy.GetParameterAsText(0))
