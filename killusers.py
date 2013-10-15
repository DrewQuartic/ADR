#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      ddowling
#
# Created:     01/10/2013
# Copyright:   (c) ddowling 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import arcpy, sys, os, ConfigParser
#import logging.config
import datetime, time
import emailsender

# Setup Logging
#logFile = open("D:\BATCH_FILES\ADR\GDBDeploy_LOG.txt", "wb")
#layerlogFile = open("D:\BATCH_FILES\ADR\GDBLoad_LAYERLOG.txt", "wb")

# Read Configuration File
config = ConfigParser.ConfigParser()
config.read("D:\BATCH_FILES\ADR\GDBLoad.ini")

localGDB = config.get('paths', 'localGDB')
AdminGDB = config.get('paths', 'AdminGDB')

def dropOtherUsers():
    try:
        users = arcpy.ListUsers(AdminGDB)
        for us in users:
            if (not (us.Name == "CITY" and us.ClientName == "GISAPPSERVERDEV")) and (not (us.Name == "SDE" and us.ClientName == "GISAPPSERVERDEV")):
                arcpy.DisconnectUser(AdminGDB, us.ID)
        #arcpy.DisconnectUser(AdminGDB, "ALL")
    except Exception, err:
        emailsender.main("load_notify@sddpc.org","x","Atlas Deploy Error","Problem dropping other user connections: " + str(err),"True")


def dropAllUsers():
    try:
        arcpy.DisconnectUser(AdminGDB, "ALL")
    except Exception, err:
        emailsender.main("load_notify@sddpc.org","x","Atlas Deploy Error","Problem dropping all connections: " + str(err),"True")
if __name__ == '__main__':
    main()
