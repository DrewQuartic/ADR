# ---------------------------------------------------------------------------
# Script:     GDBDeploy.py
# Created on: October 27 2008
# Author:     Chris Pyle, SDDPC
#
# 11/23/2010 - Dave Bishop
#              Removed registering stuff and everything related to load.
#2013/1/3 - Changed to rename new FCs inside feature datasets
#2/1/2013 - Changed to manage multiple schema processes
#2/19/2013 - Updated for multi receivers for email
#
# 08/06/2013 - Bill Corey
#              Updated script to replace Logging with writing to a text file
# ---------------------------------------------------------------------------

import arcpy, sys, getopt, ConfigParser
#import logging.config
import datetime
import smtplib

myftp = __import__('ftp_get_fgdbs')

# Setup Logging
logFile = open("D:\BATCH_FILES\ADR\GDBDeploy_LOG.txt", "a")


# Read Configuration File
config = ConfigParser.ConfigParser()
config.read("D:\BATCH_FILES\ADR\GDBDeploy.ini")

def parseBoolString(theString):
    parseBoolString = (theString[0].upper()=='T')
    return parseBoolString

def parseIntString(theString):
    try:
        return int(float(theString))
    except:
        return None

# Constants
NEW = "_NEW"
BAK= "_BAK"
BAK2 = "_BK2"

# Local variables...
fGDB = config.get('paths', 'fGDB')

localGDB_list = config.get('paths', 'localGDB')
localGDB_list = localGDB_list.split(',')
adminGDB = config.get('paths', 'adminGDB')
schema_list = config.get('names', 'schema')
schema_list = schema_list.split(",")

updateTable = config.get('paths', 'updateTable')
updateTableName = config.get('paths', 'updateTableName')
updateTableSrc = 'D:\Data\IN\SANGIS\manifest.gdb\DPI'
dataMgmtTools = config.get('paths', 'dataMgmtTools')
forceLockoutChk = parseBoolString(config.get('settings', 'forceLockout'))
forceLockout = forceLockoutChk

maxLayers = parseIntString(config.get('settings', 'maxLayers'))
maxMinutes = parseIntString(config.get('settings', 'maxMinutes'))

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def stripSchema(lName):
    # Check for schema name
    ls = lName.split(".")
    lName= ls[0]
    if len(ls) > 1:
        # Get the Layer name string
        # Assumes SCHEMA.LAYERNAME - change index to 2 for SQL-Server where SDW.SCHEMA.LAYERNAME
        # Added last if to include datasets
        if len(ls) == 2:
            lName = ls[1]
        if len(ls) == 3:
            lName = ls[2]
        if len(ls) == 5:
            lName = ls[4]
    return lName

def updateDate(fc, updateTableSrc):
    fc = stripSchema(fc)
    try:
        theCount = 0
        c1 = arcpy.da.UpdateCursor(updateTable, ["UPDATE_DATE"], "\"SANGIS_LAYER_NAME\" = '" + str(fc) + "'")
        c2 = arcpy.da.SearchCursor(updateTableSrc, ["UPDATE_DATE", "LAYER_NAME"], "\"LAYER_NAME\" = '" + str(fc) + "'")
        for r1 in c1:
            for r2 in c2:
                theCount = theCount + 1
                r1[0] = r2[0]
                c1.updateRow(r1)
        del c1

        if theCount == 0: # Layer does not exist in table
            c3 = arcpy.da.InsertCursor(updateTable, ["SANGIS_LAYER_NAME", "UPDATE_DATE",])
            SANGIS_LAYER_NAME = c2[1]
            UPDATE_DATE = c2[0]
            c3.insertRow([SANGIS_LAYER_NAME, UPDATE_DATE])
            del c2, c3,
        del theCount
        logFile.write ("\n" + "Updated Timestamp for %s..." % fc)
    except Exception, err:
        logFile.write ("\n" + "Unable to update date timestamp for %s" % fc)
        logFile.write ("\n" + 'Timestamp Update Error: %s' % str(err))

def aliasExists(aliasName):  # SANGIS.LONG_NAME_ALIAS
    aliasExists = False
    aliasName = stripSchema(aliasName)
    lName = ""
    try:
        theCount = 0
        c1 = arcpy.da.SearchCursor(updateTable, ["ALIAS"], "\"SANGIS_LAYER_NAME\" = '" + aliasName + "'")
        for r1 in c1:
            aliasExists = True
            theCount = theCount + 1
            lName = r1[0]
            logFile.write ("\n" + "%s has the alias name %s" % (lName, aliasName))
        del c1

        return [aliasExists,lName]

    except:
        logFile.write ("\n" + "Unable to check alias name %s." % aliasName)
        return [aliasExists,lName]

def getDataset(fc):
    lName = stripSchema(fc)
    theCount = 0
    dataset = ""
    try:
        c1 = arcpy.da.SearchCursor(updateTable, ["Dataset"], "\"SANGIS_LAYER_NAME\" = '" + lName + "'")
        for r1 in c1:
            theCount = theCount + 1
            dataset = r1[0]
            logFile.write ("\n" + "%s resides in the dataset %s" % (lName, dataset))
        del c1

        return[dataset]

    except:
        logFile.write ("\n" + "Unable to find dataset for %s" % fc)
        return[dataset]

def getUserRole(fc):
    lName = stripSchema(fc)
    theCount = 0
    role = ""
    try:
        c1 = arcpy.da.SearchCursor(updateTable, ["USER_PRIVILEGES"], "\"SANGIS_LAYER_NAME\" = '" + lName + "'")
        for r1 in c1:
            theCount = theCount + 1
            role = r1[0]
            logFile.write ("\n" + "%s has the role %s" % (lName, role))
        del c1

        return[role]

    except:
        logFile.write ("\n" + "Unable to find role for %s" % fc)
        return[role]

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])
        except getopt.error, msg:
            raise Usage(msg)

        # Setup Email major error
        isErr = False
        Errmlmssg = "Development Deploy ERRORS: "
        sender = 'deploy_notify@sddpc.org'
        receivers = ['steve@quarticsolutions.com', 'bill@quarticsolutions.com', 'timo@quarticsolutions.com']#'drew@quarticsolutions.com', 'rob@quarticsolutions.com', 'timo@quarticsolutions.com']
        deployedFCs = []
        numLayers = 0
        numErrors = 0
        startTime = datetime.datetime.today()

        logFile.write ("\n" + "-----------------Beginning Deploy Job---------------")


        #for each schema in config, it loops and checks for a matching (list location) gdb connection and privs list
        # if no matching then it keeps the same information for that arguement
        schemacnt = 0
        for schema in schema_list:
            fcsearch = schema + "*_NEW"
            dssearch = schema + "*"
            if schemacnt <= len(localGDB_list)-1:
                localGDB = localGDB_list[schemacnt]
            schemacnt = schemacnt + 1

            print schema
            print schemacnt
            print localGDB

            arcpy.env.workspace = localGDB

            #solve the database lock blocking other schema processes
            # by leaving the database unlocked during their process
            if not "SANGIS" in schema:
                forceLockout = False
            else:
                forceLockout = forceLockoutChk

            #forceLockout inside the schema loop, after connecting
            if forceLockout:
                #Lock-out all users
                try:
                    arcpy.AcceptConnections(adminGDB, False)
                    arcpy.DisconnectUser(adminGDB, "ALL")

                except:
                    raise Exception, "Failed To Lockout SDE Accounts"
                    isErr = True
                    Errmlmssg = Errmlmssg + "\n" + "Could not lock SDE database"


            updateFCList = arcpy.ListFeatureClasses(fcsearch)
            updateTblList = arcpy.ListTables(fcsearch)
            updateList = updateFCList + updateTblList
            updateDSList = arcpy.ListDatasets(dssearch,"Feature")
            print updateDSList
            for Ds in updateDSList:
                for fc in arcpy.ListFeatureClasses(fcsearch,"All",Ds):
                    updateList.append(fc)

            updateList.sort()

            #Check for priority layers and resort
            if updateList.count("SDW.CITY.PARCELS_ALL_NEW") > 0:
                pidx = updateList.index("SDW.CITY.PARCELS_ALL_NEW")
                if pidx > 0:
                    try:
                        updateList.remove("SDW.CITY.PARCELS_ALL_NEW")
                        updateList.insert(0,"SDW.CITY.PARCELS_ALL_NEW")
                        logFile.write ("\n" + "****SDW.CITY.PARCELS_ALL_NEW layer being moved in list")
                    except:
                        logFile.write ("\n" + "****-----")
                        logFile.write ("\n" + "****Warning: Couldn't move SDW.CITY.PARCELS_ALL_NEW to new location")
                        logFile.write ("\n" + "****-----")
                        isErr = True
                        Errmlmssg = Errmlmssg + "\n" + "Couldn't move SDW.CITY.PARCELS_ALL_NEW to new location"

            if updateList.count("SDW.CITY.ROADS_ALL_NEW") > 0:
                pidx = updateList.index("SDW.CITY.ROADS_ALL_NEW")
                if pidx > 0:
                    try:
                        updateList.remove("SDW.CITY.ROADS_ALL_NEW")
                        updateList.insert(0,"SDW.CITY.ROADS_ALL_NEW")
                        logFile.write ("\n" + "****SDW.CITY.ROADS_ALL_NEW layer being moved in list")
                    except:
                        logFile.write ("\n" + "****-----")
                        logFile.write ("\n" + "****Warning: Couldn't move SDW.CITY.ROADS_ALL_NEW to new location")
                        logFile.write ("\n" + "****-----")
                        isErr = True
                        Errmlmssg = Errmlmssg + "\n" + "Couldn't move SDW.CITY.ROADS_ALL_NEW to new location"



            if len(updateList) > 0:

                #For each updated layer
                for fc in updateList:
                    numLayers = numLayers + 1
                    endTime = datetime.datetime.today()
                    minschk, secs = divmod((endTime - startTime).seconds, 60)

                    #check for max settings
                    if maxLayers > 0:
                        if numLayers > maxLayers:
                            numLayers = numLayers - 1
                            logFile.write ("\n" + "****-----")
                            logFile.write ("\n" + "****Warning: Maximum number of layers to be deployed (%d) exceeded. Process loop stopped." % maxLayers)
                            logFile.write ("\n" + "****-----")
                            isErr = True
                            Errmlmssg = Errmlmssg + "\n" + "Maximum Layer Count Reached" + "\n" + str(numLayers - 1) + "layers loaded"
                            if forceLockout:
                                #Unlock all users
                                try:
                                    arcpy.AcceptConnections(localGDB, True)
                                except:
                                    raise Exception, "Failed to unlock SDE Accounts"
                                    isErr = True
                                    Errmlmssg = Errmlmssg + "\n" + "Failed to unlock SDE Accounts"
                            break
                    elif maxMinutes > 0:
                        if minschk > maxMinutes:
                            loadCount = numLayers - 1
                            logFile.write ("\n" + "****-----")
                            logFile.write ("\n" + "****Warning: Maximum time exceeded. No more layers will be deployed.")
                            logFile.write ("\n" + "****-----")
                            isErr = True
                            Errmlmssg = Errmlmssg + "\n" + "Maximum Time Limit Reached" + "\n" + str(numLayers) - 1 + "layers loaded"
                            if forceLockout:
                                #Unlock all users
                                try:
                                    arcpy.AcceptConnections(localGDB, True)
                                except:
                                    raise Exception, "Failed to unlock SDE Accounts"
                                    isErr = True
                                    Errmlmssg = Errmlmssg + "\n" + "Failed to unlock SDE Accounts"
                            break

                    logFile.write ("\n" + "*----------Processing layer %s" % fc)

                    # Find data type
                    desc = arcpy.Describe(localGDB + "\\" + fc)
                    dataType = desc.dataType.upper()

                    # strip off _NEW suffix
                    fc = str(fc.split(NEW)[0])

                    #Check for Alias
                    aliasName = fc
                    aliasCheck = aliasExists(fc)
                    if aliasCheck[0]:
                        # Find dataset if exists
                        if dataType == "FEATURECLASS" or dataType == "ANNOTATION":
                            layerInfo = getDataset(fc)
                            dataset = layerInfo[0] + '_old'
                            fc = dataset + "\\" + schema + aliasCheck[1]
                            aliasName = dataset + "\\" + aliasName
                        else:
                            fc = schema + aliasCheck[1]

                    localFC = localGDB + "\\" + fc
                    aliasFC = localGDB + "\\" + aliasName
                    print("localFC = " + localFC)
                    print("aliasFC = " + aliasFC)

                    #Delete any existing backup layer
                    if arcpy.Exists(aliasFC + BAK):
                        try:
                            logFile.write ("\n" + "Deleting backup for %s" % aliasFC)
                            arcpy.Delete_management(aliasFC + BAK)
                        except:
                            logFile.write ("\n" + "Unable to delete backup layer for %s" % aliasFC)
                            numErrors = numErrors + 1
                            isErr = True
                            Errmlmssg = Errmlmssg + "\n" + "Unable to delete backup layer for %s" % aliasFC
                            continue

                    #Delete any existing backup2 layer created when baks are locked out
                    if arcpy.Exists(aliasFC + BAK2):
                        try:
                            logFile.write ("\n" + "Deleting backup for %s" % aliasFC)
                            arcpy.Delete_management(aliasFC + BAK2)
                        except:
                            logFile.write ("\n" + "Unable to delete backup2 layer for %s" % aliasFC)
                            numErrors = numErrors + 1
                            isErr = True
                            Errmlmssg = Errmlmssg + "\n" + "Unable to delete backup2 layer for %s" % aliasFC
                            continue

                    # Rename any existing layer to bak
                    #Rename to bk2 if bak rename fails
                    #If SDX source, use sde command line
                    #Database Connections\jupiter_5151_sangis.sde\W_DAM
                    if arcpy.Exists(localFC):
                        try:
                            logFile.write ("\n" + "Renaming to backup for %s to %s" % (localFC, aliasFC + BAK))
                            arcpy.Rename_management(localFC, aliasFC + BAK)
                        except:
                            logFile.write ("\n" + "Unable to rename layer %s to backup" % localFC)
                            numErrors = numErrors + 1
                            isErr = True
                            Errmlmssg = Errmlmssg + "\n" + "Unable to rename layer %s to backup" % localFC
                            #try to use the bk2 suffix
                            try:
                                logFile.write ("\n" + "Renaming to backup for %s to %s" % (localFC, aliasFC + BAK))
                                arcpy.Rename_management(localFC, aliasFC + BAK)
                            except:
                                logFile.write ("\n" + "Unable to rename layer %s to backup2" % localFC)
                                numErrors = numErrors + 1
                                isErr = True
                                Errmlmssg = Errmlmssg + "\n" + "Unable to rename layer %s to backup2" % localFC
                                continue

                    #Rename new layer
                    if arcpy.Exists(aliasFC + NEW):
                        try:
                            logFile.write ("\n" + "Renaming new %s to %s" % (aliasFC + NEW,localFC))
                            arcpy.Rename_management(aliasFC + NEW, localFC)
                        except:
                            logFile.write ("\n" + "Unable to rename new %s to %s" % (aliasFC + NEW,localFC))
                            numErrors = numErrors + 1
                            continue

                    #Get Role and Grant Access to group list
                    userRole = getUserRole(fc)
                    role = userRole[0]
                    try:
                        arcpy.ChangePrivileges_management(localFC, role, 'GRANT')
                        msg = arcpy.GetMessages()
                        logFile.write ("\n" + msg)
                    except:
                        logFile.write ("\n" + 'Unable to GRANT to %s for %s' % (role,localFC))

                    if arcpy.Exists(aliasFC + BAK):
                        try:
                            arcpy.ChangePrivileges_management(aliasFC + BAK, role, 'REVOKE')
                            msg = arcpy.GetMessages()
                            logFile.write ("\n" + msg)
                        except Exception, e:
                            print e
                            logFile.write ("\n" + 'Unable to REVOKE to %s for %s' % (role,aliasFC+BAK))

                    if arcpy.Exists(aliasFC + BAK2):
                        try:
                            arcpy.ChangePrivileges_management(aliasFC + BAK2, role, 'REVOKE')
                            msg = arcpy.GetMessages()
                            logFile.write ("\n" + msg)
                        except Exception, e:
                            print e
                            logFile.write ("\n" + 'Unable to REVOKE to %s for %s' % (role,aliasFC+BAK2))

                    #Update Layer Timestamp
                    print(fc)
                    updateDate(fc, updateTableSrc)

                    endTime = datetime.datetime.today()
                    mins, secs = divmod((endTime - startTime).seconds, 60)
                    hours, mins = divmod(mins, 60)
                    #add the FC to the list of deployed FCs
                    deployedFCs.append(fc)
                    logFile.write ("\n" + "*----------Finished layer %d, total elapsed time = %d:%02d:%02d" % (numLayers, hours, mins, secs))
                    logFile.write ("\n" + "*----------*")
                    logFile.write ("\n" + "*----------*")
                    logFile.write ("\n" + "*----------*")

            #unlock database before switching schemas
            if forceLockout:
                #Unlock all users
                try:
                    arcpy.AcceptConnections(adminGDB, True)
                    logFile.write ("\n" + "-----------------%s unlocked---------------" % localGDB)
                except:
                    raise Exception, "Failed to unlock SDE Accounts"
                    isErr = True
                    Errmlmssg = Errmlmssg + "\n" + "Failed to unlock SDE Accounts"

        endTime = datetime.datetime.today()
        mins, secs = divmod((endTime - startTime).seconds, 60)
        hours, mins = divmod(mins, 60)

        logFile.write ("\n" + "-----------------Finished Update Job---------------")
        logFile.write ("\n" + "*----------Number of layers processed = %d" % numLayers)
        logFile.write ("\n" + "*----------Number of layer errors = %d" % numErrors)
        logFile.write ("\n" + "*----------Total elapsed time = %d:%02d:%02d" % (hours, mins, secs))

        if isErr:
            EmailText = "ERROR in DEVELOPMENT-Deploy" + "\n" + Errmlmssg + "\n" "The following layers were deployed:" + "\n"
            for fcs in deployedFCs:
                EmailText = EmailText + fcs + "\n"
            Subject = "DEVELOPMENT-Deploy Process had Errors"
        else:
            EmailText = "DEVELOPMENT-Deploy Successful" + "\n" + str(numLayers) + " Layers Processed" + "\n"
            for fcs in deployedFCs:
                EmailText = EmailText + fcs + "\n"
            Subject = "Successful DEVELOPMENT-Deploy Process"


        message = """\
From: %s
To: %s
Subject: %s

%s
""" % (sender, ", ".join(receivers), Subject, EmailText)

        try:
            smtpObj = smtplib.SMTP('smtp-out.sannet.gov')
            smtpObj.sendmail(sender,receivers, message)
        except:
            logFile.write ("\n" + "!!! EMAIL NOT SENT")
            logFile.write ("\n" + message)

    except Usage, err:
        #in case the process bombs without unlocking the db
        if forceLockout:
            #Unlock all users
            try:
                arcpy.AcceptConnections(adminGDB, True)
                logFile.write ("\n" + "-----------------%s unlocked---------------" % localGDB)
            except:
                raise Exception, "Failed to unlock SDE Accounts"
                isErr = True
                Errmlmssg = Errmlmssg + "\n" + "Failed to unlock SDE Accounts"
        logFile.write ("\n" + err.msg)
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())
    logFile.close()
