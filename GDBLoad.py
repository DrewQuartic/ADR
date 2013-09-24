# ---------------------------------------------------------------------------
# Script:     GDBLoad.py
# Created on: October 27 2008
# Author:     Chris Pyle, SDDPC
#
# 11/19/2010 - Dave Bishop
#              Added limit checks for time and number of layers,
#              removed registering stuff and everything related to deploy.
# 04/25/2013 - Matthew McSpadden
#              Updated code to ArcGIS 10.1, Python 2.7
#
# 08/06/2013 - Bill Corey
#              Logging replaced with writing to a text file
# ---------------------------------------------------------------------------

import arcpy, sys, os, getopt, ConfigParser, ftp_get_fgdbs
#import logging.config
import datetime, time
import smtplib

# Setup Logging
logFile = open("D:\BATCH_FILES\ADR\GDBLoad_LOG.txt", "a")
layerlogFile = open("D:\BATCH_FILES\ADR\GDBLoad_LAYERLOG.txt", "a")

# Read Configuration File
config = ConfigParser.ConfigParser()
config.read("D:\BATCH_FILES\ADR\GDBLoad.ini")

def parseBoolString(theString):
    parseBoolString = (theString[0].upper()=='T')
    return parseBoolString

def parseIntString(theString):
    try:
        return int(float(theString))
    except:
        return None

# Constants
FEATURE_CLASS = "FEATURECLASS"
ANNOTATION = "ANNOTATION"
TABLE = "TABLE"
NEW = "_NEW"

# Local variables...

localGDB = config.get('paths', 'localGDB')
AdminGDB = config.get('paths', 'AdminGDB')
Staging_NEW = config.get('paths', 'Staging_NEW')
Staging_BAK = config.get('paths', 'Staging_BAK')
deploylist = config.get('paths', 'deploylist')
localDataset = config.get('paths', 'localDataset')
schema = config.get('names', 'schema')
sde_keyword = config.get('names', 'sde_keyword')
responsible_party_str = config.get('names', 'responsible_party_list')
responsible_party_list = responsible_party_str.split(',')
updateTable = config.get('paths', 'updateTable')
updateTableName = config.get('paths', 'updateTableName')
updateTableSrc = ''
fGDB = config.get('paths', 'fGDB')
importNewLayers = parseBoolString(config.get('settings', 'importNewLayers'))
forceLayerLoad = parseBoolString(config.get('settings', 'forceLayerLoad'))
useFileList = config.get('settings', 'useFileList')


maxLayers = parseIntString(config.get('settings', 'maxLayers'))
maxMinutes = parseIntString(config.get('settings', 'maxMinutes'))
maxLayerNameLength = parseIntString(config.get('names', 'maxLayerNameLength'))

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def stripSchema(lName):
    # Check for schema name
    ls = lName.split(".")
    lName= ls[0]
    if len(ls) > 1:
        # Get the Layer name string
        # Assumes SCHEMA.LAYERNAME - change index to 2 for SQL-Server where SDE.SCHEMA.LAYERNAME
        if len(ls) == 2:
            lName = ls[1]
        if len(ls) == 3:
            lName = ls[2]
    return lName

def getLayerList(updateTableSrc):
    # Compare update date of source & target for updateable layers
    layerList = []
    try:
        # Assumes the manifest matches the zipped layers
        cur = arcpy.da.SearchCursor(updateTableSrc, ["LAYER_NAME"])
        theCount = 0
        for row in cur:
            theCount = theCount + 1
            layerName = row[0]
            layerName = str(layerName)
            if "PARCEL" in layerName:
                layerList.insert(0, layerName)
            elif "ROAD" in layerName:
                layerList.insert(0, layerName)
            elif "LOT" in layerName:
                layerList.insert(0, layerName)
            else:
                layerList.append(layerName)
        del cur, row
        return layerList
    except:
        logFile.write ("\n" + "Cannot Create Layer List")
        return []

def getLayerListFromFile(fileName):
    # Compare update date of source & target for updateable layers
    layerList = []
    if os.path.exists(fileName):
        try:
            f = open(fileName, 'r')
            for l in f:
                layerList.append(l.strip())

            return layerList
        except:
            logFile.write ("\n" + "Cannot Create Layer List from File %s" % fileName)
            return []
    else:
        logFile.write ("\n" + "Layer List file % doesn't exist" % fileName)
        return []

def checkIsNewLayer(fc,updateTableSrc):
    isNew = False
    dataType = "UNKNOWN"
    # Check for schema name

    ls = fc.split(".")
    fc = ls[0]
    if len(ls) > 1:
        # Get the Layer name string
        # Assumes SCHEMA.LAYERNAME - change index to 2 for SQL-Server where SDE.SCHEMA.LAYERNAME
        if len(ls) == 2:
            fc = ls[1]
        if len(ls) == 3:
            fc = ls[2]
    try:
        print "fc = " + fc
        print "updatetablesrc = " + updateTableSrc
        print "updatetable = " + updateTable
        theCount = 0
        logFile.write ("\n" + "-->Checking layer %s is new in old table %s" % (fc, updateTable))
        c1 = arcpy.da.SearchCursor(updateTable, ["RESPONSIBLE_DEPARTMENT", "UPDATE_DATE"], "\"SANGIS_LAYER_NAME\" = '" + str(fc) + "'")
        logFile.write ("\n" + "-->Checking layer %s is new in Src table %s" % (fc, updateTableSrc))
        c2 = arcpy.da.SearchCursor(updateTableSrc, ["UPDATE_DATE", "DATA_TYPE", "LAYER_NAME"], "\"LAYER_NAME\" = '" + str(fc) + "'")### Add fields that should be added to updatTable if new layer ###
        for r1 in c1:
            for r2 in c2:
                theCount = theCount + 1

                #Check if layer is updated locally, if so, do not update
                isUpdateable = True
                dept = r1[0]
                if (dept!=None):
                    for p in responsible_party_list:
                        if dept == p:
                            isUpdateable = False
                if (isUpdateable):
                    logFile.write ("\n" + "Is Updateable")
                    dp = r1[1]
                    dpStr = str(dp)
                    dpLst = dpStr.split(' ')
                    dpDate = dpLst[0]
                    dpLst = dpDate.split('-')
                    d1 = datetime.date(int(dpLst[0]),int(dpLst[1]),int(dpLst[2]))
                    dp = r2[0]
                    dpStr = str(dp)
                    dpLst = dpStr.split(' ')
                    dpDate = dpLst[0]
                    dpLst = dpDate.split('-')
                    d2 = datetime.date(int(dpLst[0]),int(dpLst[1]),int(dpLst[2]))
                    if d1 < d2:
                        isNew = True
                        logFile.write ("\n" + "%s was updated %s < %s" % (fc,d1, d2))
                    else:
                        logFile.write ("\n" + "%s has not been updated %s >= %s" % (fc,d1, d2))
                        print "NOTE: " + "%s has not been updated %s >= %s" % (fc,d1, d2)
                else:
                    logFile.write ("\n" + "%s is the subscriber's responsibility, and will not be updated" % fc)
        del c1, r1

        if theCount == 0: # Layer does not exist in the local table
            # Make sure it exists in the source table (Only applies when using a layer list file)
            for r2 in c2:
                if importNewLayers:
                    isNew = True
                    logFile.write ("\n" + "%s is a new layer" % fc)
                    layerlogFile.write ("\n" + "%s is a new layer" % fc)
                    c3 = arcpy.da.InsertCursor(updateTable, ["SANGIS_LAYER_NAME", "UPDATE_DATE"])# Create insert cursor #
                    layerName = r2[2]# Get values from updateTableSource #
                    UpdateDate = r2[0]# Get values from updateTableSource #
                    c3.insertRow([layerName, UpdateDate])# Insert new row #
                    logFile.write ("\n" + "Added new record to %s table" % updateTable)
                    layerlogFile.write ("\n" + "Added new record to %s table" % updateTable)
                    del c3
                else:
                    logFile.write ("\n" + "%s is a new layer, but new layers are excluded per configuration setting" % fc)
            else:
                logFile.write ("%s is does not exist in source manifest table. Cannot load." % fc)

        if isNew:
            try:
                dataType = r2[1]
            except:
                logFile.write ("Error accessing data type field for layer %s. Defaulting to %s." % (fc, FEATURE_CLASS))
                dataType = FEATURE_CLASS
        del c2, r2


        return isNew, dataType

    except:
        logFile.write ("\n" + "Unable to check if %s is new or was updated." % fc)
        print "Unable to check if %s is new or was updated." % fc
        raise "----->Bailing..."
        return isNew, dataType

def checkSchemas(fc, localFC):
    try:
        checkSchemas = True
        inputFields = arcpy.ListFields(fc)
        existingFields = arcpy.ListFields(localFC)
        for inFld, exFld in zip(inputFields,existingFields):
            if inFld.name != exFld.name:
                checkSchemas = False
                break
            if inFld.type != exFld.type:
                checkSchemas = False
                break

        return checkSchemas

    except "checkSchemaError":
        logFile.write ('Unable to checkSchemas for %s and %s' % (fc, localFC))
        return False

def getAlias(localFC):
    aliasName = localFC
    # Check for schema name
    localFC = stripSchema(localFC)
    try:
        theCount = 0
        c1 = arcpy.da.SearchCursor(updateTable,["ALIAS"], "\"SANGIS_LAYER_NAME\" = '" + localFC + "'")
        for r1 in c1:
            theCount = theCount + 1
            rAlias = r1[0]
            if rAlias != None and len(rAlias)>0:
                aliasName = rAlias
        del c1, r1

        if theCount == 0: # Layer does not exist in table
            isNew = True
            logFile.write ("\n" + "%s is a new layer" % localFC)
        return aliasName

    except:
        logFile.write ("\n" + "Unable to check %s for an alias name." % localFC)
        return aliasName

def setAlias(localFC):
    newName = localFC
    localFC = stripSchema(localFC)
    if len(localFC) > maxLayerNameLength:
        i = 0
        n = -2
        newName = localFC[:maxLayerNameLength]
        aliasCheck = aliasExists(newName, localFC)
        while aliasCheck[0]:
            i = i + 1
            if i > 9:
                n = -3
            newName = newName[:n] + "_" + str(i)
            aliasCheck = aliasExists(newName, localFC)
        try:
            if aliasCheck[1]:
                logFile.write ("\n" + "%s is a new layer, but already has an alias name of %s" % (localFC,newName))
            else:
                c1 = arcpy.da.InsertCursor(updateTable,["SANGIS_LAYER_NAME","ALIAS","UPDATE_DATE"])
                c1.insertRow([localFC, newName , "1/1/1999"])

                logFile.write ("\n" + "%s is a new layer" % localFC)
                del c1

            layerlogFile.write ("\n" + "The name of the new layer %s exceeds the max length and was given a short-name alias %s" % (localFC,newName))

            return newName

        except:
            logFile.write ("\n" + "Unable to check %s for an alias name." % localFC)
            return newName
    else:
        return newName

def aliasExists(aliasName, localFC):  # SANGIS.LONG_NAME_ALIAS
    found = False
    reuse = False
    aliasName = stripSchema(aliasName)
    try:
        c1 = arcpy.da.SearchCursor(updateTable, ["SANGIS_LAYER_NAME"], "\"ALIAS\" = '" + aliasName + "'")
        for r1 in c1:
            if r1[0] == localFC:
                reuse = True
            else:
                found = True
                logFile.write ("\n" + "%s is using the alias name %s" % (r1[0], aliasName))

        del c1, r1

        return [found, reuse]

    except:
        logFile.write ("\n" + "Unable to check alias name %s." % aliasName)
        return [found, reuse]

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
        Errmlmssg = "Load ERRORS: "
        sender = 'DEVELOPMENT load_notify@sddpc.org'
        receivers = ['steve@quarticsolutions.com', 'bill@quarticsolutions.com']#, 'timo@quarticsolutions.com'] #'drew@quarticsolutions.com', 'rob@quarticsolutions.com']

        layerCount = 0
        tableCount = 0
        loadCount = 0
        loadErrors = 0
        startTime = datetime.datetime.today()
        startCheckTime = time.time()
        strstartTime = startTime.strftime("%I:%M%p on %B %d, %Y")

        logFile.write ("\n")
        logFile.write ("\n" + "-----------------Beginning Update Job---------------")
        logFile.write ("\n" + "Started at:" + strstartTime)
        logFile.write ("\n")

        # FTP GDB from Publisher
        # FTP speed of SanGIS GDB:
        #   on dpc54015: 394747236 bytes received in 1342.49Seconds 294.04Kbytes/sec
        #   on ganymede: 816464827 bytes received in 1330.45Seconds 613.68Kbytes/sec.

        # If local FGDB is not set in GDBLoad.ini, retrieve manifest from FTP site
        mydeploylist = open(deploylist, 'wb')

        global fGDB
        if len(fGDB)==0:
            print "start into ftp"
            fGDB = ftp_get_fgdbs.main(None)


        if fGDB != None:
            arcpy.env.workspace = fGDB
        else:
            print "FAILED BIG TIME"
            raise Exception, "Failed To Download Manifest"

            isErr = True
            Errmlmssg = Errmlmssg + "\n" + "Failed to Dowload Manifest"
#        fGDB = r"D:\Data\IN\SANGIS\manifest.gdb"
        try:
             updateTableSrc = fGDB + '\\' + updateTableName
        except Exception, e:
             print e
        logFile.write ("\n")
        logFile.write ("\n" + "Update table = %s" % updateTableSrc)


        # Check for Layer Update Date Table in the manifest
        updateDateTbleExists = arcpy.Exists(updateTableSrc)
        if not updateDateTbleExists:
            raise "Layer Update Table s% Doesn't exist" % updateTableSrc
        if len(useFileList) > 0:
            layerList = getLayerListFromFile(useFileList)
        else:
            layerList = getLayerList(updateTableSrc)

        # Try to lock dataset
        #=======================================================================
        # try:
        #    arcpy.DisconnectUser(AdminGDB, "ALL")
        #    arcpy.AcceptConnections(AdminGDB, False)
        #    logFile.write ("\n" + "----------SDE Accounts Locked----------")
        # except:
        #    raise Exception, "Failed To Lockout SDE Accounts"
        #    isErr = True
        #    Errmlmssg = Errmlmssg + "\n" + "Could not lock SDE database"
        #=======================================================================

        #----------Main Loop----------
        # For each layer in the GDB,
        print "BEFORE LOOP LAYER LIST"
        ################################################
        # SHOSSACK: TODO remove this temp list
        ################################################
        #testLayerList = layerList[:10]
        for fc in layerList:
            layerCount = layerCount + 1

            #Check if layer is new or is newer than local copy
            try:
                print "BEFORE CHECK======"
                doLoad, dataType = checkIsNewLayer(fc,updateTableSrc)
                print "DO LOAD = " + str(doLoad)
            except:
                doLoad = False
                loadErrors = loadErrors + 1
                logFile.write ("\n" + "****Error checking if new layer: %s" % fc)
                isErr = True
                Errmlmssg = Errmlmssg + "\n" + "Error checking if new layer: " + fc

            endTime = datetime.datetime.today()
            minschk, secs = divmod((endTime - startTime).seconds, 60)
            hours, mins = divmod(minschk, 60)
            logFile.write ("\n" + "*----------Number of layers read = %d, total elapsed time = %d:%02d:%02d" % (layerCount, hours, mins, secs))

            # If layer is new or is newer than the local copy
            if doLoad:
                loadCount = loadCount + 1
                if maxLayers > 0:
                    if loadCount > maxLayers:
                        loadCount = loadCount - 1
                        logFile.write ("****-----")
                        logFile.write ("****Warning: Maximum number of layers to be loaded (%d) exceeded. Process loop stopped." % maxLayers)
                        logFile.write ("****-----")
                        isErr = True
                        Errmlmssg = Errmlmssg + "\n" + "Maximum Layer Count Reached"
                        break
                elif maxMinutes > 0:
                    if minschk > maxMinutes:
                        loadCount = loadCount - 1
                        logFile.write ("****-----")
                        logFile.write ("****Warning: Maximum time exceeded. No more layers will be loaded.")
                        logFile.write ("****-----")
                        isErr = True
                        Errmlmssg = Errmlmssg + "\n" + "Maximum Time Limit Reached"
                        break


                #If feature class or annotation load into feature dataset.
                dataType = dataType.upper()
                if dataType == FEATURE_CLASS or dataType == ANNOTATION:
                    localGDB1 = localGDB + "\\" + localDataset
                else:
                    localGDB1 = localGDB

                catalogName = schema + str(fc)
                localFC = localGDB1 + "\\" + catalogName

                print "START DOWNLOADING %s" % fc
                #FTP & Unzip new FGDB
                fGDB = ftp_get_fgdbs.main(None,fc)

                #Check for successful FTP
                if fGDB != None:
                    arcpy.env.workspace = fGDB
                    if not arcpy.Exists(fc):
                        loadCount = loadCount - 1
                        loadErrors = loadErrors + 1
                        logFile.write ("\n" + "Failed to download %s" % fc)
                        isErr = True
                        Errmlmssg = Errmlmssg + "\n" + "Failed to Download Layer " + fc
                        continue
                else:
                    loadCount = loadCount - 1
                    loadErrors = loadErrors + 1
                    logFile.write ("\n" + "Failed to download %s" % fc)
                    isErr = True
                    Errmlmssg = Errmlmssg + "\n" + "Failed to Download Layer " + fc
                    continue

                #Check if the layer exists and schemas match
                layerExists = arcpy.Exists(localFC)
                aliasName = str(fc)
                if layerExists:
                    schemaChecked = checkSchemas(fGDB + '\\' + fc,localFC)
                    # Check for alias names (especially for long layer names 30+ chars)
                    aliasName = getAlias(fc)
                else:
                    if importNewLayers:
                        schemaChecked = True
                        # Set an alias for long layer names (30+ chars)
                        aliasName = setAlias(fc)

                if aliasName != str(fc):
                    localFC = localGDB1 + "\\" + schema + aliasName

                if schemaChecked or forceLayerLoad:
                    layerlogFile.write ("\n" + 'Schema check succeeded for %s'% localFC)
                    if not schemaChecked:
                        layerlogFile.write ("\n" + 'Schema check failed for %s, forcing load based on configuration'% localFC)

                    localFC = localFC + NEW
#------------------------------------------Split Here-----------------------------------------------
                    # Delete Existing _NEW layer if it already exists
                    # Check for existing _NEW layer.
                    if arcpy.Exists(localFC):
#                        try:
#                            arcpy.env.workspace = localGDB
#                            logFile.write ("\n" + "Deleting existing %s" % localFC)
#                            arcpy.Delete_management(localFC)
#
#                        except:
                        loadCount = loadCount - 1
                        loadErrors = loadErrors + 1
#                        logFile.write ("\n" + "Unable to delete existing new %s" % localFC)
                        logFile.write ("\n" + "%s already exists. Check GDBDeploy for errors." % localFC)
                        msg = arcpy.GetMessages()
                        logFile.write (msg)
                        isErr = True
#                        Errmlmssg = Errmlmssg + "\n" + "Unable to delete existing NEW layer"
                        Errmlmssg = Errmlmssg + "\n" + "NEW layer already exists. Check GDBDeploy for errors." + localFC
#                        continue


                    arcpy.env.workspace = fGDB

                    if dataType == FEATURE_CLASS or dataType == ANNOTATION:
                        # Copy layer into *_new layer
                        logFile.write ("\n" + "Importing feature class %s" % localFC)
                        logFile.flush()
                        go = True
                        try:
                            arcpy.FeatureClassToFeatureClass_conversion(fGDB + '\\' + fc, localGDB1, aliasName + NEW)
                            mydeploylist.write (fGDB + '\\' + fc  + "\n")
                            mydeploylist.flush()
                            logFile.write ("\n" + "Finished Importing %s" % localFC)
                        except:
                            go = False
                            logFile.write ("\n" + "Unable to import %s" % localFC)
                            msg = arcpy.GetMessages()
                            logFile.write (msg)
                            logFile.flush()
                            isErr = True
                            Errmlmssg = Errmlmssg + "\n" + "Unable to import FC: " + localFC

                        #Analyze new layer if imported
##                        if go:
##                            logFile.write ("\n" + "Analyzing %s" % localFC)
##                            try:
##                                arcpy.Analyze_management(localFC, "BUSINESS")
##                                logFile.write ("\n" + "Finished Analyzing BUSINESS Table for %s" % localFC)
##                            except:
##                                logFile.write ("\n" + "Unable to Analyze BUSINESS Table for %s" % localFC)
##                                msg = arcpy.GetMessages()
##                                logFile.write (msg)

                    elif dataType == TABLE:
                        # Copy table into *_new table
                        logFile.write ("\n" + "Importing table %s" % localFC)
                        logFile.flush()
                        try:
                            arcpy.TableToTable_conversion(fGDB + '\\' + fc, localGDB1, aliasName + NEW)
                            mydeploylist.write (fGDB + '\\' + fc  + "\n")
                            mydeploylist.flush()
                            logFile.write ("\n" + "Finished Importing %s" % localFC)
                            tableCount = tableCount + 1
                        except:
                            logFile.write ("\n" + "Unable to import %s" % localFC)
                            msg = arcpy.GetMessages()
                            logFile.write (msg)
                            logFile.flush()
                            isErr = True
                            Errmlmssg = Errmlmssg + "\n" + "Unable to import table"

                    else:
                        # Don't know what type of data copy to do, so do nothing
                        logFile.write ("\n" + "****Error: unknown data type (%s) for import item %s" % (dataType, localFC))

                # Schema did not match existing schema - log the exception
                else:
                    logFile.write ("\n" + 'Schema check failed for %s, did not import'% localFC)
                    layerlogFile.write ("\n" + 'Schema check failed for %s, did not import'% localFC)

        #----------End of main loop----------

#        try:
#            arcpy.AcceptConnections(AdminGDB, True)
#        except:
#            raise Exception, "Failed to unlock SDE Accounts"
#            isErr = True
#            Errmlmssg = Errmlmssg + "\n" + "Failed to unlock SDE Accounts"

        logFile.write ("\n" + "-----------------Finished Update Job---------------")

        # Layer count is really a count of everything. If tables were found deduct them to get actual layer count
        newTotalCnt = layerCount
        layerCount = layerCount - tableCount
        if layerCount == 0:
            isErr = True
            Errmlmssg = Errmlmssg + "\n" + "No Layers Were Loaded"

        endTime = datetime.datetime.today()
        mins, secs = divmod((endTime - startTime).seconds, 60)
        hours, mins = divmod(mins, 60)
        logFile.write ("\n" + "*----------Number of layers read = %d" % layerCount)
        logFile.write ("\n" + "*----------Number of tables read = %d" % tableCount)
        logFile.write ("\n" + "*----------Number of layers/tables loaded = %d" % loadCount)
        logFile.write ("\n" + "*----------Number of load errors = %d" % loadErrors)
        logFile.write ("\n" + "*----------Total elapsed time = %d:%02d:%02d" % (hours, mins, secs))
        print "done!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        print "*----------Total elapsed time = %d:%02d:%02d" % (hours, mins, secs)


        if isErr:
            EmailText = "ERROR in Load" + "\n" + Errmlmssg
            Subject = "Load Process had Errors"
        else:
            EmailText = "Load Successful" + "\n" + str(newTotalCnt) + " total Layers Loaded"  + "\n"
            Subject = "Successful Load Process"


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
            logFile.write (message)

    except Usage, err:
        logFile.write (err.msg)
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help"
        return 2
        try:
            arcpy.AcceptConnections(AdminGDB, True)
        except:
            raise Exception, "Failed to unlock SDE Accounts"
            isErr = True
            Errmlmssg = Errmlmssg + "\n" + "Failed to unlock SDE Accounts"
    logFile.close()
    layerlogFile.close()

if __name__ == "__main__":
    sys.exit(main())

