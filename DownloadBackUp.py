#-------------------------------------------------------------------------------
# Name:        loadSanGISToSDE
# Purpose:      Download and unzip fcs from Sangis and copy older matching fcs
#               from SDE data wharehouse on atlas to a local fgdb for backup
#
# Author:      DDowling
#
# Created:     26/09/2013
# Copyright:   (c) DDowling 2013
#-------------------------------------------------------------------------------

import arcpy, sys, os, getopt, ConfigParser, ftp_get_fgdbs
#import logging.config
import datetime, time
import smtplib
import emailsender

# Setup Logging
logFile = open("D:\BATCH_FILES\ADR\GDBLoad_LOG.txt", "wb")
layerlogFile = open("D:\BATCH_FILES\ADR\GDBLoad_LAYERLOG.txt", "wb")

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
updateTableSDE = config.get('paths', 'updateTable')
updateTableNameSangis = config.get('paths', 'updateTableName')
updateTableSrc = ''
fGDB = config.get('paths', 'fGDB')
importNewLayers = parseBoolString(config.get('settings', 'importNewLayers'))
forceLayerLoad = parseBoolString(config.get('settings', 'forceLayerLoad'))
useFileList = config.get('settings', 'useFileList')


maxLayers = parseIntString(config.get('settings', 'maxLayers'))
maxMinutes = parseIntString(config.get('settings', 'maxMinutes'))



def getLayerList(updateTableSrc):
    # Compare update date of source & target for updateable layers
    layerList = []
    try:
        """Assumes the manifest matches the zipped layers"""
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
    """ USed when we have a list of FC to update, not the manifest table"""
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
def checkstatus():
    status = true
    if maxLayers > 0:
        if loadCount > maxLayers:
            loadCount = loadCount - 1
            logFile.write ("****-----")
            logFile.write ("****Warning: Maximum number of layers to be loaded (%d) exceeded. Process loop stopped." % maxLayers)
            logFile.write ("****-----")
            isErr = True
            Errmlmssg = Errmlmssg + "\n" + "Maximum Layer Count Reached"
            status = false

    elif maxMinutes > 0:
        if minschk > maxMinutes:
            loadCount = loadCount - 1
            logFile.write ("****-----")
            logFile.write ("****Warning: Maximum time exceeded. No more layers will be loaded.")
            logFile.write ("****-----")
            isErr = True
            Errmlmssg = Errmlmssg + "\n" + "Maximum Time Limit Reached"
            status = false
    return status

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

def checkIsNewLayer(fc,updateTableSrc):
    """This function compares the fcs entry in the sangis manifest agains the entry in the citys layers_update_table table. It compares update dates
    To see which is newer and also retures the dataset the fc belogns in as well as the name that the city calls the layer, which is often
    different from what SanGIS calls it"""
    isNew = False
    dataType = "UNKNOWN"
    datasetName = "SDW.CITY.SANGIS"
    cityFCName = ""
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
        print "updatetable = " + updateTableSDE
        theCount = 0
        logFile.write ("\n" + "-->Checking layer %s is new in old table %s" % (fc, updateTableSDE))
        #c1 = arcpy.da.SearchCursor(updateTableSDE, ["RESPONSIBLE_DEPARTMENT", "UPDATE_DATE","Dataset","CITY_LAYER_NAME"], "\"SANGIS_LAYER_NAME\" = '" + str(fc) + "'")
        with arcpy.da.SearchCursor(updateTableSDE, ["RESPONSIBLE_DEPARTMENT", "UPDATE_DATE","Dataset","CITY_LAYER_NAME"], "\"SANGIS_LAYER_NAME\" = '" + str(fc) + "'") as c1:
            for r1 in c1:
                if len(r1[2]) > 0:
                    datasetName = r1[2]
                if len(r1[3]) > 0:
                    cityFCName = r1[3]
                else:
                    cityFCName =str(fc)
                logFile.write ("\n" + "-->Checking layer %s is new in Src table %s" % (fc, updateTableSrc))
                with arcpy.da.SearchCursor(updateTableSrc, ["UPDATE_DATE", "DATA_TYPE", "LAYER_NAME"], "\"LAYER_NAME\" = '" + str(fc) + "'") as c2:### Add fields that should be added to updatTable if new layer ###
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
                                logFile.write ("\n" + "%s will be updated as %s < %s" % (fc,d1, d2))
                            else:
                                logFile.write ("\n" + "%s will not been updated as %s >= %s" % (fc,d1, d2))
                                print "NOTE: " + "%s has not been updated %s >= %s" % (fc,d1, d2)
                        else:
                            logFile.write ("\n" + "[0] is [1] responsibility, and will not be updated".format(fc, dept))

        if theCount == 0: # Layer does not exist in the local table
            # Get the table DPI records for the new FC and use it to update the
            with arcpy.da.SearchCursor(updateTableSrc, ["UPDATE_DATE", "DATA_TYPE", "LAYER_NAME"], "\"LAYER_NAME\" = '" + str(fc) + "'") as c2:
                for r2 in c2:
                    if importNewLayers:
                        isNew = True
                        logFile.write ("\n" + "%s is a new layer" % fc)
                        layerlogFile.write ("\n" + "%s is a new layer" % fc)
                        layerName = r2[2]# Get values from updateTableSource #
                        #UpdateDate = r2[0]# Get values from updateTableSource #
                        cityFCName = layerName #need to set this return variable here as it will be blank
                        with arcpy.da.InsertCursor(updateTableSDE, ["CITY_LAYER_NAME","SANGIS_LAYER_NAME", "Dataset"]) as c3: # Create insert cursor #
                            if r2[1] =="FeatureDataSet":
                                c3.insertRow([layerName, layerName, "SDW.CITY." + r2[2]])# Insert new row #
                            else:
                                c3.insertRow([layerName, layerName, "SDW.CITY.SANGIS"])#
                        logFile.write ("\n" + "Added new record {0} to {1} table".format(layerName, updateTableSDE))
                        layerlogFile.write ("\n" + "Added new record {0} to {1} table".format(layerName, updateTableSDE))
                        #del c3
                        emailsender.main("load_notify@sddpc.org","x","Load Process added layer entries","The load process found a layer from SanGIS. {0}, that is not in the SDE.Layers_Update_Table".format(layerName),"False")
                    else:
                        logFile.write ("\n" + "%s is a new layer, but new layers are excluded per configuration setting" % fc)
        else:
            logFile.write ("%s is does not exist in source manifest table. Cannot load." % fc)

        if isNew:
            try:
                dataType = r2[1]
            except:
                logFile.write ("Error accessing data type field for layer %s. Defaulting to %s. \n" % (fc, FEATURE_CLASS))
                dataType = FEATURE_CLASS
        del c2, r2
        logFile.flush()

        return isNew, dataType, datasetName, cityFCName

    except Exception, err:
        logFile.write ("\n" + "Error checking if {0} is new or was updated.".format(fc) + "\n")
        logFile.write (str(err) + "\n")
        logFile.write (arcpy.GetMessages() + "\n")
        logFile.flush()
        print  "Error checking if {0} is new or was updated.".format(fc)
        #raise "----->Bailing..."

def main():
    layerCount = 0
    tableCount = 0
    loadCount = 0
    loadErrors = 0
    startTime = datetime.datetime.today()
    startCheckTime = time.time()
    strstartTime = startTime.strftime("%I:%M%p on %B %d, %Y")
    # Setup Email major error
    isErr = False
    Errmlmssg = "Load ERRORS: "
    #This table will hold the list of feature classes to be loaded in the deployscript
    mydeploylist = open(deploylist, 'wb')

    logFile.write ("\n" + "-----------------Beginning Update Job---------------")
    logFile.write ("\n" + "Started at:" + strstartTime)
    logFile.write ("\n")
    arcpy.env.overwriteOutput = True
    #Check to see if there is a list of layers in the ini or if not get the manifest
    try:
        if len(useFileList) > 0:
            layerList = getLayerListFromFile(useFileList)
        else:
            #Get the manifest file, either by download or by from the ini
            global fGDB
            if not  fGDB:
                print "start into ftp"
                logFile.write("Begining ftp of manifest")
                fGDB = ftp_get_fgdbs.main(None)
            else:
                arcpy.env.workspace = fGDB
            updateTableSrc = fGDB + '\\' + updateTableNameSangis
            if not arcpy.Exists(updateTableSrc):
                logFile.write ("*********Load Process had fatal Errors.Manifest not found***********")
                emailsender.main("load_notify@sddpc.org","x","Load Process had fatal Errors","Manifest not found","True")
                sys.exit()
            layerList = getLayerList(updateTableSrc)


        #loop through the layers in the updatelist and download them if they are newer. Record their names
        #in a deploy list
        #also backup correspoding older layers from SDE to a local backup fgdb
        for fc in layerList:
            layerCount = layerCount + 1
            endTime = datetime.datetime.today()
            minschk, secs = divmod((endTime - startTime).seconds, 60)
            hours, mins = divmod(minschk, 60)
            logFile.write ("\n" + "*----------Number of layers read = %d, total elapsed time = %d:%02d:%02d" % (layerCount, hours, mins, secs))
            logFile.flush()
            try:
                doLoad, dataType, localDataset, cityFCName = checkIsNewLayer(fc,updateTableSrc)
            except:
                doLoad = False
                loadErrors = loadErrors + 1
                logFile.write ("\n" + "****Error checking if new layer: %s" % fc)
                logFile.flush()
                isErr = True
                Errmlmssg = Errmlmssg + "\n" + "Error checking if new layer: " + fc
            if doLoad:
                loadCount = loadCount + 1
                if checkstatus:

                    print "START DOWNLOADING %s" % fc
                    #FTP & Unzip new FGDB
                    fGDB = ftp_get_fgdbs.main(None,fc)
                    #Check for successful FTP
                    if fGDB == None or not arcpy.Exists(fGDB + "\\" + fc):
                        loadCount = loadCount - 1
                        loadErrors = loadErrors + 1
                        logFile.write ("\n" + "****Failed to download %s*******" % fc)
                        isErr = True
                        Errmlmssg = Errmlmssg + "\n" + "Failed to Download Layer " + fc
                        logFile.flush()
                        continue
                    else:
                        arcpy.env.workspace = fGDB

                    #If feature class or annotation load into feature dataset. Add DS name as part of string
                    dataType = dataType.upper()
                    if dataType == FEATURE_CLASS or dataType == ANNOTATION:
                        localGDB1 = localGDB + "\\" + localDataset
                    else:
                        localGDB1 = localGDB
                    #finsh constructing the fully qualified FC name in SDE
                    catalogName = schema + cityFCName
                    localFC = localGDB1 + "\\" + catalogName

                    #check the SDE FC exists and then compare to the downloaded version
                    if arcpy.Exists(localFC):
                        schemaChecked = checkSchemas(fGDB + '\\' + fc,localFC)
                        if schemaChecked:
                            layerlogFile.write ("\n" + 'Schema check succeeded for %s'% localFC)
                        else:
                            if importNewLayers:
                                schemaChecked = True
                    else:
                        schemaChecked = False
                        logFile.write("\n cannot find SDE version,{0}, of sangis layer: {1} ".format(localFC, fc))
                        logFile.write("\n schema comparison not run ")
                        logFile.flush()

                    if schemaChecked or forceLayerLoad:
                        if not schemaChecked:
                            layerlogFile.write ("\n" + 'Schema check failed  or not run for %s, forcing load based on configuration'% localFC)

                        # Check for existing  layer and copy to backup if it exists, Back it up to the correct DS, if the DS
                        #doesnt exist create it
                        if arcpy.Exists(localFC):
                            try:
                                desc = arcpy.Describe(localFC)
                                if dataType == FEATURE_CLASS or dataType == ANNOTATION:
                                    nameparts = localFC.split("\\")
                                    desc = arcpy.Describe(nameparts[0] + "\\" + nameparts[1]  + "\\" + nameparts[2] + "\\" + nameparts[3])
                                    #print nameparts[0] + "\\" + nameparts[1]  + "\\" + nameparts[2] + "\\" + nameparts[3]
                                    #print ("Dataset Type: {0}".format(desc.datasetType))
                                    if desc.datasetType == "FeatureDataset":
                                        if not arcpy.Exists(Staging_BAK  + "\\" +nameparts[3].split(".")[2]):
                                            arcpy.CreateFeatureDataset_management(Staging_BAK, nameparts[3].split(".")[2], localFC)
                                        #arcpy.CopyFeatures_management(localFC, Staging_BAK + '\\' + nameparts[3].split(".")[2]  + '\\' + cityFCName)
                                        arcpy.Copy_management(localFC, Staging_BAK + '\\' + nameparts[3].split(".")[2]  + '\\' + cityFCName)
                                    else:
                                        #arcpy.CopyFeatures_management(localFC, Staging_BAK + '\\' + cityFCName)
                                        arcpy.Copy_management(localFC, Staging_BAK + '\\' + cityFCName)
                                    mydeploylist.write (fGDB + '\\' + fc  + "\n")
                                    mydeploylist.flush()
                                elif dataType == TABLE:
                                    if arcpy.Exists(Staging_BAK + '\\' + fc):
                                        arcpy.Delete_management(Staging_BAK + '\\' + fc)
                                    arcpy.CopyRows_management(localFC, Staging_BAK + '\\' + fc)
                                    mydeploylist.write (fGDB + '\\' + fc  + "\n")
                                    mydeploylist.flush()
                            except Exception, err:
                                logFile.write ("\n" + 'Failed to either backup {0} or write to deploy list feature class: {1}'.format(localFC, fGDB + '\\' + fc) + "\n")
                                logFile.write (str(err) + "\n")
                                logFile.write (arcpy.GetMessages() + "\n")
                                logFile.flush()

                        else:
                            mydeploylist.write (fGDB + '\\' + fc  + "\n")
                            mydeploylist.flush()
                            # SDE Copy of the feature class not found
                            logFile.write ("\n" + "****Warning*** SDE FC {0} not found in SDE, not backed up".format(localFC))
                            emailsender.main("load_notify@sddpc.org","x","FC in manifest not found in 5150: {0}".format(localFC),"FC in manifest not found in 5150: {0}".format(localFC),"False")

            else:
                logFile.write ("\n" + 'Download failed for %s, did not download from sangis. Might be older ot schema chack failed'% localFC)
                layerlogFile.write ("\n" + 'Schema check failed for %s, did not import'% localFC)
                logFile.flush()
                layerlogFile.flush()

    except Exception, err:
        logFile.write ("*********Load Process had fatal Errors.***********\n")
        logFile.write (str(err))
        logFile.flush()
        emailsender.main("load_notify@sddpc.org","x","Load Process had fatal Errors",str(err),"True")
    finally:
        logFile.close()
        layerlogFile.close()

if __name__ == '__main__':
    main()
