#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      ddowling
#
# Created:     16/10/2013
# Copyright:   (c) ddowling 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import arcpy, ConfigParser, emailsender
config = ConfigParser.ConfigParser()
config.read("D:\BATCH_FILES\ADR\GDBLoad.ini")
SDEGDB = config.get('paths', 'localGDB')
AdminGDB = config.get('paths', 'AdminGDB')
Staging_NEW = config.get('paths', 'Staging_NEW')
Staging_BAK = config.get('paths', 'Staging_BAK')
updateTable = config.get('paths', 'updateTable')
arcpy.env.overwriteOutput = True
class CityBackupLoad(object):
    """Class that will search atlas for FCs matching thoses in the _NEW fgdb and back
    them up to the _BAK fgdb. It will then load the files in the _NEW fgdb to atlas """


    def backup(self, DataToBackup):
        """Class that will backup old fc to staging _BAK fgdb before the load of new ones
        them up to the _BAK fgdb. It will then load the files in the _NEW fgdb to atlas accepts a tuple of [featuredataset, FC]"""
        try:

            result = False
            fdsName = DataToBackup[0]
            itemName = DataToBackup[1]
            expression = arcpy.AddFieldDelimiters(updateTable, 'CITY_LAYER_NAME') + " = '" + itemName + "'"
            loadCounter = 0
            with arcpy.da.SearchCursor(updateTable,['Dataset','RESPONSIBLE_DEPARTMENT','CITY_LAYER_NAME'], where_clause=  expression) as cursor:
                for row in cursor:
                    #Check if exists in SDE
                    loadCounter += 1
                    if arcpy.Exists(SDEGDB + "\\" + row[0] + "\\" + itemName):
                        print "found in SDE"
                        #check if already exists in the backup fgdb and if so delete and then make a new copy of FC in SDE
                        if arcpy.Exists(Staging_BAK + "\\" + fdsName + "\\" + itemName):
                            print "found in _BAK"
                            desc = arcpy.Describe(Staging_BAK + "\\"  + fdsName + "\\" + itemName)
                            if desc.dataType == "FeatureClass" or desc.dataType == "Table" or desc.dataType == "Annontation":
                                arcpy.Delete_management(Staging_BAK + "\\" + fdsName + "\\" + itemName)
                                print arcpy.GetMessages()
                            elif desc.dataType == "FeatureDataset":
                                rc_list = [c.name for c in arcpy.Describe(Staging_BAK + "\\" + itemName).children if c.datatype == "RelationshipClass"]
                                for rc in rc_list:
                                    rc_path = Staging_BAK + "\\" + rc
                                    des_rc = arcpy.Describe(rc_path)
                                    destination = des_rc.destinationClassNames
                                    for item in destination:
                                        arcpy.Delete_management(Staging_BAK  + "\\" +  item)
                                arcpy.Delete_management(Staging_BAK + "\\" + itemName)
                    #Copy the SDE version to the _BAK fgdb
                    try:
                        arcpy.Copy_management(SDEGDB + "\\"+ row[0] + "\\" + itemName, Staging_BAK + "\\"  + fdsName + "\\" + itemName )
                        print "Backed up by Copy " + itemName
                        #print arcpy.GetMessages()
                        result = True
                    except arcpy.ExecuteError:
                        spatialerror =  arcpy.GetMessages(2).find("ERROR 000260")
                        if spatialerror > -1:
                            try:
                                print arcpy.GetMessages()
                                logFile.write("failed Copy and past import of {0} due to spatial domain differences. Attempting an import".format(loaditem))
                                arcpy.FeatureClassToFeatureClass_conversion(SDEGDB + "\\"+ row[0] + "\\" + itemName, Staging_BAK + "\\"  + fdsName , itemName)
                                print "Backed up by FC2FC" + itemName
                                result = True
                            except Exception, err:
                                print str(err)
                    except Exception, err:
                        print str(err)
            #If no record found in the update_table then skip backup and send email warning
            if loadCounter == 0:
                emailsender.main("load_notify@sddpc.org","x","City Deploy problem with FDS:{0}, DS:{1}".format(fdsName,itemName),"No entry for {0}\\{1} found in City Update table. Please add one before it can be updated.".format(fdsName,itemName),"False")
        except Exception, err:
            print str(err)
            emailsender.main("load_notify@sddpc.org","x","City Deploy problem with FDS:{0}, DS:{1}".format(fdsName,itemName),str(err),"True")
        finally:
            return result


    def load(self,DataToLoad):
        """Class that will load the files in the _NEW fgdb to atlas, accepts a tuple of [featuredataset, FC]"""
        fdsName = DataToLoad[0]
        itemName = DataToLoad[1]
        loadCounter = 0
        expression = arcpy.AddFieldDelimiters(updateTable, 'CITY_LAYER_NAME') + " = '" + itemName + "'"
        with arcpy.da.SearchCursor(updateTable,['Dataset','RESPONSIBLE_DEPARTMENT','CITY_LAYER_NAME'], where_clause=  expression) as cursor:
            for row in cursor:
                #Check if exists in SDE
                loadCounter += 1
                if arcpy.Exists(SDEGDB + "\\" + row[0] + "\\" + itemName):
                    print "found in SDE"
                    arcpy.Delete_management(SDEGDB + "\\" + row[0] + "\\" + itemName)
                try:
                    arcpy.Copy_management(Staging_NEW + "\\" + fdsName + "\\" + itemName, SDEGDB + "\\" + row[0] + "\\" + itemName)
                    print "LoadedBy by Copy " + itemName
                except  arcpy.ExecuteError:
                    arcpy.FeatureClassToFeatureClass_conversion(Staging_NEW + "\\" + fdsName + "\\" + itemName, SDEGDB + "\\" + row[0], itemName)
                    print "Loaded by FC2FC" + itemName
                except Exception, err:
                    print err
        if loadCounter ==0:
            emailsender.main("load_notify@sddpc.org","x","City Deploy problem with FDS:{0}, DS:{1}".format(fdsName,itemName),"No entry for {0}\\{1} found in City Update table. Please add one before it can be updated.".format(fdsName,itemName),"False")

    def __init__(self):
        self.data = []