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
import arcpy, CityBackUpLoad, ConfigParser
config = ConfigParser.ConfigParser()
config.read("D:\BATCH_FILES\ADR\GDBLoad.ini")
SDEGDB = config.get('paths', 'localGDB')
AdminGDB = config.get('paths', 'AdminGDB')
Staging_NEW = config.get('paths', 'Staging_NEW')
Staging_BAK = config.get('paths', 'Staging_BAK')
updateTable = config.get('paths', 'updateTable')
arcpy.env.overwriteOutput = True
def main():
    ldr = CityBackUpLoad.CityBackupLoad()
    arcpy.env.workspace = Staging_NEW
    fds = arcpy.ListDatasets()

    for fd in fds:
        fcs = arcpy.ListFeatureClasses(feature_dataset=fd )
        for fc in fcs:
            target =[]
            target.append(fd)
            target.append(fc)
            if ldr.backup(target):
                ldr.load(target)


if __name__ == '__main__':
    main()
