# ---------------------------------------------------------------------------
# Script:     GDBSend.py
# Created on: July 26, 2013
# Author:     Steve Hossack - Quartic Solutions LLC
#
# ---------------------------------------------------------------------------
import arcpy, os, shutil, datetime, zipfile, glob
from ftplib import FTP

# Database Connections\city@atlasdev.sde
# Database Connections\\SHossack@atlasdev.sde
# use the SHossack connection until 'city' account has been
# granted read privileges to SDW.dbo.VW_LAYER_LAST_UPDATED
source_location = 'Database Connections\\City@atlasdev.sde'
layer_list_view = 'SDW.dbo.VW_LAYER_LAST_UPDATED'
sde_db_prefix = 'SDW.CITY.'
staging_location = 'D:\\Data\\OUT'
destination_location = ''
no_days_since_create = 10
output_manifest = 'LAYERS_UPDATE_TABLE'
ftp_host = "66.121.127.166"
ftp_user = "ctygis"
ftp_password = "iEgIj1p5r"
ftp_output_folder = "/sdeadr/SDCity/In"
log_file = "GDBSend.log"

def zipdir(path,zip,gdbName,lf):
    i = 1
    for root, dirs, files in os.walk(path):
        for file in files:
            #print "Filename: " + file + ", No: " + str(i)
            try:
                zip.write(os.path.join(root, file),gdbName + file)
            except Exception, e:
                print "Error trying to write to zip file " + gdbName
                print e
                lf.write("[" + str(datetime.datetime.now()) +"]- " + "Error trying to write to zip file " + gdbName + '\n')
                lf.write("[" + str(datetime.datetime.now()) +"]- ERROR:" + str(e) + '\n')
            i += 1

            #zip.write(file)
def zipFGDB(fDir,fName,lgF):
    zip = zipfile.ZipFile(fDir + fName + '.zip', 'w')
    zipdir(fDir + fName + ".gdb",zip,fName + ".gdb\\",lgF)



def deleteAllStuffInStaging(strLoc):
    for f in os.listdir(strLoc):
        fPath = os.path.join(strLoc,f)
        try:
            if os.path.isfile(fPath):
                os.remove(fPath)
            else:
                shutil.rmtree(fPath)
        except Exception, e:
            print e
    #shutil.rmtree(strLoc)

def createFileGDB(fgdbName, fgdbLoc):
    arcpy.CreateFileGDB_management(fgdbLoc,fgdbName,"CURRENT")

def layerExists():
    print "still working"

def buildFCPath(fcLoc, fcFDataset, fcName):
    fcDTrim = fcFDataset.strip()
    outPath = ""
    if (len(fcDTrim) == 0):
        outPath = fcLoc + "\\" + sde_db_prefix + fcName
    else:
        outPath = fcLoc + "\\" + fcFDataset + "\\" + sde_db_prefix + fcName

    return outPath

def main():

    startTime = datetime.datetime.now()
    logfile = open(log_file,'a')
    logfile.write('\n')
    logfile.write("==============START: GDBZipAndShip.py script start time: " + str(startTime) + '\n')

    deleteAllStuffInStaging(staging_location)
    logfile.write("[" + str(datetime.datetime.now()) +"]- " + "Deleted old files" + '\n')

    # read view
    fc = source_location + '\\' + layer_list_view
    fieldList = arcpy.ListFields(fc)
    valueList = []

    try:
        print fc
        #rows = arcpy.SearchCursor(r'Database Connections\SHossack@atlasdev.sde\SDW.dbo.VW_LAYER_LAST_UPDATED')
        whereClause = 'days_since_create <= ' + str(no_days_since_create)
        whereClause += ' AND replicate_sangis = ' + "'Y'"
        rows = arcpy.SearchCursor(fc,whereClause)
        #print "ran query: " + whereClause
        logfile.write("[" + str(datetime.datetime.now()) +"]- " + "Successfully ran query: " + whereClause + '\n')
        row = rows.next()

        while row:
            #print row.name + " was last updated " + str(row.days_since_create) + " days ago"
            fcPath = buildFCPath(source_location,row.Dataset,row.name)
            tempFGDBPath = staging_location + row.name + ".gdb"
            if (arcpy.Exists(fcPath) == True) and (arcpy.Exists(tempFGDBPath) == False) :

                #print "found a layer: " + fcPath
                logfile.write("[" + str(datetime.datetime.now()) +"]-" + " found a layer: " + fcPath + '\n')
                createFileGDB(row.name,staging_location)
                print fcPath
                #print staging_location + row.name + ".gdb"
                logfile.write("[" + str(datetime.datetime.now()) +"]- " + "writing: " + staging_location + row.name + ".gdb" '\n')
                print row.name
                arcpy.FeatureClassToFeatureClass_conversion(fcPath,staging_location  + row.name + ".gdb", row.name)
                tempDict = {}
                for f in fieldList:
                    #print f.name
                    tempDict[f.name] = row.getValue(f.name)
                valueList.append(tempDict)
                zipFGDB(staging_location,row.name,logfile)



            else:
                #print "layer not found: " + fcPath + ", OR layer already exists as a FGDB in: " + tempFGDBPath
                logfile.write("[" + str(datetime.datetime.now()) +"]- " + "ERROR: layer not found: " + fcPath + ", OR layer already exists as a FGDB in: " + tempFGDBPath + '\n')

            #print fcPath + str(arcpy.Exists(fcPath))
            row = rows.next()

        print output_manifest + " " + staging_location
        createFileGDB(output_manifest,staging_location)
        print staging_location + output_manifest + ".gdb"
        logfile.write("[" + str(datetime.datetime.now()) +"]- " + "creating manifest: " + staging_location + output_manifest + ".gdb" + '\n')
        arcpy.CreateTable_management(staging_location + output_manifest + ".gdb",output_manifest,fc)

        rows = arcpy.InsertCursor(staging_location + output_manifest + ".gdb\\" + output_manifest)

        for recDict in valueList:
            row = rows.newRow()
            for f in fieldList:
                if f.name <> "OBJECTID":
                    row.setValue(f.name,recDict[f.name])
            rows.insertRow(row)

        # clean up
        del row
        del rows

        print 'ready to zip'
        # zip them up
        zip = zipfile.ZipFile(staging_location + output_manifest + '.zip', 'w')
        zipdir(staging_location + output_manifest + ".gdb",zip,output_manifest + ".gdb\\",logfile)
        zip.close()


        # send to sangis ftp (pass the biscuits)
        # open ftp connection
        ftp = FTP(ftp_host)
        ftp.login(ftp_user,ftp_password)
        ftp.set_pasv(False)
        ftp.cwd(ftp_output_folder)

        for zf in glob.glob(staging_location + '*.zip'):


            #ftpTXFRFile = open(staging_location + output_manifest + '.zip', 'rb')
            #print "sending " + zf + " via ftp!"
            logfile.write("[" + str(datetime.datetime.now()) +"]- " + "sending " + zf + " via ftp!" + '\n')

            ftpTXFRFile = open(zf, 'rb')
            ftp.storbinary('STOR ' + os.path.basename(zf), ftpTXFRFile)
            ftpTXFRFile.close()


        ftp.quit()
        del ftp

        # we got er done
        endTime = datetime.datetime.now()
        mins, secs = divmod((endTime - startTime).seconds, 60)
        hours, mins = divmod(mins, 60)
        print "*FINISH----------Total elapsed time = %d:%02d:%02d" % (hours, mins, secs)
        logfile.write("[" + str(endTime) +"]- " + "FINISH----------Total elapsed time = %d:%02d:%02d" % (hours, mins, secs) + '\n')
        logfile.close()
    except Exception, e:
            print e
if __name__ == "__main__":
    main()