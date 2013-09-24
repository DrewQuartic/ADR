#**********************************************************************
# File name: Unzip.py
# Description:
#    Unzips the contents of a zip file into a new folder, file geodatabase or ArcInfo
#    workspace. If the zip file contains a file geodatabase, the output workspace name should
#    be given a .gdb extension.
# Arguments:
#  0 - Input zip file
#  1 - Output location that will contain the new workspace
#  2 - The name of the new workspace
#
# Created by: ESRI
# Modified by: Chris Pyle, SDDPC
#                -Removed Geoprocessing dependencies
#**********************************************************************

# Import modules
import sys, os, traceback, zipfile
#import logging
#import logging.config

#logging replaced with writing to txt logfile
#logging.config.fileConfig("logging.conf")
#logger = logging.getLogger("GDBLoad")
logFile = open("D:\BATCH_FILES\ADR\GDBLoad_LOG.txt", "a")

# Function for unzipping the contents of the zip file
def unzip(infile, path): #removed 'logger'
    logFile.write ("\n" + "-----------------Starting Unzip---------------")
    logFile.write ("\n" + "infile="+infile+" "+"path="+path)
    try:
        logFile.write ('first infile')
        logFile.write (infile)
        isdir = os.path.isdir

        # If the output location does not yet exist, create it
        if not isdir(path):
            os.makedirs(path)

##        logger.debug('Timo')
##        logger.debug(infile)
##        logger.debug('After infile')
##        mode = 'r'
##        p = 'D:\\Data\\IN\\SANGIS'
##        ## infile = 'manifest.zip'
##        ff = path + '\\\\'+ infile
        #zf = zipfile.ZipFile(path + '\\\\'+ infile,'r')
        zf = zipfile.ZipFile(infile,'r')
        zf.extractall(path)

    except:
        # Return any python specific errors
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        #pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"
        pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + str(sys.exc_info) + "\n"
        logFile.write ("\n" + "!!!!!!!!!!!! Failed to Unzip %s !!!!!!!!!!!!!" % infile)
        logFile.write (pymsg)

    logFile.write ("\n" + "-----------------Finished Unzipping %s ---------------" % infile)

def main(infile = None, outloc = None): # Removed 'inlogger = None'

    #    logger = None
    #    if inLogger == None:
    #        logger = logging.getLogger(sys.argv[1])
    #    else:
    #        logger = inLogger

        if infile == None:
            # Get the tool parameter values
##            infile = sys.argv[2]
##            outloc = sys.argv[3]
            logFile.write (infile)
            logFile.write (infile)
            logFile.write (outloc)

        unzip(infile, outloc) #Removed 'logger'


if __name__ == "__main__":
    sys.exit(main())
