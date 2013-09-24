# ---------------------------------------------------------------------------
# Script:     ftp_get_fgdbs.py
# Created on: October 9 2009
# Author:     Chris Pyle, SDDPC
#
# 11219/2010 - Dave Bishop
#              Since this can be called from either GDBLoad or GDBDeploy,
#              changes were made to make it more adaptable to either process.
#              Changed argument list. Arg 1: logger, Arg 2: optional file to download.
#              Created ftp.ini rather than use GDBLoad.
# --------------------------------------------------------------------------
from ftplib import FTP
import sys, os, traceback, ConfigParser
import logging.config
#import unzip_exe
import unzip_file
from os.path import join

#logging.config.fileConfig("logging.conf")
#logger = logging.getLogger("GDBLoad")

# Read Configuration File
config = ConfigParser.ConfigParser()
config.read("D:\\BATCH_FILES\\ADR\\ftp.ini")
# Test Site: ftp://ftp.sannet.gov/IN/e8p/adr_gen2_test/
ftp_host = config.get('ftp', 'ftp_host')
ftp_usr = config.get('ftp', 'ftp_usr')
ftp_pwd = config.get('ftp', 'ftp_pwd')
ftp_path = config.get('ftp', 'ftp_path')
ftp_manifest = config.get('ftp', 'ftp_manifest')
ftp_download_path = config.get('ftp', 'ftp_download_path')
ftp_download_dir = config.get('ftp', 'ftp_download_dir')

def RemoveDir(rm_dir, logger):
    if os.path.exists(rm_dir):
        logger.debug("removing contents from %s" % rm_dir)
        for root, dirs, files in os.walk(rm_dir, topdown=False):
            for name in files:
                os.remove(join(root, name))
                for name in dirs:
                    os.rmdir(join(root, name))
        logger.info("removed contents from %s" % rm_dir)

        try:
            logger.debug("removing %s" % rm_dir)
            os.removedirs(rm_dir)
            logger.info("removed %s" % rm_dir)
        except:
            logger.info("Unable to remove %s" % rm_dir)

def main(inLogger = None, inManifest = None):
    logger = None
    try:

        if inLogger == None:
            #logger = logging.getLogger(sys.argv[1])
            logging.config.fileConfig("GDBLoadDeploy_logging.config")
            logger = logging.getLogger("GDBftp_get_fgdbs")
        else:
            logger = inLogger

        if inManifest is None:
            if len(sys.argv) > 2:
                basename = sys.argv[2]
            else:
                basename = ftp_manifest
        else:
            basename = inManifest

        logger.info("-----------------Beginning FTP Job---------------")

        #Open Connection
        print ftp_host
        print ftp_usr
        print ftp_pwd
        print ftp_path

        try:
            ftp = FTP(ftp_host)
            ftp.connect(ftp_host, 21,3600)

        except Exception, e:
            print e
        # Add try block to test for correct login exit code:
        #'230 Anonymous access granted, restrictions apply.'
        ftp.login(ftp_usr, ftp_pwd)
        ftp.cwd(ftp_path)

        # set passive to false or it bombs  added 8/12/2013
        ftp.set_pasv(False)

        print "changed ftp dir to " + ftp_path

        filename = basename + '.zip'
        outfolder = ftp_download_path + '\\' + ftp_download_dir
        filepath = outfolder + '\\' + filename
        outfilepath = outfolder + '\\' + basename + '.gdb'

        print "filename: " + filename
        print "outfolder: " + outfolder
        print "filepath: " + filepath
        print "outfilepath: " + outfilepath

        if os.path.exists(outfolder):
            print "outfolder exists: " + outfolder
            try:

                print ftp.nlst()
                if filename in ftp.nlst():
                    # Open the file for writing in binary mode
                    print 'Opening local file ' + filepath
                    f = open(filepath, 'wb')

                    ftp.retrbinary('RETR ' + filename, f.write)
                    f.close()
                    del f
                else:
                    print "File %s not found on FTP host" % filename
                    logger.info("File %s not found on FTP host" % filename)

                    return None
            except Exception, e:
                print e
        else:
            logger.info("Invalid download directory %s" % outfolder)
            print "Invalid download directory %s" % outfolder
            return None

        # Close & release FTP object
        ftp.quit()
        print "FTP QUIT"
        del ftp
        try:
            if os.path.exists(filepath):
                # Remove existing gdb
                if os.path.exists(outfilepath):
                    RemoveDir(outfilepath, logger)

                # unzip2.main(filepath, ftp_download_path, ftp_download_dir)
                logger.info("Extracting %s" % filepath)
                # unzip_exe.main(logger, filepath, outfolder)
                unzip_file.main( filepath, outfolder)

            else:
                logger.info("Unable to download file %s" % filename)
                print "Unable to download file %s" % filename
                return None
            # Check that file exists
            if os.path.exists(outfilepath):
                logger.info("-----------------Finished FTP Job---------------")
                return outfilepath
            else:
                logger.info("Failed to unzip download file: " + filepath)
                print "Failed to unzip download file: " + filepath
                return None
        except Exception, e:
            print e

    except:
        #logger.debug('FTP_get_fgdbs Error:')
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        #pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"
        pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n    " + str(sys.exc_info) + "\n"
        #logger.debug(pymsg)

        return None

if __name__ == "__main__":
    sys.exit(main())
