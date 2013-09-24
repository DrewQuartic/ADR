# ---------------------------------------------------------------------------
# lockout_5150.py
# Created on: Tues July 7 2009
#
# 08/06/2013 - Bill Corey
#              Replaced logging with writing to text file
# ---------------------------------------------------------------------------
# Import system modules
import sys, traceback, cx_Oracle, ConfigParser
#import logging.config

# Setup Logging
#logging.config.fileConfig("GDBDeploy_logging.config")
#logger = logging.getLogger("GDBDeploy")

logFile = open("D:\BATCH_FILES\ADR\GDBDeploy_LOG.txt", "a")


# Read Configuration File
config = ConfigParser.ConfigParser()
config.read('GDBDeploy.ini')
                    
# Local variables...
credentials = config.get('connections', 'credentials')
                 
def main(lock = None):
    if lock == None:
        # Get the parameter value
        lock = sys.argv[1]
        #lock = False

    curs = ''
    orcl = ''
    db = credentials.split('@')[1]
    try:
        if lock:
            logFile.write ("\n" + "Locking database " + db + "...")
        else:
            logFile.write ("\n" + "Unlocking database " + db + "...")
            
        orcl = cx_Oracle.connect(credentials)
        curs = orcl.cursor()

        if lock:
            curs.callproc("sys.lockout")
            msg  = "SDE Connections Locked for " + db
        else:
            curs.callproc("sys.unlock_accounts")
            msg = "SDE Connections Enabled for " + db
            
        logFile.write (msg)
        return ""
    
    except:
        info = sys.exc_info()
        tb = info[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n" + str(info[0]) + "\n" + str(info[1]) + "\n"

        logFile.write (pymsg)
        return "Error"

    finally:
        if type(curs).__name__ == 'OracleCursor':
            curs.close()
        if type(orcl).__name__ == 'Connection':
            orcl.close()

if __name__ == "__main__":
    sys.exit(main())      
