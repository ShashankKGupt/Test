import sys
import os
import re
import logging
import logging.handlers
import tarfile
import time
import shutil
import ConfigParser
import subprocess
import tempfile
import traceback
from datetime import date,datetime,timedelta
from time import mktime

config = None

class LockFile:
    def __init__(self, parFile):
        self.myLockFile = None
        if os.path.exists(parFile):
            lockfile = open(parFile, "r")
            thePid = lockfile.read()
            lockfile.close()
            if os.path.exists("/proc/%s" % thePid):
                raise Exception("Failed to acquire lockfile!")
            else:
                os.remove(parFile)

        self.myLockFile = parFile
        lockfile = open(self.myLockFile, 'w')
        lockfile.write(str(os.getpid()))
        lockfile.close()

    def __del__(self):
        if self.myLockFile != None:
            os.remove(self.myLockFile)

CONFIG_FILE="/etc/opt/EABcfBackup/backup.cfg"
LOGFILE="/tmp/cfbackup.log"
HOST_NAME=(os.uname()[1]).split(".")[0]
BACKUP_DIR = "/tmp/backup"
BASE_DIR="/opt/EABcfBackup/"
CONFIG_DIR="/etc/opt/EABcfBackup/"
MYNAME="EABcfBackup"
LOCKFILE="/var/lock/%s.lck" % MYNAME

HANDLERS_DIRS = [BASE_DIR+"lib/", BASE_DIR+"handlers/", BASE_DIR+"sub/"]

#Use the first alternative for handlers directory that exists
HANDLERS_DIR = HANDLERS_DIRS[0]
for d in HANDLERS_DIRS:
    if os.path.isdir(d):
        HANDLERS_DIR = d
        break

theDebugMode = False
try:
    if os.environ["BACKUP_DEBUG"]:
        print "Debug mode"
        theDebugMode = True
        CONFIG_FILE="backup.cfg"
except:
    #Don't care
    pass

if not os.path.isdir(HANDLERS_DIR):
    HANDLERS_DIR="sub"
else:
    #Set include path for modules
    sys.path.append("/opt/EABcfBackup/")
    sys.path.append(HANDLERS_DIR)

from BackupHelper import BackupHelper
from PrologHandlerBase import PrologHandlerBase

def check_call_noout(params):
    FNULL = open('/dev/null', 'w')
    p = subprocess.Popen(params, stdout=FNULL, stderr=FNULL)
    sts = os.waitpid(p.pid, 0)[1]
    FNULL.close()
    sts = sts >> 8
    if sts != 0:
        raise subprocess.CalledProcessError(sts, params)

def createBackupDir(parBase):
    """
    This function will create a backup directory for the current date and iteration in the base path
    @return: the path to the created backup directory
    """
    num = 0
    theBackupDir =""
    while(True):
        theBackupDir = parBase+"/"+time.strftime("%Y%m%d_%H%M%S")+"."+str(num)
        if not os.path.isdir(theBackupDir):
            break
        num = num+1
    try:
        os.makedirs(theBackupDir)
    except:
        logging.critical("Error: Unable to create destination backup directory: "+theBackupDir)
        sys.exit(1)
    return theBackupDir

def findPrologHandlers(path):
    """
    Find any prolog agents (subclasses of PrologHandlerBase) in the given path
    @return: a sorted list if class instances for the prolog agents
    """
    return _findHandlers(path, PrologHandlerBase)

def _findHandlers(path, handlerType):

    subclasses=[]

    def look_for_subclass(modulename,handlerType):
        logging.debug("searching %s" % (modulename))
        module = __import__(modulename)

        #walk the dictionaries to get to the last one
        d=module.__dict__
        for m in modulename.split('.')[1:]:
            d=d[m].__dict__

        #look through the dictionary
        for key, value in d.items():
            if key == handlerType.__name__:
                continue

            try:
                if issubclass(value, handlerType):
                    logging.debug("Found subclass: "+key)
                    m = module.create(logging, config)
                    subclasses.append(module.create(logging, config))
            except TypeError:
                #this happens when a non-type is passed in to issubclass.
                #We don't care as it can't be a subclass of Job if it isn't a type
                continue

    for root, dirs, files in os.walk(path):
        for name in files:
            if name.endswith(".py") and (not name.startswith("__")) and os.path.exists(os.path.join(root,name)):
                modulename = name.rsplit('.', 1)[0]
                look_for_subclass(modulename,handlerType)

    subclasses = sorted(subclasses, key=lambda handlerType: handlerType.priority)
    return subclasses

def getPathByDev(parDev):
    base = "/dev/disk/by-path/"
    links = os.listdir(base)
    for link in links:
        path = "%s%s" % (base, os.readlink("%s/%s" % (base, link)))
        path = os.path.abspath(path)
        if path.lower() == parDev.lower():
            return "%s%s" % (base, link)
    return None


"""
This function will build the host information part of the TOC file
"""
def getSystemData():
    blkid = {}
    tocLines = []

    def findDevFromUUID(parUUID):
        for dev in blkid:
            if dev.startswith("/dev/vx/"):
                continue #Skip veritas devices
            if blkid[dev]["UUID"] == parUUID:
                return dev
        return None

    def getDevParitions(parDev):
        proc = subprocess.Popen(["/sbin/parted", "-m", "-s", parDev, "unit", "B", "print"], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        theLabel = "msdos"
        for line in proc.stdout.readlines():
            #1      512B   488MB   488MB   primary  ext4         boot
            data = line.split(":")
            if data[0].startswith("/"):
                theLabel = data[5]
            elif data[0].isdigit():
                size  = data[3][0:len(data[3])-1]
                start = data[1][0:len(data[1])-1]
                end   = data[2][0:len(data[2])-1]
                flags = data[6].rstrip(';').rstrip().rstrip(";")
                if len(flags.strip()) == 0:
                    flags = "N/A"
                tocLines.append(parDev+"-part"+data[0]+" PHY "+parDev+" %s %s %s %s %s\n" % (start, end, size, theLabel, flags))

    def getExtAttributes(parDev):
        retval = {}
        proc = subprocess.Popen(["/sbin/tune2fs", "-l", parDev], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        for line in proc.stdout.readlines():
            m = re.match("^(.+):\s+(.*)$", line.strip())
            if m:
                retval[m.group(1)] = m.group(2)
        return retval

    logging.debug("getSystemData - Get UUIDs")
    try:
        proc = subprocess.Popen(["/sbin/blkid", "-c", "/dev/null"], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        for line in proc.stdout.readlines():
            m = re.match("^([^:]+):\s+.*UUID=\"([^\"]+)\"\s+TYPE=\"([^\"]+)", line)
            if m:
                blkid[m.group(1)] = {"UUID":m.group(2), "type":m.group(3)}
    except:
        logging.critical("Running blkid failed.")

    #Filesystems
    logging.debug("getSystemData - Get Filesystems")
    tocLines.append("\n#Filesystems\n")
    fp = open("/etc/fstab", 'r')
    for line in fp.readlines():
        m = re.match("^([^\s]+)\s+([^\s]+)\s+([^\s]+)", line)
        if m:
            dev = m.group(1)
            if dev.startswith("/") or dev.startswith("UUID="):
                theDevice = m.group(1)
                thePath   = m.group(2)
                if (not thePath.endswith("/")) and thePath.lower() != "swap":
                    thePath = thePath+"/"
                m2 = re.match("UUID=([^\s]+)", theDevice)
                if m2:
                    theDevice = findDevFromUUID(m2.group(1))
                if theDevice:
                    theDevicePath = theDevice
                    if theDevice.startswith("/dev/sd"):
                        theDevicePath = getPathByDev(theDevice)
                    if blkid.has_key(theDevice):
                        theMountOptions="N/A"
                        if "ext" in blkid[theDevice]["type"]:
                            #Read mount options from the superblock
                            superblock = getExtAttributes(theDevice)
                            if superblock.has_key("Default mount options"):
                                if superblock["Default mount options"] != "(none)":
                                    theMountOptions = superblock["Default mount options"].replace(" ", ",")
                        tocLines.append(thePath.ljust(15) + " " + theDevicePath.ljust(25) + " " + blkid[theDevice]["type"] + " " + blkid[theDevice]["UUID"]+" "+theMountOptions+"\n")

    #Devices Data
    logging.debug("getSystemData - Get Devices")
    tocLines.append("\n#Devices\n")
    proc = subprocess.Popen(["/sbin/lvm", "pvs", "--nohead"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in proc.stdout.readlines():
        m = re.match("^\s*([^\s\d]+)", line)
        if m:
            theDev = getPathByDev(m.group(1))
            getDevParitions(theDev)
    proc = subprocess.Popen(["/sbin/lvm", "lvs", "--nohead"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in proc.stdout.readlines():
        m = re.match("^\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)", line)
        if m:
            tocLines.append("/dev/mapper/"+m.group(2)+"-"+m.group(1) + "    LVM    " + m.group(2) + "    N/A  N/A  " + m.group(4) + "    N/A    N/A\n")

    #LVM Data
    logging.debug("getSystemData - Get LVM")
    tocLines.append("\n#LVM\n")
    proc = subprocess.Popen(["/sbin/lvm", "pvs", "--nohead"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in proc.stdout.readlines():
        m = re.match("^\s*([^\s]+)\s+([^\s]+)", line)
        if m:
            tocLines.append(m.group(2) + " " +getPathByDev(m.group(1))+"\n")
    logging.debug("getSystemData - Done")
    return tocLines



def saveSysInfo(parFile,helper):
    fp = open(parFile, "w")
    fp.write("System information regarding $(uname -n)\n")
    fp.write("\n");
    fp.write("When restoring this backup the same hardware needs to be available\n")
    fp.write("otherwise it is likely that problems will occur.\n")
    fp.write("######################################################################\n")
    fp.write("\n#Backup TOC\n")
    for toc in helper.getTocList():
      fp.writelines(toc)
    fp.writelines(getSystemData())
    fp.close()


def ignoreBrokenLinks(dir, files):
    theFilesToIgnore  = []
    for file in files:
        if not os.path.exists(os.path.join(dir, file)):
            theFilesToIgnore.append(file)
    return theFilesToIgnore



######################################################
## Main
######################################################
def main(argv=sys.argv):
    global config
    config = ConfigParser.RawConfigParser()
    config.optionxform = str
    config.read(CONFIG_FILE)

    theBackupDir = ""
    theBackupBaseDir = ""

    #Set locale environemtn so that we know messages will be in English
    #And that we will get a decimal point, not a decimal comma
    theLocale = "en_US.UTF-8"
    os.environ['LANG'] = theLocale
    os.environ['LC_NUMERIC'] = theLocale
    os.environ['LC_MESSAGES'] = theLocale

    #Try to aquire a lock file
    try:
        theLockFile = LockFile(LOCKFILE)
    except:
        logging.critical("Aborting: Lockfile already exists!")
        return 1

    try:
        os.remove(LOGFILE)
    except:
        #Not really anything we can do about it
        pass

    logging.basicConfig(filename=LOGFILE,level=logging.DEBUG)
    err_handler = logging.StreamHandler()
    err_handler.setLevel(logging.ERROR)
    logging.getLogger().addHandler(err_handler)
    helper = BackupHelper(logging,config)

    try:
        theBackupMethod = config.get('Backup', 'method')
        theBackupDevice = config.get('Backup', 'device')
        #compress
        theKeepOnlyOne = config.get('Backup', 'keepOnlyOneBackup')
        theNoOfDaysRetain = config.get('Backup', 'noOfDaysRetain')
        if theNoOfDaysRetain == "Always":
            theNoOfDaysRetain = None
        else:
            theNoOfDaysRetain = int(theNoOfDaysRetain)
    except:
        if theBackupMethod == "none":
            logging.info("Aborting: No backup made as the method is none")
            return 0
        if not theNoOfDaysRetain or not isinstance(theNoOfDaysRetain, int):
            logging.critical("Aborting: noOfDaysRetain should be integer or Always")
            return 1
        logging.critical("Aborting: Failed to read configuration file")
        return 1

    #Identify the backup method used local_tape, net_filesystem or none
    if theBackupMethod == "local_tape":
        logging.info("Backup method is local_tape using device "+theBackupDevice)
        theBackupDir = tempfile.mkdtemp()
        config.set("Backup", "BackupDir", theBackupDir)
        try:
            check_call_noout(["/bin/mt", "-f", theBackupDevice, "rewind"])
        except:
            logging.critical("No backup made as there is no tape in drive")
            return 1
    elif theBackupMethod == "net_filesystem":
        logging.info("Backup method is net_filesystem using device "+theBackupDevice)
        theBackupBaseDir = theBackupDevice
        if theKeepOnlyOne.lower() == "true":
            theBackupDevice = theBackupDevice+"/temp"
            os.system("/bin/rm -rf %s" % theBackupDevice)
            os.system("/bin/mkdir -p -m700 %s" % theBackupDevice)
        if theKeepOnlyOne.lower() == "false" and theNoOfDaysRetain:
            d = int(mktime((datetime.now() - timedelta(days=theNoOfDaysRetain)).timetuple()))
            os.chdir(theBackupBaseDir)
            for folder in os.listdir(theBackupBaseDir):
                 if os.path.isdir(folder):
                     if os.path.getmtime(folder) < d:
                         shutil.rmtree(folder)

        theBackupDir = createBackupDir(theBackupDevice)
        config.set("Backup", "BackupDir", theBackupDir)
    elif theBackupMethod == "none":
        logging.info("Aborting: No backup made as the method is none")
        return 0
    else:
        logging.critical("Aborting: No backup made as the method is not 'local_tape', 'net_filesystem' or 'none'")
        return 1

    try:
        logging.info("Filesystem backup started at "+time.strftime("%Y-%m-%d %H:%M:%S"))
        logging.info("Creating backups in "+ theBackupDir)


        #Find backup handlers
#        theBackupHandlers = findBackupHandlers(HANDLERS_DIR)


        #Write backup information file
        saveSysInfo(theBackupDir+"/BackupInfo.txt",helper)


        #Copy restore tools to the backup
        theSubDir = HANDLERS_DIR
        theRestoreScript = BASE_DIR+"/bin/restore"
        theHelperScript = BASE_DIR+"/bin/RestoreHelper.py"
        theBackupHelperScript = BASE_DIR+"/bin/BackupHelper.py"
        theBootBackupHelperScript = BASE_DIR+"/bin/BootBackupHelper.py"
        theConfFile = CONFIG_DIR+"/backup.cfg"
        shutil.copy(theRestoreScript, theBackupDir+"/restore")
        shutil.copy(theHelperScript, theBackupDir+"/RestoreHelper.py")
        shutil.copy(theBackupHelperScript, theBackupDir+"/BackupHelper.py")
        shutil.copy(theConfFile, theBackupDir+"/backup.cfg")
        shutil.copytree(theSubDir, theBackupDir+"/sub", False, ignoreBrokenLinks)

        #Save passwd and groups files
        #They are required during restore in order to be able to restore ACLs
        os.mkdir(theBackupDir+"/etc/")
        shutil.copy("/etc/passwd", theBackupDir+"/etc/passwd")
        shutil.copy("/etc/group", theBackupDir+"/etc/group")

        #Find prolog handlers
        thePrologHandlers = findPrologHandlers(HANDLERS_DIR)

        #Create the prolog files
        for prolog in thePrologHandlers:
            output = prolog.generatePrologOuput()
            if( isinstance(output, dict) ):
                for fileName in output.iterkeys():
                    outputFile = open("%s/%s"%(theBackupDir,fileName),'a')
                    outputFile.write( output[fileName] )
                    outputFile.close()
            else:
                logging.info("PrologHandler \"%s\" returned a non dict() from generatePrologOuput()"%prolog.name)

        #Save restore scripts etc. to the tape
        if theBackupMethod == "local_tape":
            os.chdir(theBackupDir)
            check_call_noout(["/bin/tar", "--acls", "--selinux", "-czf", config.get('Backup', 'device'), ".", "--exclude", "\"lost+found\""])
            os.chdir("/")
            shutil.rmtree(theBackupDir)
            theBackupDir = tempfile.mkdtemp()
            config.set("Backup", "BackupDir", theBackupDir)

        #Create the backups
        helper.doBackup()

        #finalize log and write last segment to tape if using tape
        logging.info("Filesystem backup ended at "+time.strftime("%Y-%m-%d %H:%M:%S"))
    except:
        logging.critical("Backup failed.")
        logging.critical(sys.exc_info()[1])
        logging.critical("Trace:\n%s" % "".join(traceback.format_tb(sys.exc_info()[2])))
        return 1
    finally:
        #save the backup logs outside the backup, in this case on disk under /var/log
        shutil.copy( LOGFILE, "/var/log/cfbackup"+"_"+HOST_NAME+"_"+time.strftime("%Y%m%d_%H%M%S")+".log" )

    shutil.move(LOGFILE, theBackupDir+"/backup.log")
    if theBackupMethod == "local_tape":
        #TODO write the log to the tape
        os.chdir(theBackupDir)
        check_call_noout(["/bin/tar", "--acls", "--selinux", "-czf", config.get('Backup', 'device'), ".", "--exclude","\"lost+found\""])
        os.chdir("/")
        shutil.rmtree(theBackupDir)

        #Rewind
        logging.info("Rewinding and ejecting tape.")
        try:
            subprocess.check_call(["/bin/mt", "-f", theBackupDevice, "rewind"])
            subprocess.check_call(["/bin/mt", "-f", theBackupDevice, "offline"])
        except:
            logging.critical("Failed to rewind and eject the tape")
            return 2
    elif theBackupMethod == "net_filesystem":
        logging.info("Backup method is net_filesystem using device "+theBackupDevice)
        if theKeepOnlyOne.lower() == "true":
            # Fix the directories if we should only have one backup stored on remote disk
            os.system("mv -f %s/saved/* %s/ >/dev/null 2>&1" % (theBackupBaseDir, theBackupDevice))
            os.system("mv -f %s %s/saved/" % (theBackupDir, theBackupBaseDir))

if __name__ == "__main__":
    sys.exit(main())
[root@dlmnnaf01 cron]#
