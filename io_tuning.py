#! /usr/bin/python

import ConfigParser
import logging, logging.handlers
import os
import re
import sys
import subprocess

# Regular expressions to match LUN characteristics

lunMatch = (
    # regex transfer, readahead, sched, schedOpts
    # test u, data
    ( re.compile(r'[gG].*[0-9]A[0-9]+D'),
      8192, 16384, 'noop', () ),
    # test u, metadata
    ( re.compile(r'[gG].*[0-9][AT][0-9]+M'),
      1024, 2048, 'noop', () ),
    # test DBHome, data
    ( re.compile(r'[gG].*[0-9]DBH[0-9]+D'),
      1024, 2048, 'noop', () ),
    # test DBHome, metadata
    ( re.compile(r'[gG].*[0-9]DBH[0-9]+M'),
      1024, 2048, 'noop', () ),
    # test BGSYS, data
    ( re.compile(r'[gG].*[0-9]BGS[0-9]+D'),
      1024, 2048, 'noop', () ),
    # test BGSYS, metadata
    ( re.compile(r'[gG].*[0-9]BGS[0-9]+M'),
      1024, 2048, 'noop', () ),
    #( re.compile(r'[gG].*[0-9]A[0-9]+A'),
    #  8192, 8192, 'deadline', (16,) ),
    # test admin
    ( re.compile(r'[gG].*[0-9]A[0-9]+A'),
      8192, 16384, 'noop', () ),
    # sb, data
    ( re.compile(r'[gG].*[0-9][dD][sS][Bb][0-9]+'),
      4096, 8192, 'deadline', (32,) ),
    # sb, metadata
    ( re.compile(r'[gG].*[0-9][mM][sS][Bb][0-9]+'),
      4096, 8192, 'deadline', (16,) ),
    # lb, data
    ( re.compile(r'[gG].*[0-9][dD][lL][Bb][0-9]+'),
      8192, 16384, 'deadline', (32,) ),
    # lb, metadata
    ( re.compile(r'[gG].*[0-9][mM][lL][Bb][0-9]+'),
      4096, 8192, 'deadline', (16,) ),
    
    )

################################################################

class BlockDeviceOps(object):
    """Collect common operations on block devices."""
    # FIXME: Derive a class for Linux, have an abstract base class.

    @classmethod
    def set_io_variable(cls, dev, var, val):
        """Set a I/O elevator variable on the device."""
        import os.path, glob

        # Find the device under /sys/block.  We may need to resolve
        # symlinks.
        dev = os.path.realpath(dev)
        key = os.path.basename(dev)
        if not os.path.isdir('/sys/block/' + key):
            logging.warn('Unable to manipulate I/O tunable %s for %s',
                         var, dev)
            return
        # We need to manage slave settings first.
        for s in glob.iglob('/sys/block/'+key+'/slaves/*'):
            cls.set_io_variable(s, var, val)

        # Now set it for the master.
        sbname = '/sys/block/'+key+'/'+var
        with open(sbname, 'w') as f:
            try:
                f.write(str(val))
            except:
                logging.warn('Failed to update %s to %s: %s',
                             sbname, val, sys.exc_value)
            else:
                logging.info('Update %s to %s', sbname, val)

    @classmethod
    def set_io_scheduler(cls, dev, sched):
        """Set the I/O scheduler"""
        cls.set_io_variable(dev, 'queue/scheduler', sched)

    @classmethod
    def set_io_transfer_size(cls, dev, s):
        """Set the I/O transfer size to the device"""
        cls.set_io_variable(dev, 'queue/max_sectors_kb', s)

    @classmethod
    def set_io_readahead_size(cls, dev, s):
        """Set the I/O readahead size to the device"""
        cls.set_io_variable(dev, 'queue/read_ahead_kb', s)

    @classmethod
    def set_io_deadline_fifo_batch(cls, dev, s):
        """Set the deadline scheduler fifo batch size."""
        cls.set_io_variable(dev, 'queue/iosched/fifo_batch', s)


################################################################

class IOTuner(object):

    def __init__(self, logger, cFileName):
        """Initialize IOTuner object."""
        self._logger = logger
        self._lunMatch = []
        cf = ConfigParser.SafeConfigParser()
        cf.read(cFileName)
        self.compile_tuning(cf)

    def compile_tuning(self, cf):
        """Compile the tuning information."""
        for s in cf.sections():
            try:
                rexp = cf.get(s, 'regex')
            except ConfigParser.NoOptionError:
                self._logger.error('Section "%s" missing "regex" option', s)
                continue
            try:
                r = re.compile(rexp)
            except:
                self._logger.error('Section "%s" invalid "regex" option', s)
                continue
            try:
                transfer = cf.getint(s, 'transfer')
            except ConfigParser.NoOptionError:
                self._logger.info('Section "%s" missing "transfer" option', s)
                transfer = 512
            except ValueError:
                self._logger.error('Section "%s", "transfer" option must be integer', s)
                continue
            try:
                readahead = cf.getint(s, 'readahead')
            except ConfigParser.NoOptionError:
                self._logger.info('Section "%s" missing "readahead" option', s)
                readahead = 2*transfer
            except ValueError:
                self._logger.error('Section "%s", "readahead" option must be integer', s)
                continue
            try:
                scheduler = cf.get(s, 'scheduler')
            except ConfigParser.NoOptionError:
                self._logger.info('Section "%s" missing "scheduler" option', s)
                scheduler = None
            opts = []
            if scheduler == 'deadline':
                try:
                    fifo_batch = cf.getint(s, 'fifo_batch')
                except ConfigParser.NoOptionError:
                    self._logger.info('Section "%s" missing "fifo_batch" option', s)
                    fifo_batch = None
                except ValueError:
                    self.logger.error('Section "%s", "fifo_batch" option must be integer', s)
                    continue
                opts = [ fifo_batch, ]
            self._lunMatch.append( ( r, transfer, readahead, scheduler, opts ) )
                

def collect_device_lun_SM():
    """Collect a map of device names to LUNs, using SMdevices"""

    devlun = {}

    r = re.compile( r'\s*(/dev/\S+).*Logical Drive\s+(\S+),' )
    P = subprocess.Popen('SMdevices', shell=True,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         close_fds=True)
    co,ci,ce = P.stdout, P.stdin, P.stderr
    for L in co:
        m = r.match(L)
        if m:
            devlun[m.group(1)] = m.group(2)

    return devlun

def process_multipath_devices(devlun):
    """Go through the list of multipath devices and tune I/O."""

    global lunMatch

    rwnn = re.compile( r'^(?P<wwn>[0-9a-f]+)\s+(?P<dev>dm-\S+)\s' )
    rscsi = re.compile( r'^\|.*\s+(?P<scsi>sd\S+)\s+' )

    dmwnn = {}
    dmlun = {}
    lundev = None
    wnn = None
    
    P = subprocess.Popen('multipath -ll', shell=True,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         close_fds=True)
    co,ci,ce = P.stdout, P.stdin, P.stderr
    for L in co:
        M = rwnn.match(L)
        if M:
            # We have a line giving wwn and device name.
            lundev = M.group('dev')
            wnn =  M.group('wwn')
            continue
        M = rscsi.match(L)
        if M and lundev is not None:
            s = M.group('scsi')
            try:
                lun = devlun[os.path.join('/dev', s)]
            except KeyError:
                pass
            else:
                dmwnn[lundev] = wnn
                dmlun[lundev] = lun
                lundev = wnn = None

    # Now go through the devices we have located.  Match LUN names,
    for dm,lun in dmlun.items():
        for r,transfer,readhead,sched,schedopts in lunMatch:
            if r.match(lun):
                BlockDeviceOps.set_io_scheduler(dm, sched)
                BlockDeviceOps.set_io_transfer_size(dm, transfer)
                BlockDeviceOps.set_io_readahead_size(dm, readhead)
                if sched == 'deadline':
                    fifobatch, = schedopts
                    BlockDeviceOps.set_io_deadline_fifo_batch(dm, fifobatch)

def usage():
    sys.stdout.write('''%s OPTIONS

    OPTIONS are:

    -h,--help          Display this help
    -c,--config=FNAME  Use FNAME as the configuration file
    -l,--log=LNAME     Use LNAME as a log file (otherwise use syslog)
    -v,--verbose       Verbose logging\n''' % (sys.argv[0],))

def main():
    import getopt
    
    # Parse the command line.
    try:
        opts,args = getopt.getopt(sys.argv[1:],
                                  "h?c:l:v", [
                                      "help","config=","log=","verbose"])
    except getop.GetoptError, err:
        sys.stderr.write(sys.argv[0]+": "+str(err))
        usage()
        sys.exit(1)
    cfile = None
    lfile = None
    verbosity = 0
    for o,a in opts:
        if o in ('-h','-?','--help'):
            usage()
            sys.exit(0)
        elif o in ('-c','--config'):
            cfile = a
        elif o in ('-l','--log'):
            lfile = a
        elif o in ('-v','--verbose'):
            verbosity += 1
        else:
            assert False,'error in command line options'

    # Set up logging.
    logger = logging.getLogger('io_tuning')
    if lfile is None:
        handler = logging.handlers.SysLogHandler(address='/dev/log')
    elif lfile in ('-', 'stderr'):
        handler = logging.StreamHandler()
    else:
        handler = logging.FileHandler(lfile)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if verbosity >= 2:
        logger.setLevel(Logging.DEBUG)
    elif verbosity >= 1:
        logger.setLevel(Logging.INFO)
        
    # Set up the IO Tuner object.
    iot = IOTuner(logger, cfile)
    return

    # Collect device information.
    devlun = collect_device_lun_SM()

    # Scan through multipath devices and apply attributes.
    process_multipath_devices(devlun)

if __name__ == '__main__':
    main()
