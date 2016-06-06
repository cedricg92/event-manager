import datetime
import fnmatch
import gzip
import shutil
import tarfile
import time

import croniter
import hdfs
from watchdog.events import PatternMatchingEventHandler, FileCreatedEvent, FileModifiedEvent, FileMovedEvent, os, \
    FileSystemEvent


def add_log_str(log_pattern, strlog):
    """Add log in logfile

    :param log_pattern: Pattern of log
    :type log_pattern: str
    :param strlog: Log message
    :type strlog: str
    """
    logfile = log_pattern
    logfile = logfile.replace("%Y", time.strftime("%Y"))
    logfile = logfile.replace("%m", time.strftime("%m"))
    logfile = logfile.replace("%d", time.strftime("%d"))
    logfile = logfile.replace("%H", time.strftime("%H"))
    logfile = logfile.replace("%M", time.strftime("%M"))
    logfile = logfile.replace("%S", time.strftime("%S"))
    with open(logfile, "ab+") as myfile:
        myfile.write(strlog)
        myfile.close()


def add_log(log_pattern, event, filename, destination, event_type, event_subtype, exec_program, args, return_val):
    """Add log in filelog

    :param log_pattern: Pattern of log
    :type log_pattern: str
    :param event: Event Name
    :param filename: File name
    :param destination: Destination
    :param event_type: Event type
    :param event_subtype: Event sub type
    :param exec_program: executable
    :param args: Arguments
    :param return_val: Return Value
    """
    log = time.strftime("%Y-%m-%d %H:%M:%S")
    log += "|" + str(event)
    log += "|" + str(filename)
    log += "|" + str(destination)
    log += "|" + str(event_type)
    log += "|" + str(event_subtype)
    log += "|" + str(exec_program)
    log += "|" + str(args)
    if return_val:
        log += "|" + str(0)
    else:
        log += "|" + str(1)
    log += os.linesep
    add_log_str(log_pattern, log)


class ExecHandler(PatternMatchingEventHandler):
    """Class ExecHandler

    """
    FILE_LOG = ""

    def __init__(self, event_conf):
        """

        :param event_conf: EventConf
        :type event_conf: em.event.EventConf
        :return:
        """
        super(self.__class__, self).__init__(["*"], ["*.err"], True, False)
        self._patterns = event_conf.patterns
        self.event_conf = event_conf

    def is_scheduled(self):
        """Check if the event handler is scheduled

        :return: True if the event handler is scheduled
        :rtype: bool
        """
        return self.event_conf.is_scheduled()

    def on_created(self, event):
        """Function called when file is created

        :param event: File created event
        :type event: FileCreatedEvent
        """
        if isinstance(event, FileCreatedEvent):
            self.process(event)

    def on_modified(self, event):
        """Function called  when file is modified

        :param event: File modified event
        :type event: FileModifiedEvent
        """
        if isinstance(event, FileModifiedEvent):
            self.process(event)

    def on_moved(self, event):
        """Function called when file is moved

        :param event: File moved event
        :type event: FileMovedEvent
        """
        if isinstance(event, FileMovedEvent):
            self.process(event)

    def process(self, event):
        """Function process

        :param event: Event file
        :type event: FileSystemEvent
        :return: Return value (True success, False error)
        :rtype: bool
        """
        if self.event_conf.enabled == 0:
            return 0
        args = str(self.event_conf.get_context_value("execArgs"))
        args = args.replace("%filenale", event.src_path)
        args = args.replace("%destination", self.event_conf.destination)
        exec_dir = os.path.dirname(self.event_conf.get_context_value("execProgram"))
        exec_app = os.path.dirname(self.event_conf.get_context_value("execProgram"))
        ret = os.system("cd "+exec_dir+";./"+exec_app+" "+args)
        add_log(self.FILE_LOG, self.event_conf.name, event.src_path, self.event_conf.destination, self.event_conf.type,
                self.event_conf.subtype, self.event_conf.get_context_value("execProgram"), args, ret)
        if ret is False:
            os.rename(event.src_path, event.src_path + ".err")
        else:
            os.remove(event.src_path)
        return ret

    def check_schedule(self, now):
        """Check if the event should be launched

        :param now: Actual date and time
        :type now: datetime.datetime
        :return: True if the event should be launched
        :rtype: bool
        """
        cron = croniter.croniter(self.event_conf.get_cron(), now)
        current_exec_datetime = cron.get_current(datetime.datetime)
        return (current_exec_datetime.year == now.year and current_exec_datetime.month == now.month and
                current_exec_datetime.day == now.day and current_exec_datetime.hour == now.hour and
                current_exec_datetime.minute == now.minute)


class FsHandler(PatternMatchingEventHandler):
    """File System Handler

    """

    FILE_LOG = ""
    TYPE_MOVE = 1
    TYPE_ARCHIVE = 2
    TYPE_COMPRESS = 3
    TYPE_UNARCHIVE = 4
    TYPE_UNCOMPRESS = 5
    STR_TYPE_MOVE = "move"
    STR_TYPE_ARCHIVE = "archive"
    STR_TYPE_COMPRESS = "compress"
    STR_TYPE_UNARCHIVE = "unarchive"
    STR_TYPE_UNCOMPRESS = "uncompress"

    def __init__(self, event_conf, fs_type):
        """

        :param event_conf: ExecConf
        :type event_conf: em.event.EventConf
        :param fs_type: Process type of Fs handler
        :type fs_type: str
        """
        super(self.__class__, self).__init__(["*"], ["*.tmp", "*.err", "*.run"], True, False)
        self.event_conf = event_conf
        self.fs_type = fs_type
        self.delimiter = os.path.sep

    def is_scheduled(self):
        """Check if the event handler is scheduled

        :return: True if the event handler is scheduled
        :rtype: bool
        """
        return self.event_conf.is_scheduled()

    def on_any_event(self, event):
        if not event.src_path.endswith(".tmp"):
            for pattern in self.event_conf.patterns:
                if fnmatch.fnmatch(os.path.basename(event.src_path), pattern):
                    print(event)

    def on_created(self, event):
        """Handler listener on creation of file

        :param event: File created event
        :type event: FileCreatedEvent
        :return: True if the process run correctly
        :rtype: bool
        """
        if isinstance(event, FileCreatedEvent):
            return self.process(event.src_path)
        return False

    def on_modified(self, event):
        """Handler listener on modification of file

        :param event: File modified event
        :type event: FileModifiedEvent
        :return: True if the process run correctly
        :rtype: bool
        """
        if isinstance(event, FileModifiedEvent):
            return self.process(event.src_path)
        return False

    def on_moved(self, event):
        """Handler listener on move of file

        :param event: File moved event
        :type event: FileMovedEvent
        :return: True if the process run correctly
        :rtype: bool
        """
        if isinstance(event, FileMovedEvent):
            return self.process(event.dest_path)
        return False

    def process(self, full_filename):
        """Function process

        :param full_filename: Full path of filename
        :type full_filename: str
        :return: True if the process run correctly
        :rtype: bool
        """
        if self.event_conf.enabled == 0:
            return True
        if not os.path.exists(full_filename):
            return False
        if os.path.dirname(full_filename) != self.event_conf.directory:
            return False
        res = False
        filename = os.path.basename(full_filename)
        matched = False
        for pattern in self.event_conf.patterns:
            if fnmatch.fnmatch(filename, pattern):
                matched = True
                break
        if not matched:
            return False
        os.rename(full_filename, full_filename + ".run")
        if self.fs_type == FsHandler.TYPE_MOVE or self.fs_type == FsHandler.STR_TYPE_MOVE:
            res = self.process_move(filename, "run")
        elif self.fs_type == FsHandler.TYPE_ARCHIVE or self.fs_type == FsHandler.STR_TYPE_ARCHIVE:
            res = self.process_archive(filename, "run", "tmp")
        elif self.fs_type == FsHandler.TYPE_COMPRESS or self.fs_type == FsHandler.STR_TYPE_COMPRESS:
            res = self.process_compress(filename, "run", "tmp")
        elif self.fs_type == FsHandler.TYPE_UNCOMPRESS or self.fs_type == FsHandler.STR_TYPE_UNCOMPRESS:
            res = self.process_uncompress(filename, "run", "tmp")
        elif self.fs_type == FsHandler.TYPE_UNARCHIVE or self.fs_type == FsHandler.STR_TYPE_UNARCHIVE:
            res = self.process_unarchive(filename, "run")
        add_log(self.FILE_LOG, self.event_conf.name, full_filename, self.event_conf.destination, self.event_conf.type,
                self.event_conf.subtype, "", "", res)
        if not res:
            os.rename(full_filename + ".run", full_filename + ".err")
        return res

    def process_move(self, filename, extension):
        """Move file to destination
        directory -> File -> destination

        :param filename: Filename
        :type filename: str
        :param extension: Extention of file (run)
        :type extension: str
        :return: True if the process run correctly
        :rtype: bool
        """
        if os.path.exists(self.event_conf.destination + self.delimiter + filename):
            os.remove(self.event_conf.destination + self.delimiter + filename)
        os.rename(self.event_conf.directory + self.delimiter + filename + "." + extension,
                  self.event_conf.destination + self.delimiter + filename)
        return os.path.exists(self.event_conf.destination + self.delimiter + filename)

    def process_archive(self, filename, extension, tmp_extension):
        """Create archive file (tar)

        :param filename: Filename
        :type filename: str
        :param extension: Extension of file (run)
        :type extension: str
        :param tmp_extension: Tmp extension
        :type tmp_extension: str
        :return: True if the process run correctly
        :rtype: bool
        """
        if os.path.exists(self.event_conf.destination + self.delimiter + filename + ".tar" + "." + tmp_extension):
            os.remove(self.event_conf.destination + self.delimiter + filename + ".tar" + "." + tmp_extension)
        if os.path.exists(self.event_conf.destination + self.delimiter + filename + ".tar"):
            os.remove(self.event_conf.destination + self.delimiter + filename + ".tar")
        tar = tarfile.open(self.event_conf.destination + self.delimiter + filename + ".tar" + "." + tmp_extension, "w")
        tar.add(self.event_conf.directory + self.delimiter + filename + "." + extension, filename)
        tar.close()
        os.remove(self.event_conf.directory + self.delimiter + filename + "." + extension)
        os.rename(self.event_conf.destination + self.delimiter + filename + ".tar" + "." + tmp_extension,
                  self.event_conf.destination + self.delimiter + filename + ".tar")
        return os.path.exists(self.event_conf.destination + self.delimiter + filename + ".tar")

    def process_compress(self, filename, extension, tmp_extension):
        """Compress file (gzip)

        :param filename: Filename
        :type filename: str
        :param extension: Extension of file (run)
        :type extension: str
        :param tmp_extension: Tmp extension
        :type tmp_extension: str
        :return: True if the process run correctly
        :rtype: bool
        """
        if os.path.exists(self.event_conf.destination + self.delimiter + filename + ".gz"):
            os.remove(self.event_conf.destination + self.delimiter + filename + ".gz")
        if os.path.exists(self.event_conf.destination + self.delimiter + filename + ".gz" + "." + tmp_extension):
            os.remove(self.event_conf.destination + self.delimiter + filename + ".gz" + "." + tmp_extension)
        with open(self.event_conf.directory + self.delimiter + filename + "." + extension, 'rb') as f_in, \
                gzip.open(self.event_conf.destination + self.delimiter + filename + ".gz" + "." + tmp_extension,
                          'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            f_in.close()
            f_out.close()
        os.remove(self.event_conf.directory + self.delimiter + filename + "." + extension)
        os.rename(self.event_conf.destination + self.delimiter + filename + ".gz" + "." + tmp_extension,
                  self.event_conf.destination + self.delimiter + filename + ".gz")
        return os.path.exists(self.event_conf.destination + self.delimiter + filename + ".gz")

    def process_uncompress(self, filename, extension, tmp_extension):
        """Uncompress file (gunzip)

        :param filename: Filename
        :type filename: str
        :param extension: Extension of file (run)
        :type extension: str
        :param tmp_extension: Tmp extension
        :type tmp_extension: str
        :return: True if the process run correctly
        :rtype: bool
        """
        if os.path.exists(self.event_conf.destination + self.delimiter + filename.replace(".gz", "") + "." +
                          tmp_extension):
            os.remove(self.event_conf.destination + self.delimiter + filename.replace(".gz", "") + "." + tmp_extension)
        if os.path.exists(self.event_conf.destination + self.delimiter + filename.replace(".gz", "")):
            os.remove(self.event_conf.destination + self.delimiter + filename.replace(".gz", ""))
        with gzip.open(self.event_conf.directory + self.delimiter + filename + "." + extension) as f_in, \
                open(self.event_conf.destination + self.delimiter + filename.replace(".gz", "") + "." + tmp_extension,
                     "w") as f_out:
            f_out.write(f_in.read())
        os.rename(self.event_conf.destination + self.delimiter + filename.replace(".gz", "") + "." + tmp_extension,
                  self.event_conf.destination + self.delimiter + filename.replace(".gz", ""))
        os.remove(self.event_conf.directory + self.delimiter + filename + "." + extension)
        return os.path.exists(self.event_conf.destination + self.delimiter + filename.replace(".gz", ""))

    def process_unarchive(self, filename, extension):
        """Unarchive file (untar)

        :param filename: Filename
        :type filename: str
        :param extension: Extension of file (run)
        :type extension: str
        :return: True if the process run correctly
        :rtype: bool
        """
        try:
            tar = tarfile.open(self.event_conf.directory + self.delimiter + filename + "." + extension)
            tar.extractall(self.event_conf.destination)
            tar.close()
            os.remove(self.event_conf.directory + self.delimiter + filename + "." + extension)
            return True
        except Exception:
            return False

    def run_schedule(self):
        """Run scheduled event

        :return: None
        """
        for filename in os.listdir(self.event_conf.directory):
            for pattern in self.event_conf.patterns:
                if fnmatch.fnmatch(filename, pattern):
                    exit(self.process(self.event_conf.directory + self.delimiter + filename))
        exit(1)

    def check_schedule(self, now):
        """Check if the event should be launched

        :param now: Actual date and time
        :type now: datetime.datetime
        :return: True if the event should be launched
        :rtype: bool
        """
        cron = croniter.croniter(self.event_conf.get_cron(), now)
        current_exec_datetime = cron.get_current(datetime.datetime)
        return (current_exec_datetime.year == now.year and current_exec_datetime.month == now.month and
                current_exec_datetime.day == now.day and current_exec_datetime.hour == now.hour and
                current_exec_datetime.minute == now.minute)


class HDFSHandler(PatternMatchingEventHandler):
    """HDFS handler

    """
    FILE_LOG = ""

    TYPE_PUT = 1
    TYPE_GET = 2
    STR_TYPE_PUT = "put"
    STR_TYPE_GET = "get"

    def __init__(self, event_conf, hdfs_type):
        """

        :param self:
        :param event_conf: ExecConf
        :type event_conf: em.event.EventConf
        :return:
        """
        super(self.__class__, self).__init__(["*"], ["*.tmp", "*.err", "*.run"], True, False)
        self.event_conf = event_conf
        self.hdfs_type = hdfs_type
        self.delimiter = os.path.sep

    def is_scheduled(self):
        """Check if the event handler is scheduled

        :return: True if the event handler is scheduled
        :rtype: bool
        """
        if not self.event_conf.is_scheduled() and\
                (self.event_conf.subtype == self.STR_TYPE_GET or self.event_conf.subtype == self.TYPE_GET):
            raise ValueError("HDFSHandler: Subtype error - get should be scheduled !")
        return self.event_conf.is_scheduled()

    def on_any_event(self, event):
        if not event.src_path.endswith((".tmp", ".err", ".run")):
            for pattern in self.event_conf.patterns:
                if fnmatch.fnmatch(os.path.basename(event.src_path), pattern):
                    print(event)

    def on_created(self, event):
        """Handler listener on creation of file

        :param event: File created event
        :type event: FileCreatedEvent
        :return: True if the process run correctly
        :rtype: bool
        """
        if isinstance(event, FileCreatedEvent):
            return self.process(event.src_path)
        return False

    def on_modified(self, event):
        """Handler listener on modification of file

        :param event: File modified event
        :type event: FileModifiedEvent
        :return: True if the process run correctly
        :rtype: bool
        """
        if isinstance(event, FileModifiedEvent):
            return self.process(event.src_path)
        return False

    def on_moved(self, event):
        """Handler listener on move of file

        :param event: File moved event
        :type event: FileMovedEvent
        :return: True if the process run correctly
        :rtype: bool
        """
        if isinstance(event, FileMovedEvent):
            return self.process(event.dest_path)
        return False

    def process(self, full_filename):
        """Function process

        :param full_filename: Full path of filename
        :type full_filename: str
        :return: True if the process run correctly
        :rtype: bool
        """
        ret = False
        if self.event_conf.enabled == 0:
            return False
        filename = os.path.basename(full_filename)
        matched = False
        for pattern in self.event_conf.patterns:
            if fnmatch.fnmatch(filename, pattern):
                matched = True
                break
        if not matched:
            return False
        if self.event_conf.is_fs_directory():
            os.rename(full_filename, full_filename + ".run")
        if self.hdfs_type == self.TYPE_PUT or self.hdfs_type == self.STR_TYPE_PUT:
            ret = self.process_put(filename, "run", "err")
        if self.hdfs_type == self.TYPE_GET or self.hdfs_type == self.STR_TYPE_GET:
            ret = self.process_get(filename)
        add_log(self.FILE_LOG, self.event_conf.name, full_filename, self.event_conf.destination, self.event_conf.type,
                self.event_conf.subtype, self.event_conf.get_context_value("hdfsUrl"),
                self.event_conf.get_context_value("hdfsUser"), ret)
        return ret

    def process_put(self, filename, extension, err_extension):
        """Put file to HDFS

        :param filename: Filename
        :type filename: str
        :param extension: extension of file (run)
        :type extension: str
        :param err_extension: error extension (err)
        :type err_extension: str
        :return: True if the process run correctly
        :rtype: bool
        """
        res = None
        try:
            client = hdfs.InsecureClient(self.event_conf.get_context_value("hdfsUrl"),
                                         self.event_conf.get_context_value("hdfsUser"))
            client.upload(self.event_conf.destination + "/" + filename,
                          self.event_conf.directory + self.delimiter + filename + "." + extension, overwrite=True)
            res = client.status(self.event_conf.destination + "/" + filename, False)
        except Exception as e:
            print(e)
        ret = False
        if res is None:
            os.rename(self.event_conf.directory + self.delimiter + filename + "." + extension,
                      self.event_conf.directory + self.delimiter + filename + "." + extension + "." + err_extension)
        else:
            ret = True
            os.remove(self.event_conf.directory + self.delimiter + filename + "." + extension)
        return ret

    def process_get(self, filename):
        """Get file from HDFS

        :param filename: Filename
        :type filename: str
        :return: True if the process run correctly
        :rtype: bool
        """
        ret = False
        try:
            client = hdfs.InsecureClient(self.event_conf.get_context_value("hdfsUrl"),
                                         self.event_conf.get_context_value("hdfsUser"))
            res = client.download(self.event_conf.directory + "/" + filename, self.event_conf.destination, True)
            ret = res == (self.event_conf.destination + os.path.sep +
                          os.path.basename(self.event_conf.directory + "/" + filename))
            if ret:
                client.delete(self.event_conf.directory + "/" + filename)
        except Exception as e:
            print(e.message)
        return ret

    def run_schedule(self):
        """Run scheduled event

        :return: None
        """
        if self.event_conf.subtype == self.STR_TYPE_GET or self.event_conf.subtype == self.TYPE_GET:
            client = hdfs.InsecureClient(self.event_conf.get_context_value("hdfsUrl"),
                                         self.event_conf.get_context_value("hdfsUser"))
            files = client.list(self.event_conf.directory)
            for filename in files:
                for pattern in self.event_conf.patterns:
                    if fnmatch.fnmatch(filename, pattern):
                        exit(self.process(self.event_conf.directory + "/" + filename))
        else:
            for filename in os.listdir(self.event_conf.directory):
                for pattern in self.event_conf.patterns:
                    if fnmatch.fnmatch(filename, pattern):
                        exit(self.process(self.event_conf.directory + self.delimiter + filename))
        exit(1)

    def check_schedule(self, now):
        """Check if the event should be launched

        :param now: Actual date and time
        :type now: datetime.datetime
        :return: True if the event should be launched
        :rtype: bool
        """
        cron = croniter.croniter(self.event_conf.get_cron(), now)
        current_exec_datetime = cron.get_current(datetime.datetime)
        return (current_exec_datetime.year == now.year and current_exec_datetime.month == now.month and
                current_exec_datetime.day == now.day and current_exec_datetime.hour == now.hour and
                current_exec_datetime.minute == now.minute)
