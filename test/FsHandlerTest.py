import os
import shutil

from ptest.assertion import assert_true
from ptest.decorator import TestClass, BeforeMethod, Test, AfterMethod
from watchdog.events import FileCreatedEvent

from shirp.event import EventConf
from shirp.handler import FsHandler

MOVE_GROUP = "grp-move"
ARCHIVE_GROUP = "grp-archive"
COMPRESS_GROUP = "grp-compress"
UNARCHIVE_GROUP = "grp-unarchive"
UNCOMPRESS_GROUP = "grp-uncompress"


@TestClass(run_mode="singleline")
class FsHandlerTest:
    def __init__(self, fs_handler=None, event_conf=None):
        """

        :param fs_handler:
        :type fs_handler: FsHandler
        :param event_conf:
        :type event_conf: EventConf
        """
        self.fs_handler = fs_handler
        self.event_conf = event_conf
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.result = False

    @BeforeMethod(group=MOVE_GROUP)
    def before_move_test(self):
        self.event_conf = EventConf(True, "test move", "fs", FsHandler.TYPE_MOVE,
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\in",
                                    ["test_????.txt"],
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\out", {})
        self.fs_handler = FsHandler(self.event_conf, self.event_conf.subtype)
        self.fs_handler.FILE_LOG = self.current_dir + os.path.sep + "events.log"

    @Test(group=MOVE_GROUP)
    def move_test(self):
        shutil.copy("D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\test_2208.txt",
                    self.event_conf.directory)
        event = FileCreatedEvent(self.event_conf.directory + os.path.sep + "test_2208.txt")
        self.result = self.fs_handler.on_created(event)
        assert_true(os.path.exists(self.event_conf.destination + os.path.sep + "test_2208.txt"))

    @AfterMethod(group=MOVE_GROUP)
    def after_move_test(self):
        if self.result:
            os.remove(self.event_conf.destination + os.path.sep + "test_2208.txt")
        else:
            os.remove(self.event_conf.directory + os.path.sep + "test_2208.txt.err")

    @BeforeMethod(group=ARCHIVE_GROUP)
    def before_archive_test(self):
        self.event_conf = EventConf(True, "test move", "fs", FsHandler.TYPE_ARCHIVE,
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\in",
                                    ["test_????.txt"],
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\out", {})
        self.fs_handler = FsHandler(self.event_conf, self.event_conf.subtype)
        self.fs_handler.FILE_LOG = self.current_dir + os.path.sep + "events.log"

    @Test(group=ARCHIVE_GROUP)
    def archive_test(self):
        shutil.copy("D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\test_2208.txt",
                    self.event_conf.directory)
        event = FileCreatedEvent(self.event_conf.directory + os.path.sep + "test_2208.txt")
        self.result = self.fs_handler.on_created(event)
        assert_true(os.path.exists(self.event_conf.destination + os.path.sep + "test_2208.txt.tar"))

    @AfterMethod(group=ARCHIVE_GROUP)
    def after_archive_test(self):
        if self.result:
            os.remove(self.event_conf.destination + os.path.sep + "test_2208.txt.tar")
        else:
            os.remove(self.event_conf.directory + os.path.sep + "test_2208.txt.err")

    @BeforeMethod(group=COMPRESS_GROUP)
    def before_compress_test(self):
        self.event_conf = EventConf(True, "test move", "fs", FsHandler.TYPE_COMPRESS,
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\in",
                                    ["test_????.txt"],
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\out", {})
        self.fs_handler = FsHandler(self.event_conf, self.event_conf.subtype)
        self.fs_handler.FILE_LOG = self.current_dir + os.path.sep + "events.log"

    @Test(group=COMPRESS_GROUP)
    def compress_test(self):
        shutil.copy("D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\test_2208.txt",
                    self.event_conf.directory)
        event = FileCreatedEvent(self.event_conf.directory + os.path.sep + "test_2208.txt")
        self.result = self.fs_handler.on_created(event)
        assert_true(os.path.exists(self.event_conf.destination + os.path.sep + "test_2208.txt.gz"))

    @AfterMethod(group=COMPRESS_GROUP)
    def after_compress_test(self):
        if self.result:
            os.remove(self.event_conf.destination + os.path.sep + "test_2208.txt.gz")
        else:
            os.remove(self.event_conf.directory + os.path.sep + "test_2208.txt.err")

    @BeforeMethod(group=UNARCHIVE_GROUP)
    def before_unarchive_test(self):
        self.event_conf = EventConf(True, "test move", "fs", FsHandler.TYPE_UNARCHIVE,
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\in",
                                    ["test_????.txt.tar"],
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\out", {})
        self.fs_handler = FsHandler(self.event_conf, self.event_conf.subtype)
        self.fs_handler.FILE_LOG = self.current_dir + os.path.sep + "events.log"

    @Test(group=UNARCHIVE_GROUP)
    def unarchive_test(self):
        shutil.copy("D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\test_2208.txt.tar",
                    self.event_conf.directory)
        event = FileCreatedEvent(self.event_conf.directory + os.path.sep + "test_2208.txt.tar")
        self.result = self.fs_handler.on_created(event)
        assert_true(os.path.exists(self.event_conf.destination + os.path.sep + "test_2208.txt"))

    @AfterMethod(group=UNARCHIVE_GROUP)
    def after_unarchive_test(self):
        if self.result:
            os.remove(self.event_conf.destination + os.path.sep + "test_2208.txt")
        else:
            os.remove(self.event_conf.directory + os.path.sep + "test_2208.txt.tar.err")

    @BeforeMethod(group=UNCOMPRESS_GROUP)
    def before_uncompress_test(self):
        self.event_conf = EventConf(True, "test move", "fs", FsHandler.TYPE_UNCOMPRESS,
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\in",
                                    ["test_????.txt.gz"],
                                    "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\out", {})
        self.fs_handler = FsHandler(self.event_conf, self.event_conf.subtype)
        self.fs_handler.FILE_LOG = self.current_dir + os.path.sep + "events.log"

    @Test(group=UNCOMPRESS_GROUP)
    def uncompress_test(self):
        shutil.copy("D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\test_2208.txt.gz",
                    self.event_conf.directory)
        event = FileCreatedEvent(self.event_conf.directory + os.path.sep + "test_2208.txt.gz")
        self.result = self.fs_handler.on_created(event)
        assert_true(os.path.exists(self.event_conf.destination + os.path.sep + "test_2208.txt"))

    @AfterMethod(group=UNCOMPRESS_GROUP)
    def after_uncompress_test(self):
        if self.result:
            os.remove(self.event_conf.destination + os.path.sep + "test_2208.txt")
        else:
            os.remove(self.event_conf.directory + os.path.sep + "test_2208.txt.gz.err")
