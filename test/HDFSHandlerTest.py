import os
import shutil

from ptest.assertion import assert_true
from ptest.decorator import TestClass, BeforeMethod, Test, AfterMethod
from watchdog.events import FileCreatedEvent

from shirp.event import EventConf
from shirp.handler import HDFSHandler

HDFS_GROUP = "grp-hdfs"


@TestClass(run_mode="singleline")
class HDFSHandlerTest:
    def __init__(self, hdfs_put_handler=None, hdfs_get_handler=None, put_event_conf=None, get_event_conf=None):
        """

        :param hdfs_put_handler:
        :type hdfs_put_handler: HDFSHandler
        :param hdfs_get_handler:
        :type hdfs_get_handler: HDFSHandler
        :param put_event_conf:
        :type put_event_conf: EventConf
        :param get_event_conf:
        :type get_event_conf: EventConf
        """
        self.hdfs_put_handler = hdfs_put_handler
        self.hdfs_get_handler = hdfs_get_handler
        self.put_event_conf = put_event_conf
        self.get_event_conf = get_event_conf
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.result = False

    @BeforeMethod(group=HDFS_GROUP)
    def before_hdfs_test(self):
        self.put_event_conf = EventConf(True, "test move", "hdfs", HDFSHandler.TYPE_PUT,
                                        "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\in",
                                        ["test_????.txt"], "/user/hduser",
                                        {"hdfsUrl": "http://192.168.1.24:50070", "hdfsUser": "hduser"})
        self.get_event_conf = EventConf(True, "test move", "hdfs", HDFSHandler.TYPE_GET, "/user/hduser",
                                        ["test_????.txt"],
                                        "D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\out",
                                        {"hdfsUrl": "http://192.168.1.24:50070", "hdfsUser": "hduser"})
        HDFSHandler.FILE_LOG = self.current_dir + os.path.sep + "events.log"
        self.hdfs_put_handler = HDFSHandler(self.put_event_conf, self.put_event_conf.subtype)
        self.hdfs_get_handler = HDFSHandler(self.get_event_conf, self.get_event_conf.subtype)

    @Test(group=HDFS_GROUP)
    def move_test(self):
        shutil.copy("D:\\Users\\Cedric\\PycharmProjects\\event-manager\\rep_test\\test_2208.txt",
                    self.put_event_conf.directory)
        event = FileCreatedEvent(self.put_event_conf.directory + os.path.sep + "test_2208.txt")
        assert_true(self.hdfs_put_handler.on_created(event))
        assert_true(self.hdfs_get_handler.process("/user/hduser/test_2208.txt"))
        assert_true(os.path.exists(self.get_event_conf.destination + os.path.sep + "test_2208.txt"))

    @AfterMethod(group=HDFS_GROUP)
    def after_hdfs_test(self):
        os.remove(self.get_event_conf.destination + os.path.sep + "test_2208.txt")
