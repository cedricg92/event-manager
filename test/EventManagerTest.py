import os
import shutil
import time
from multiprocessing import Process

from ptest.assertion import assert_equals
from ptest.decorator import TestClass, Test, BeforeMethod, AfterMethod
from ptest.plogger import preporter

import run_event_manager


@TestClass(run_mode="singleline")  # the test cases in this class will be executed by multiple threads
class EventManagerTest:
    def __init__(self):
        self.process = None
        self.current_dir = os.path.dirname(os.path.realpath(__file__))

    @BeforeMethod(description="Prepare test data.")
    def before(self):
        preporter.info("Run event manager.")
        self.process = Process(target=run_event_manager.main)
        self.process.start()

    @Test(tags=["tar", "gz"], enabled=False)
    def test1(self):
        if os.path.exists(self.current_dir + "\\..\\rep_test\\out\\test_2208.txt.tar.gz"):
            os.remove(self.current_dir + "\\..\\rep_test\\out\\test_2208.txt.tar.gz")
        shutil.copy(self.current_dir + "\\..\\rep_test\\test_2208.txt", self.current_dir + "\\..\\rep_test\\in")
        time.sleep(5)
        assert_equals(os.path.exists(self.current_dir + "\\..\\rep_test\\out\\test_2208.txt.tar"), True)

    @AfterMethod(always_run=True, description="Clean up")
    def after(self):
        self.process.terminate()
