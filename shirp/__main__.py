import os
import time
from datetime import datetime
from threading import Thread

from watchdog.observers import Observer

import event
import handler

observers = []
schedules = []

current_dir = os.path.dirname(os.path.realpath(__file__))
path_sep = os.sep


def find_json(path):
    """Find Json in path

    :param path: Path to search
    :type path: str
    :return: None
    """
    for filename in os.listdir(path):
        if os.path.isfile(path + os.path.sep + filename) and filename.endswith(".json"):
            events_conf = event.EventLoader.load_event_from_json(path + os.path.sep + filename)
            for event_conf in events_conf.values():
                if not os.path.exists(current_dir + path_sep + "tmp" + path_sep + event_conf.name):
                    os.mkdir(current_dir + path_sep + "tmp" + path_sep + event_conf.name, 0o777)
                files = []
                if event_conf.is_fs_directory():
                    files = os.listdir(event_conf.directory)
                    for datafile in files:
                        if os.path.isfile(event_conf.directory + path_sep + datafile):
                            os.rename(event_conf.directory + path_sep + datafile, current_dir + path_sep + "tmp" +
                                      path_sep + event_conf.name + path_sep + datafile)
                for name in dir(handler):
                    if name.lower() == event_conf.type + "handler":
                        event_class = getattr(handler, name)
                        event_class.FILE_LOG = current_dir + path_sep + "log" + path_sep + "events.log"
                        event_handler = event_class(event_conf, event_conf.subtype)
                        try:
                            if not event_handler.is_scheduled():
                                t_observer = Observer()
                                t_observer.schedule(event_handler, event_conf.directory, recursive=False)
                                t_observer.start()
                                observers.append(t_observer)
                            else:
                                schedules.append(event_handler)
                        except ValueError as e:
                            print e.message
                        break
                for datafile in files:
                    if os.path.exists(current_dir + path_sep + "tmp" + path_sep + event_conf.name + path_sep + datafile):
                        os.rename(current_dir + path_sep + "tmp" + path_sep + event_conf.name + path_sep + datafile,
                                  event_conf.directory + path_sep + datafile)
        elif os.path.isdir(path + os.path.sep + filename):
            find_json(path + os.path.sep + filename)


def main():
    """Main program

    :return:
    """
    find_json(current_dir + path_sep + "json")
    threads = []
    try:
        while True:
            now = datetime.now()
            ts_now = time.time()
            tmp_threads = threads
            threads = []
            nb_threads = {}
            for t_thread in tmp_threads:
                if not t_thread._Thread__stopped:
                    if t_thread.max_execution_time <= 0 or ts_now - t_thread.ts_started <= t_thread.max_time_execution:
                        if t_thread.getName() in nb_threads:
                            nb_threads[t_thread.getName()] += 1
                        else:
                            nb_threads[t_thread.getName()] = 1
                        threads.append(t_thread)
                    else:
                        t_thread._stop()
            for event_handler in schedules:
                nb_execs = 0
                if event_handler.event_conf.name in nb_threads:
                    nb_execs = nb_threads[event_handler.event_conf.name]
                if event_handler.check_schedule(now) and (nb_execs < event_handler.event_conf.max_executions or
                                                          event_handler.event_conf.max_executions == 0):
                    t1 = Thread(target=event_handler.run_schedule)
                    t1.setName(event_handler.event_conf.name)
                    t1.max_execution_time = event_handler.event_conf.max_time_execution
                    t1.ts_started = time.time()
                    t1.start()
                    threads.append(t1)
            time.sleep(60 - datetime.now().second)
    except KeyboardInterrupt:
        # Ctrl + C stop all
        for observer in observers:
            observer.stop()
        for thread in threads:
            thread.stop()
    for observer in observers:
        observer.join()
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
