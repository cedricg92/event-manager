import json

from shirp.handler import HDFSHandler


class EventConf:
    """Configuration of Event

    """
    def __init__(self, enabled, name, event_type, subtype, directory, patterns, destination, context):
        """Constructor

        :param enabled:
        :type enabled: bool
        :param name:
        :type name: str
        :param event_type:
        :type event_type: str
        :param subtype:
        :type subtype: str
        :param directory:
        :type directory: str
        :param patterns:
        :type patterns: list of str
        :param destination:
        :type destination: str
        :param context: Context of event
        :type context: dict
        :return:
        """
        self.enabled = enabled
        self.name = name
        self.type = event_type
        self.subtype = subtype
        self.directory = directory
        self.destination = destination
        self.patterns = patterns
        self.context = context

    def is_scheduled(self):
        """Check if the event is scheduled

        :return: True if the event is cheduled
        :rtype: bool
        """
        return self.get_context_value("schedule", False)

    def get_cron(self):
        return self.get_context_value("cron")

    def get_max_time_execution(self):
        return self.get_context_value("maxTimeExecution", 0)

    def get_max_executions(self):
        return self.get_context_value("maxExecutions", 0)

    def get_context_value(self, name, default_value=None):
        if name in self.context:
            return self.context[name]
        return default_value

    def is_fs_directory(self):
        """Check if the event use a linux fs directory

        :return: True if the event use a linux fs directory
        :rtype: bool
        """
        return self.type.lower() != "hdfs" or (self.subtype != HDFSHandler.STR_TYPE_GET and
                                               self.subtype != HDFSHandler.TYPE_GET)


class EventLoader:
    """Events Loader

    """
    def __init__(self):
        self.val = None

    @staticmethod
    def load_event_from_json(json_file):
        """Load events from Json

        :param json_file: Filename of json
        :type json_file: str
        :return: List of Events
        :rtype: dict of EventConf
        """
        j_file = open(json_file)
        json_data = json.load(j_file)
        event_list = {}
        for event in json_data["events"]:
            enabled = event["enabled"]
            name = event["name"]
            event_type = event["type"]
            subtype = event["subtype"]
            directory = event["directory"]
            patterns = event["filePatterns"]
            destination = event["destination"]
            exec_program = event["execProgram"]
            exec_args = event["execArgs"]
            hdfs_url = event["hdfsUrl"]
            hdfs_user = event["hdfsUser"]
            event_list[name] = EventConf(enabled, name, event_type, subtype, directory, patterns, destination, event)
        return event_list
