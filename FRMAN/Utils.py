#!/usr/bin/env python3
"""
FRMAN Utilities File. So far, just yml and logging.
"""

from argparse import ArgumentParser
from logging import FileHandler, StreamHandler, basicConfig, getLevelName
from os import environ
from os.path import abspath, join
from pathlib import Path
from re import compile
from tabulate import tabulate

from yaml import SafeLoader, load


def pretty_print(dataframe, showindex=True, tablefmt="presto"):
    """PRINT A TABULAR DATAFRAME."""
    print(tabulate(dataframe, showindex=showindex, tablefmt=tablefmt,
                   headers="keys"))


def logger(level="info", timestamp=True, filename=None,
           custom_format=None, inherit_env=True):
    """
    Default Logging Method for FRMAN. This takes multiple
    parameters/environmental variable to define logging output.
    This function accepts the following environmental variables:

    ${LOG_LEVEL} - STRING - What level of messages to
                    log (ie. debug, info, warn, error)
    ${LOG_TIMESTAMP} - BOOLEAN - Whether or not to include
                    a timestamp before logged messages
    ${LOG_FILENAME} - STRING - Path of file to record logging messages to.
                    No output to file unless provided
    ${LOG_FORMAT} - STRING - log formatting string, defaults to
                    "%(asctime)s [%(levelname)8s]: %(message)s"

    Example:
        log = logger(level="debug", timestamp=True)
        log.debug("This is a debug message")
        log.info("This is an info message.")
        log.warning("This is a warning message.")
        log.error("This is an error message.")
        log.critical("This is a critical message.")
        > 2019-11-16 15:37:56,805 [   DEBUG]: This is a debug message
        > 2019-11-16 15:37:56,805 [    INFO]: This is an info message.
        > 2019-11-16 15:37:56,805 [ WARNING]: This is a warning message.
        > 2019-11-16 15:37:56,805 [   ERROR]: This is an error message.
        > 2019-11-16 15:37:56,805 [CRITICAL]: This is a critical message.
    :param str level: the level of depth you wish to log
    :param bool timestamp: whether or not to include timestamp in message
    :param str filename: enables export to file, provide file name/path
    :param str custom_format: log formatting string,
        defaults to "%(asctime)s [%(levelname)8s]: %(message)s"
    :param bool inherit_env: environmental vairables will overwrite other parameters
    :return: the logging object
    """
    if inherit_env:
        log_level = environ.get(key="LOG_LEVEL", default=level)
        log_timestamp = str(environ.get(
            key="LOG_TIMESTAMP",
            default=timestamp)).lower() in ["true", "t", "yes", "y", "1"]
        log_filename = environ.get(key="LOG_FILENAME", default=filename)
        custom_format = environ.get(
            key="LOG_FORMAT", default=custom_format)
    else:
        log_level = level
        log_timestamp = timestamp
        log_filename = filename
        custom_format = custom_format
    if not custom_format:
        format = "%(asctime)s [%(levelname)8s]: %(message)s"
    else:
        format = custom_format
    if not log_timestamp and not custom_format:
        format = format.replace("%(asctime)s ", "")
    handlers = [StreamHandler()]
    if log_filename:
        handlers.append(FileHandler(log_filename))

    logger = basicConfig(level=getLevelName(log_level.upper()),
                         format=format,
                         handlers=handlers)
    return logger


yml_location = join(Path(abspath(__file__)).parent.parent,
                    "config", "config.yml")
argument_yml = join(Path(abspath(__file__)).parent.parent,
                    "config", "arguments.yml")


def yml(file_path=yml_location, data=None, tag=None):
    """
    Load a yaml configuration file (path) or data object(data)
    and resolve any environment variables. The environment
    variables must be in this format to be parsed: ${VAR_NAME}.
    E.g.:
    database:
        host: ${HOST}
        port: ${PORT}
        ${KEY}: ${VALUE}
    app:
        log_path: "/var/${LOG_PATH}"
        something_else: "${AWESOME_ENV_VAR}/var/${A_SECOND_AWESOME_VAR}"
    :param str file_path: the path to the yaml file
    :param str data: the yaml data itself as a stream
    :param str tag: the tag to look for
    :return: the dict configuration
    :rtype: dict[str, T]
    """
    if "arguments" in str(file_path).lower():
        file_path = argument_yml
    pattern = compile(r".*?\${(\w+)}.*?")
    loader = SafeLoader
    loader.add_implicit_resolver(tag=tag, regexp=pattern, first=None)

    def env_var_constructor(loader, node):
        """
        Extracts the environment variable from the node's value
        :param yaml.Loader loader: the yaml loader
        :param node: the current node in the yaml
        :return: the parsed string that contains the value of the environment
        variable
        """
        value = loader.construct_scalar(node=node)
        match = pattern.findall(string=value)
        if match:
            full_value = value
            for g in match:
                full_value = full_value.replace(
                    "${{{key}}}".format(key=g), environ.get(key=g, default=g))
            return full_value
        return value

    loader.add_constructor(tag=tag, constructor=env_var_constructor)

    if file_path:
        with open(file_path) as conf_data:
            return load(stream=conf_data, Loader=loader)
    elif data:
        return load(stream=data, Loader=loader)
    else:
        raise ValueError("Either a path or data should be defined as input")


class Arguments:
    """CREATE AN ARGUMENT PARSER FROM A DICTIONARY.
    USE A LONG ARGUMENT AND A SHORT ARGUMENT (SINGLE LETTER) WILL
    AUTOMATICALLY BE CREATED."""

    def __init__(self, dictionary, file=False):
        """USE A DICTIONARY OR A JSON FILE PATH.

        SAMPLE DICTIONARY:

        {
          "application": "arggz",
          "version": "1.0",
          "description": "Argument Parsing Testing!",
          "arguments": [{
              "arg": "dog",
              "action": "store",
              "dest": "dog",
              "default": "Huck",
              "help": "Dog Name"
            },
            {
              "arg": "cat",
              "action": "store",
              "dest": "cat",
              "default": "Pumpkin",
              "help": "Cat Name"
            }
          ]
        }
        """
        if file:
            with open(dictionary) as json_file:
                self.dictionary = load(json_file)
        else:
            self.dictionary = dictionary

    def parse(self):
        """PARSE THE DICTIONARY TO CREATE AN ARGUMENT PARSER."""
        parser = ArgumentParser(description=self.dictionary["description"])
        letter_list = []
        for argument in self.dictionary["arguments"]:
            formatted_argument = "--{}".format(
                argument["arg"].replace("-", "")).lower()
            argument_letter = "-{}".format(
                formatted_argument.replace("--", "-")[1])
            letter_list.append(argument_letter)
            if not len(letter_list) == len(set(letter_list)):
                exception_message = "Duplicate argument letter found.\n \
                The argzz module only supports arguments beginning with \
                different letters.\n \
                Please use argparse.ArgumentParser for this functionality." \
                    .replace("  ", "")
                raise Exception(exception_message)
            parser.add_argument(formatted_argument, argument_letter,
                                action=argument["action"],
                                dest=argument["dest"],
                                default=argument["default"],
                                help=argument["help"])
        parser.add_argument(
            "--version", action="version",
            version="{application} {version}".format(
                application=self.dictionary["application"],
                version=self.dictionary["version"]))
        return parser.parse_args().__dict__


class FRMANError(Exception):
    """
    Exception class for FRMAN.
    """
    pass


class LOG_LEVEL:
    CRITICAL = 50
    ERROR = 40
    WARNING = 30
    INFO = 20
    DEBUG = 10


logger(level="info", timestamp=True, filename=None,
       custom_format=None, inherit_env=True)
