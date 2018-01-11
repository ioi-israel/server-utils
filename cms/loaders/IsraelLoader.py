#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
Loader for Israel YAML format.
See base_loader for full documentation.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from datetime import timedelta
import os
import yaml

from cms.db import Contest, User, Task, Statement, \
    SubmissionFormatElement, Dataset, Manager, Testcase
from cmscontrib.loaders.base_loader import ContestLoader, TaskLoader, \
    UserLoader

from server_utils.config import CONTESTS_DIR, TASKS_DIR, USERS_FILE, \
    time_from_str
from task_utils.processing.TaskProcessor import TaskProcessor


class IsraelTaskLoader(TaskLoader):
    """
    Load a task in Israel YAML format.

    The path should be a task directory. It should contain a directory
    "auto.gen" with the generated task files, including a "module.yaml" file.

    """

    def __init__(self, path, file_cacher):
        super(IsraelTaskLoader, self).__init__(path, file_cacher)

    def get_task(self, get_statement):
        """
        See docstring in base_loader.
        """
        raise NotImplementedError("Please extend TaskLoader")

    def task_has_changed(self):
        """
        See docstring in base_loader.
        """
        raise NotImplementedError("Please extend TaskLoader")


class IsraelUserLoader(UserLoader):
    """
    See docstring in base_loader.

    We assume all users are in the global users file, see config.yaml.
    We take loading a "path" to mean loading a username.
    """

    @staticmethod
    def detect(path):
        """
        See docstring in base_loader.

        We abuse the path argument to mean username.
        """
        username = path
        return IsraelUserLoader._get_user_info(username) is not None

    @staticmethod
    def _get_user_info(username):
        """
        Get info about the given user from the global users file.
        If not a valid user, return None.

        If it is a valid user, return a dictionary with keys:
        username, password, first_name, last_name.
        Other keys in the users file are ignored.
        """

        with open(USERS_FILE) as stream:
            users_list = yaml.safe_load(stream)

        for user_dict in users_list:
            if user_dict["username"] == username:
                return {
                    "username": username,
                    "password": user_dict["password"],
                    "first_name": user_dict["first_name"],
                    "last_name": user_dict["last_name"]
                }
        return None

    def __init__(self, path, file_cacher):
        """
        See docstring in base_loader.

        We abuse the path argument to mean username.
        """
        super(IsraelUserLoader, self).__init__(path, file_cacher)
        self.username = path
        self.user_info = IsraelUserLoader._get_user_info(self.username)

    def get_user(self):
        """
        See docstring in base_loader.
        """
        return User(**self.user_info)

    def user_has_changed(self):
        """
        See docstring in base_loader.
        """
        return True

    def get_task_loader(self, taskname):
        """
        This method is only implemented in the contest loader,
        it is here for linting, because BaseLoader defines it as abstract.
        """
        raise NotImplementedError("IsraelUserLoader does not provide "
                                  "get_task_loader")


class IsraelContestLoader(ContestLoader):
    """
    Load a contest in Israel YAML format.
    """

    short_name = "israel_contest"
    description = "Israel YAML contest format"

    @staticmethod
    def detect(path):
        """
        See docstring in base_loader.

        A contest path is valid if it contains "module.yaml".
        The path is first checked by itself, and if not valid,
        it is checked inside the contests directory.
        """

        # Check the given path.
        module_path = os.path.join(path, "module.yaml")
        if os.path.isfile(module_path):
            return True

        # Check the given path inside contests directory.
        module_path = os.path.join(CONTESTS_DIR, module_path)
        return os.path.isfile(module_path)

    def __init__(self, path, file_cacher):
        """
        See docstring in base_loader.

        The contest path should contain a "module.yaml" file (see templates).
        """
        super(IsraelContestLoader, self).__init__(path, file_cacher)

        # Get the module from the given path, inside the contests directory
        # if needed.
        module_path = os.path.join(path, "module.yaml")
        if not os.path.isfile(module_path):
            module_path = os.path.join(CONTESTS_DIR, module_path)

        self.contest_dir = os.path.dirname(os.path.abspath(module_path))

        with open(module_path) as stream:
            self.params = yaml.safe_load(stream)

    def get_task_loader(self, taskname):
        raise NotImplementedError("Please extend Loader")

    def get_contest(self):
        """
        See docstring in base_loader.
        """
        contest = self.get_contest_object()
        # TODO

    def get_contest_object(self):
        """
        Return the Contest database object.
        """
        args = {}

        # Names.
        args["name"] = self.params["short_name"]
        args["description"] = self.params["long_name"]

        # Languages.
        args["languages"] = self.params["languages"]

        # Times.
        args["start"] = time_from_str(self.params["start_time"])
        args["stop"] = time_from_str(self.params["end_time"])

        # Limits.
        args["max_submission_number"] = self.params["max_submission_number"]
        args["max_user_test_number"] = self.params["max_user_test_number"]

        interval_seconds = self.params["min_submission_interval"]
        delta = timedelta(seconds=interval_seconds)
        args["min_submission_interval"] = delta

        interval_seconds = self.params["min_user_test_interval"]
        delta = timedelta(seconds=interval_seconds)
        args["min_user_test_interval"] = delta

        return Contest(**args)

    def contest_has_changed(self):
        """
        See docstring in base_loader.
        """
        raise NotImplementedError("Please extend ContestLoader")
