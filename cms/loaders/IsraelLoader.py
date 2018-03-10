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
import json
import logging
import os
import time
import yaml

from cms import SCORE_MODE_MAX
from cms.db import Contest, Task, Statement, \
    SubmissionFormatElement, Dataset, Manager, Testcase, Attachment
from cmscommon.datetime import make_datetime
from cmscontrib.loaders.base_loader import ContestLoader, TaskLoader
from cmscontrib import touch

from server_utils.config import CLONE_DIR, time_from_str, SERVER_NAME
from task_utils.processing.TaskProcessor import TaskProcessor

logger = logging.getLogger(__name__)


class IsraelTaskLoader(TaskLoader):
    """
    Load a task in Israel YAML format.

    The path should be a task directory. It should contain a directory
    "auto.gen" with the generated task files, including a "module.yaml" file.

    Loading tasks outside of contests is not supported. A task is defined
    by its path, but also by its name which is provided by the contest.
    This allows more flexibility, e.g. when renaming tasks.
    """

    @staticmethod
    def detect(path):
        """
        See docstring in base_loader.

        Task detection is not supported.
        """
        raise NotImplementedError("IsraelTaskLoader doesn't "
                                  "support detection.")

    def __init__(self, path, file_cacher, task_contest_info=None,
                 contest_dir=None):
        """
        Create a new task loader.
        task_contest_info is a dictionary containing the task info specified
        in the contest. We don't load without it.
        """
        super(IsraelTaskLoader, self).__init__(path, file_cacher)
        if task_contest_info is None or contest_dir is None:
            raise Exception("Tasks can only be loaded from a contest.")
        self.task_contest_info = task_contest_info
        self.short_name = task_contest_info["short_name"]

        logger.info("Instantiating task loader for %s with path %s",
                    self.short_name, path)

        self.post_gen_dir = os.path.join(path, "auto.gen")
        module_path = os.path.join(self.post_gen_dir, "module.yaml")
        self.processor = TaskProcessor(module_path, path, post_gen_dir=None)
        self.subtasks = self.processor.get_subtasks()
        self.task_type = self.processor.get_task_type()
        self.has_checker = self.processor.has_checker()
        self.has_grader = self.processor.has_grader()
        self.graders = self.processor.get_graders()
        self.headers = self.processor.get_headers()
        self.managers = self.processor.get_managers()

        # Use ".ok_task" and ".error_task" files in the contest directory
        # to keep track of whether the task changed/imported successfully.
        self.contest_ok_mark = os.path.join(contest_dir, ".ok.%s_%s" %
                                            (SERVER_NAME, self.short_name))
        self.contest_error_mark = os.path.join(contest_dir, ".error.%s_%s" %
                                               (SERVER_NAME, self.short_name))
        self.task_ok_mark = os.path.join(self.post_gen_dir, "gen.ok")
        self.task_error_mark = os.path.join(self.post_gen_dir, "gen.error")

        logger.info("Fetched data from TaskProcessor.")

    def get_task(self, get_statement):
        """
        See docstring in base_loader.
        """

        # Cannot import a task that with generation errors.
        if os.path.isfile(self.task_error_mark):
            raise Exception("Task has an error mark: %s" %
                            self.task_error_mark)
        if not os.path.isfile(self.task_ok_mark):
            raise Exception("Task does not have an okay mark: %s" %
                            self.task_ok_mark)

        # Mark this import as an error until we finish.
        touch(self.contest_error_mark)
        if os.path.isfile(self.contest_ok_mark):
            os.remove(self.contest_ok_mark)

        args = {}

        self.put_names(args)
        if get_statement:
            self.put_statements(args)
        self.put_score_mode(args)
        self.put_task_submission_format(args)
        self.put_attachments(args)

        task = Task(**args)
        task.active_dataset = self.create_dataset(task)

        # Success - mark this task as okay.
        touch(self.contest_ok_mark)
        os.remove(self.contest_error_mark)
        return task

    def put_names(self, args):
        """
        Put the task's short name and long name in the given args.
        """
        args["name"] = self.task_contest_info["short_name"]
        args["title"] = self.task_contest_info["long_name"]

    def put_statements(self, args):
        """
        Create Statement objects and put them in the given args.
        Define all statements as primary.
        """
        args["statements"] = []
        statements = self.processor.get_statements()

        for statement_info in statements:
            language = statement_info["language"]
            path = statement_info["path"]
            description = "Statement for task %s (lang: %s)" % \
                          (self.short_name, language)
            digest = self.file_cacher.put_file_from_path(path, description)

            args["statements"] += [Statement(language, digest)]

        languages = [statement["language"] for statement in statements]
        args["primary_statements"] = json.dumps(languages)

    def put_score_mode(self, args):
        """
        Put the score mode in the given args.
        Currently we only use the best submission (max score).
        """
        args["score_mode"] = SCORE_MODE_MAX

    def put_task_submission_format(self, args):
        """
        Put the task's submission format in the given args.
        """

        if self.task_type == "Batch":
            # Batch programs are always named Task.cpp, Task.java, etc.
            # Note that in Java this means the class must be "Task".
            args["submission_format"] = [SubmissionFormatElement("Task.%l")]

        elif self.task_type == "OutputOnly":
            # Output files must always be in the form "output_000.txt",
            # "output_001.txt", and so on.
            total_testcases = sum(len(subtask["testcases"])
                                  for subtask in self.subtasks)

            args["submission_format"] = []
            for index in xrange(total_testcases):
                args["submission_format"] += [
                    SubmissionFormatElement("output_%03d.txt" % index)
                ]

        elif self.task_type == "TwoSteps":
            # TwoSteps files are always "encoder" and "decoder".
            args["submission_format"] = [
                SubmissionFormatElement("encoder.%l"),
                SubmissionFormatElement("decoder.%l")
            ]

    def put_attachments(self, args):
        """
        Create Attachment objects and put them in the given args.
        """
        args["attachments"] = []
        attachment_paths = self.processor.get_attachments()
        for path in attachment_paths:
            base_name = os.path.basename(path)
            description = "Attachment %s for task %s" % \
                          (base_name, self.short_name)
            digest = self.file_cacher.put_file_from_path(path, description)
            args["attachments"] += [Attachment(base_name, digest)]

    def create_dataset(self, task):
        """
        Create the main dataset for this task.
        """

        args = {}
        self.put_dataset_basic_info(args, task)
        self.put_dataset_limits(args)
        self.put_dataset_score_type(args)
        self.put_dataset_type_parameters(args)
        self.put_dataset_managers(args)
        self.put_dataset_testcases(args)

        return Dataset(**args)

    def put_dataset_basic_info(self, args, task):
        """
        Put the basic dataset info in the given args:
        task, type, description, autojudge.
        """
        args["task"] = task
        args["task_type"] = self.task_type
        args["description"] = "Default"
        args["autojudge"] = False

    def put_dataset_limits(self, args):
        """
        Put the time and memory limits in the given args.
        """
        args["time_limit"] = float(self.processor.get_time())
        args["memory_limit"] = self.processor.get_memory()

    def put_dataset_score_type(self, args):
        """
        Put the score type parameters in the given args.
        """
        # The subtask structure is used for the score type parameters.
        # Each item in the list is of the form [score, codename_regex].
        # For example: [[10, "01\..*"], [90, "(01\..*)|(02\..*)"]]
        # We use the list of contained subtasks to generate the regex.
        subtask_structure = []
        for (subtask_index, subtask) in enumerate(self.subtasks):

            # The subtask contains all testcases that begin with its number.
            # For every additional contained subtask, we take the union
            # of its corresponding regex.
            regex = "(%02d\\..*)" % (subtask_index + 1)

            if "contains" in subtask:
                for other_subtask_index in subtask["contains"]:
                    # Other subtask index is 1-based.
                    regex += "|(%02d\\..*)" % other_subtask_index

            subtask_structure += [[subtask["score"], regex]]

        args["score_type_parameters"] = json.dumps(subtask_structure)

        # The score type is always "GroupMin". See CMS documentation.
        args["score_type"] = "GroupMin"

    def put_dataset_type_parameters(self, args):
        """
        Put the task type parameters in the given args.
        """

        if self.has_checker:
            comparator_str = "comparator"
        else:
            comparator_str = "diff"

        if self.has_grader:
            grader_str = "grader"
        else:
            grader_str = "alone"

        if self.task_type == "Batch":
            # Batch type expects the first parameter to be "grader" or
            # "alone"; the second parameter to have input/output file names
            # (we leave them empty to use stdin/stdout); the third parameter
            # is "comparator" or "diff".
            result = [grader_str, ["", ""], comparator_str]

        elif self.task_type in ("OutputOnly", "TwoSteps"):
            # OutputOnly and TwoSteps only expect the comparator info.
            result = [comparator_str]

        else:
            raise Exception("Unknown task type: %s" % self.task_type)

        args["task_type_parameters"] = json.dumps(result)

    def put_dataset_managers(self, args):
        """
        Put the task managers in the given args.
        Managers are all files related to the user's compilation and execution:
        checker, graders, headers, and manager.cpp (for TwoSteps).
        """
        args["managers"] = []

        for grader_path in self.graders:
            base_name = os.path.basename(grader_path)
            description = "Grader for task %s" % self.short_name
            digest = self.file_cacher.put_file_from_path(grader_path,
                                                         description)
            args["managers"] += [Manager(base_name, digest)]

        for header_path in self.headers:
            base_name = os.path.basename(header_path)
            description = "Header for task %s" % self.short_name
            digest = self.file_cacher.put_file_from_path(header_path,
                                                         description)
            args["managers"] += [Manager(base_name, digest)]

        for manager_path in self.managers:
            base_name = os.path.basename(manager_path)
            description = "Manager for task %s" % self.short_name
            digest = self.file_cacher.put_file_from_path(manager_path,
                                                         description)
            args["managers"] += [Manager(base_name, digest)]

        if self.has_checker:
            checker_path = os.path.join(self.post_gen_dir, "checker")
            description = "Manager for task %s" % self.short_name
            digest = self.file_cacher.put_file_from_path(checker_path,
                                                         description)
            args["managers"] += [Manager("checker", digest)]

    def put_dataset_testcases(self, args):
        """
        Put the task's testcases in the given args.
        """
        args["testcases"] = []

        for (subtask_index, subtask) in enumerate(self.subtasks):
            for (testcase_index, testcase) in enumerate(subtask["testcases"]):
                input_path = testcase["input"]
                output_path = testcase["output"]

                input_desc = "Input %02d.%02d for task %s" % \
                             (subtask_index, testcase_index, self.short_name)
                output_desc = "Output %02d.%02d for task %s" % \
                              (subtask_index, testcase_index, self.short_name)

                input_digest = self.file_cacher.put_file_from_path(
                    input_path, input_desc)
                output_digest = self.file_cacher.put_file_from_path(
                    output_path, output_desc)

                codename = "%02d.%02d" % (subtask_index + 1,
                                          testcase_index + 1)

                args["testcases"] += [
                    Testcase(codename, True, input_digest, output_digest)
                ]

    def task_has_changed(self):
        """
        See docstring in base_loader.
        """

        # A task needs to be reimported if there is no okay mark
        # in the contest directory, or there is an error mark,
        # or the okay mark is older than the last generation.
        if os.path.isfile(self.contest_error_mark):
            return True
        if not os.path.isfile(self.contest_ok_mark):
            return True

        contest_ok_time = os.path.getmtime(self.contest_ok_mark)
        task_ok_time = os.path.getmtime(self.task_ok_mark)
        return contest_ok_time < task_ok_time


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
        """
        return IsraelContestLoader.get_module_path(path) is not None

    @staticmethod
    def get_module_path(path):
        """
        A contest path is valid if it contains "module.yaml".
        The given path is checked in this order:
        - If this path itself contains the file.
        - If this path under the contests directory contains the file.
        - If this path's base name under the contests directory contains
          the file.

        Return the absolute path to the module if it exists, otherwise None.
        """
        path = os.path.abspath(path)

        # Check the given path.
        module_path = os.path.join(path, "module.yaml")
        if os.path.isfile(module_path):
            return module_path

        # Check the given path inside the clone directory.
        module_path = os.path.join(CLONE_DIR, path, "module.yaml")
        if os.path.isfile(module_path):
            return module_path

        # Check the base name inside the clone directory.
        base_name = os.path.basename(path)
        module_path = os.path.join(CLONE_DIR, base_name, "module.yaml")
        if os.path.isfile(module_path):
            return module_path

        return None

    def __init__(self, path, file_cacher):
        """
        See docstring in base_loader.

        The contest path should contain a "module.yaml" file (see templates).
        """
        super(IsraelContestLoader, self).__init__(path, file_cacher)

        # Get the module from the given path.
        module_path = IsraelContestLoader.get_module_path(path)
        self.contest_dir = os.path.dirname(module_path)

        with open(module_path) as stream:
            self.params = yaml.safe_load(stream)

    def get_task_loader(self, taskname):
        """
        Return an IsraelTaskLoader object for the given task name.
        The object is initialized with the task info, containing its names.
        """
        task_info = None
        for info in self.params["tasks"]:
            if info["short_name"] == taskname:
                task_info = info
        if task_info is None:
            raise Exception("Task %s not found in contest %s." %
                            (taskname, self.contest_dir))

        task_path = os.path.join(CLONE_DIR, task_info["path"])
        return IsraelTaskLoader(task_path, self.file_cacher, task_info,
                                self.contest_dir)

    def get_contest(self):
        """
        See docstring in base_loader.
        """
        contest = self.get_contest_object()
        participations = self.get_participations_info()
        tasks = self.get_tasks_list()
        return contest, tasks, participations

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

        # Communication
        args["allow_questions"] = self.params.get("allow_questions", False)

        # Times.
        start_time = time_from_str(self.params["start_time"])
        stop_time = time_from_str(self.params["end_time"])
        args["start"] = make_datetime(time.mktime(start_time.timetuple()))
        args["stop"] = make_datetime(time.mktime(stop_time.timetuple()))

        # Limits.
        args["max_submission_number"] = self.params["max_submission_number"]
        args["max_user_test_number"] = self.params["max_user_test_number"]

        interval_seconds = self.params["min_submission_interval"]
        if interval_seconds is not None:
            delta = timedelta(seconds=interval_seconds)
            args["min_submission_interval"] = delta

        interval_seconds = self.params["min_user_test_interval"]
        if interval_seconds is not None:
            delta = timedelta(seconds=interval_seconds)
            args["min_user_test_interval"] = delta

        return Contest(**args)

    def get_tasks_list(self):
        """
        Return a list of this contest's tasks' short names.
        """
        return [task["short_name"] for task in self.params["tasks"]]

    def contest_has_changed(self):
        """
        See docstring in base_loader.
        """
        return True

    def get_participations_info(self):
        """
        To create a participation, we need two fields:
        a username, and whether the user is hidden and/or unrestricted.

        Passwords are ignored, since we don't use contest-specific passwords.
        """

        users_file = os.path.join(CLONE_DIR, self.params["users_file"])
        with open(users_file) as stream:
            users_list = yaml.safe_load(stream)

        result = []
        for user in users_list:
            participation_info = {"username": user["username"]}
            if "hidden" in user:
                participation_info["hidden"] = user["hidden"]
            if "unrestricted" in user:
                participation_info["unrestricted"] = user["unrestricted"]
            result += [participation_info]
        return result
