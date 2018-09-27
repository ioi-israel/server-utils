#!/usr/bin/env python2
"""
TaskSandbox is in charge of running a TaskProcessor (from task_utils package)
inside a sandbox, to generate all relevant files.
"""

import argparse
import os
import subprocess
import sys
import yaml
from task_utils.processing import TaskProcessor
from server_utils.config import CLONE_DIR


class TaskSandbox(object):
    """
    A class in charge of running a TaskProcessor (from task_utils package)
    inside a sandbox.
    """

    @staticmethod
    def execute(task_dir, gen_dir=None):
        """
        Initialize the sandbox, run the processor in it, and clean up.
        Raise an exception on failure.

        If gen_dir is not given, it is assumed to be "auto.gen" inside
        task_dir. In any case, it is created if it doesn't exist,
        and it is given full permissions (777).

        If the task does not need generating according to the TaskProcessor
        class, do nothing.
        """

        if not os.path.isdir(task_dir):
            raise Exception("Task directory not found: %s" % task_dir)

        if gen_dir is None:
            gen_dir = os.path.join(task_dir, "auto.gen")

        try:
            need_gen = TaskProcessor.TaskProcessor.needs_generating(task_dir,
                                                                    gen_dir)
            if not need_gen:
                return
        except Exception:
            pass

        if not os.path.isdir(gen_dir):
            os.mkdir(gen_dir)

        os.chmod(gen_dir, 0777)

        # The caller needs to handle any exceptions from initializing.
        TaskSandbox._init_isolate()

        # We want the caller to handle any exceptions from the execution
        # as well, so we don't catch, but we clean up before returning.
        try:
            TaskSandbox._execute_isolate(task_dir, gen_dir)
        finally:
            # Clean up. Note if both execution and cleaning up fail,
            # then only the clean up exception will be passed on.
            TaskSandbox._cleanup_isolate()

    @staticmethod
    def _run(commands, input_text=""):
        """
        Run the given commands as a subprocess, without giving it
        shell control. Return (return_code, stdout, stderr)
        """
        process = subprocess.Popen(commands,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate(input=input_text)
        return (process.returncode, stdout, stderr)

    @staticmethod
    def _init_isolate():
        """
        Initialize the sandbox. Raise an exception on failure.
        """
        flags = ["--box-id=90", "--cg", "--init"]
        return_code, stdout, stderr = TaskSandbox._run(["isolate"] + flags)
        if return_code != 0:
            raise Exception("Cannot initialize sandbox.\n"
                            "Return code: %s\n"
                            "Stdout: %s\n"
                            "Stderr: %s\n" %
                            (return_code, stdout, stderr))

    @staticmethod
    def _cleanup_isolate():
        """
        Cleanup the sandbox. Raise an exception on failure.
        """
        flags = ["--box-id=90", "--cg", "--cleanup"]
        return_code, stdout, stderr = TaskSandbox._run(["isolate"] + flags)
        if return_code != 0:
            raise Exception("Cannot clean up sandbox.\n"
                            "Return code: %s\n"
                            "Stdout: %s\n"
                            "Stderr: %s\n" %
                            (return_code, stdout, stderr))

    @staticmethod
    def _execute_isolate(task_dir, gen_dir):
        """
        Run the processor in the sandbox. The given gen_dir must be
        writable by all. Raise an exception on failure.
        """

        # Size limits, in KB.
        max_files_size = 200000
        max_memory = 1600000

        # Time limits, in seconds.
        max_time = 300
        max_time_wall = 300

        # Other limits.
        max_processes = 50

        # Path to the task module.
        module_path = os.path.join(task_dir, "module.py")

        # Path variables must be allowed, in case the processor.
        # will need to run additional commands (e.g. g++).
        path_var = "/usr/local/sbin:/usr/local/bin:/usr/sbin:" \
                   "/usr/bin:/sbin:/bin"

        # Python path for IOI repositories.
        # This is expected to be the grandparent of TaskProcessor's directory.
        # Access to TaskProcessor's directory is needed to run it.
        # Access to other scripts, like in task_algorithms, may be needed.
        processor_path = os.path.abspath(TaskProcessor.__file__)
        processor_dir = os.path.dirname(processor_path)
        task_utils_dir = os.path.dirname(processor_dir)
        python_path = os.path.dirname(task_utils_dir)

        flags = [
            "--box-id=90",
            "--cg",
            "--dir=%s" % task_dir,
            "--dir=%s:rw" % gen_dir,
            "--dir=%s" % python_path,
            "--env=PYTHONPATH=%s" % python_path,
            "--env=PATH=%s" % path_var,
            "--env=HOME=./",
            "--fsize=%s" % max_files_size,
            "--mem=%s" % max_memory,
            "--cg-mem=%s" % max_memory,
            "--processes=%s" % max_processes,
            "--time=%s" % max_time,
            "--wall-time=%s" % max_time_wall,
            "--run",
            "--",
            "/usr/bin/python2",
            processor_path,
            "--task_dir",
            task_dir,
            "--gen_dir",
            gen_dir,
            "--params_file",
            module_path,
            "--generate_all"
        ]

        return_code, stdout, stderr = TaskSandbox._run(["isolate"] + flags)

        if return_code != 0:
            raise Exception("Sandbox run finished with an error.\n"
                            "Task: %s\n"
                            "Return code: %s\n"
                            "Stdout: %s\n"
                            "Stderr: %s\n" %
                            (task_dir, return_code, stdout, stderr))


def create_processor(task_dir):
    """
    Create a TaskProcessor object from the given task path,
    which is assumed to be generated into the default auto.gen
    directory. The task path must be relative to the clone directory
    e.g. "tasks/user/taskname".

    Raise an exception on failure.
    """

    path = os.path.realpath(os.path.join(CLONE_DIR, task_dir))
    if not path.startswith(CLONE_DIR):
        raise Exception("Invalid task directory: %s" % path)

    gen_dir = os.path.join(path, "auto.gen")
    module_path = os.path.join(gen_dir, "module.yaml")
    return TaskProcessor.TaskProcessor(module_path, path, gen_dir)


def processor_from_contest(task_name, contest_name):
    """
    Create a TaskProcessor object from the given task and contest names.
    """

    contest_path = os.path.realpath(os.path.join(CLONE_DIR, "contests",
                                                 contest_name))
    if not contest_path.startswith(CLONE_DIR):
        raise Exception("Invalid contest: %s" % contest_path)

    contest_module_path = os.path.join(contest_path, "module.yaml")
    with open(contest_module_path) as stream:
        contest_params = yaml.safe_load(stream)

    for task in contest_params["tasks"]:
        if task["short_name"] == task_name:
            return create_processor(task["path"])

    raise Exception("Task %s not present in contest %s." %
                    (task_name, contest_name))


def main():
    """
    Execute the TaskProcessor.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--task_dir", help="task directory", required=True)
    parser.add_argument("--gen_dir", default=None,
                        help="target generation directory. "
                             "task_dir/auto.gen by default.")
    args = parser.parse_args()

    # Executing may result in an exception, which we want the caller to handle.
    TaskSandbox.execute(args.task_dir, args.gen_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
