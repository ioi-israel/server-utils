#!/usr/bin/env python2
"""
TaskSandbox is in charge of running a TaskProcessor (from task_utils package)
inside a sandbox, to generate all relevant files.
"""

import argparse
import os
import subprocess
import sys
from task_utils.processing import TaskProcessor


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
        """

        if not os.path.isdir(task_dir):
            raise Exception("Task directory not found: %s" % task_dir)

        if gen_dir is None:
            gen_dir = os.path.join(task_dir, "auto.gen")

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
        max_time = 120
        max_time_wall = 120

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
                            "Return code: %s\n"
                            "Stdout: %s\n"
                            "Stderr: %s\n" %
                            (return_code, stdout, stderr))


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
