#!/usr/bin/env python2

"""
Update tasks and contests on the server, safe from race conditions.
Uses a flufl lock in the path indicated by config.

Use the "with" keyword:

    with SafeUpdater() as updater:
        updater.clone_repo("devs/joe/task1")

As a standalone, use the command line arguments described by argparse.
"""

from __future__ import unicode_literals

import argparse
import os
import subprocess
import sys
import yaml

from flufl.lock import Lock

from server_utils.cms.scripts.DatabaseUtils import get_contest_tasks, \
    remove_submissions, add_submissions
from server_utils.config import LOCK_FILE, LOCK_LIFETIME, LOCK_TIMEOUT, \
    CLONE_DIR
from server_utils.tasks.TaskSandbox import TaskSandbox, create_processor


class SafeUpdater(object):
    """
    Container for the safe update functionality.
    """

    def __init__(self, lifetime=LOCK_LIFETIME, timeout=LOCK_TIMEOUT):
        """
        Create a SafeUpdater with the given lock lifetime and timeout
        (see flufl.lock documentation). The defaults are the lifetime
        and timeout found in the config.
        """
        self.lock = Lock(LOCK_FILE, lifetime=lifetime)
        self.timeout = timeout

    def clone_repo(self, repo):
        """
        Clone a repository on the disk.
        The parent directories are created under the cloning directory from
        config. For example, the repository name might be "devs/joe/task1".
        If "devs/joe" was not a directory under the cloning directory, it is
        created.

        Raise an exception on failure.
        """

        # Get the full path for "devs/joe/task1".
        repo_path = os.path.abspath(os.path.join(CLONE_DIR, repo))

        # Sanity check - make sure the path is actually in the cloning
        # directory.
        if not repo_path.startswith(CLONE_DIR):
            raise Exception("Illegal base path for repo: %s" % repo_path)

        # Clone the task into the desired directory.
        # git will create the subdirectories if needed.
        SafeUpdater.run(["git", "clone",
                         "gitolite3@localhost:%s" % repo,
                         repo_path])

    def update_repo(self, repo, allow_clone):
        """
        Update a repository on the disk. If allow_clone is set, and the
        repository doesn't exist on the disk yet, it will be cloned
        with the clone_repo method.

        Raise an exception on failure.
        """

        # Clone if needed. If the repository doesn't exist and we shouldn't
        # clone, raise an error for the caller.
        repo_path = os.path.join(CLONE_DIR, repo)
        if not os.path.isdir(repo_path):
            if allow_clone:
                # We only need to clone in this case.
                # Pulling (below) is not relevant.
                self.clone_repo(repo)
                return
            else:
                raise Exception("Directory doesn't exist, "
                                "and allow_clone is false: %s" % repo_path)

        # Make sure the repository is up to date.
        # We temporarily change the working directory, for git.
        old_working_dir = os.getcwd()
        os.chdir(repo_path)
        try:
            # We want to "git pull" here, except that repositories may have
            # been updated with force, and we want the newest version.
            SafeUpdater.run(["git", "fetch", "origin"])
            SafeUpdater.run(["git", "reset", "--hard", "origin/master"])
        finally:
            os.chdir(old_working_dir)

    def generate_task(self, repo, update, allow_clone, gen_dir=None):
        """
        Generate a task on the disk with TaskSandbox into gen_dir.
        If update is true, we first update the task.
        If both update and allow_clone are true, and the task
        doesn't exist on the disk yet, it will be cloned with
        the clone_repo method.

        If gen_dir is not specified, the default of TaskSandbox is used
        (auto.gen inside the task directory).

        Raise an exception on failure.
        """

        if update:
            self.update_repo(repo, allow_clone)

        repo_path = os.path.abspath(os.path.join(CLONE_DIR, repo))
        if not os.path.isdir(repo_path):
            raise Exception("Task directory not found: %s" % repo_path)

        TaskSandbox.execute(repo_path, gen_dir=gen_dir)

    def update_contest(self, repo, update, generate_new, add_users,
                       update_users, auto_submit, auto_submit_new):
        """
        Update a contest and its tasks on the database.
        This should be done after generating newly updated tasks
        with TaskSandbox, in order to update CMS.

        Tasks that exist in the contest and are not yet cloned,
        are cloned and generated, if generate_new is true.

        The contest repository itself is updated (cloned if needed),
        if update is true.

        If update_users is true, the users repository is updated,
        and the contest's users are updated. Users are never modified
        or deleted (this requires manual action).

        auto_submit_tasks is a set/list of task repositories for which
        auto_submit will be invoked.

        If auto_submit_new is given, auto_submit will also be invoked
        for tasks that were not in the contest before.

        Raise an exception on failure.
        """

        # Update/clone contest.
        if update:
            self.update_repo(repo, allow_clone=True)

        # Get contest module.
        repo_path = os.path.abspath(os.path.join(CLONE_DIR, repo))
        module_path = os.path.join(repo_path, "module.yaml")

        # Read contest params.
        with open(module_path) as stream:
            contest_params = yaml.safe_load(stream)

        # Update/clone users, and add them.
        if add_users:
            self.add_new_users(contest_params["users_file"], update_users)

        if generate_new:
            # Clone and generate tasks that are not yet present.
            for task in contest_params["tasks"]:
                task_repo = task["path"]
                task_path = os.path.join(CLONE_DIR, task_repo)
                if not os.path.isdir(task_path):
                    self.generate_task(task_repo, update=True,
                                       allow_clone=True)

        # Fetch the tasks that were already in the contest before.
        # If an exception is raised, this contest is not yet in the database.
        contest_name = contest_params["short_name"]
        try:
            existing_tasks = set(get_contest_tasks(contest_name))
        except Exception:
            existing_tasks = set()

        # Note: cmsImportContest drops participations when updating
        # a contest. We can give the --update-contest flag because
        # our cmsImportContest script was modified to ignore
        # participations. See issue #775.
        SafeUpdater.run(["cmsImportContest",
                         "--import-tasks",
                         "--update-tasks",
                         "--update-contest",
                         repo_path])

        # Invoke auto_submit for every task that didn't exist
        # in the contest before, and every task in auto_submit.
        for task in contest_params["tasks"]:
            task_name = task["short_name"]
            task_path = task["path"]
            if (auto_submit_new and task_name not in existing_tasks) or \
               task_path in auto_submit:
                self.auto_submit(contest_name, task)

    def auto_submit(self, contest_name, task_info):
        """
        Perform auto submission for the given contest and task,
        removing previous auto submissions from the database.
        If the task does not specify any solutions to auto submit,
        do nothing.
        """

        username = "autotester"
        task_name = task_info["short_name"]
        task_dir = os.path.join(CLONE_DIR, task_info["path"])
        processor = create_processor(task_dir)

        # For each submission, we convert the list of files to a dictionary
        # that maps the submission filename to the path. For example:
        # {"Task.%l": "path/to/sol.cpp"}
        # This relies on the type being batch, with a single file
        # per submission.
        auto_submit_items = []
        for item in processor.get_auto_submit_items():
            file_path = item["files"][0]
            auto_submit_items += [{"Task.%l": file_path}]

        if not auto_submit_items:
            return

        if not remove_submissions(contest_name, task_name, username):
            raise Exception("Auto submission failed: could not remove old "
                            "submissions, they are in progress.")
        add_submissions(contest_name, task_name, username, auto_submit_items)

    def add_new_users(self, users_file, update_repo):
        """
        Add the users in the given YAML path to the database.
        Users that already exist are ignored.
        This never deletes or modifies existing users.

        If update_repo is true, update/clone the users repository first.

        Raise an exception on failure.
        """

        # Update the users repository.
        if update_repo:
            self.update_repo("users", allow_clone=True)

        # Get the information from the users file.
        yaml_path = os.path.join(CLONE_DIR, users_file)
        with open(yaml_path) as stream:
            users = yaml.safe_load(stream)

        # Try to insert each user.
        # The script cmsAddUser returns 1 if the user already exists.
        for user in users:
            return_code, stdout, stderr = SafeUpdater.run(
                ["cmsAddUser", user["first_name"], user["last_name"],
                 user["username"], "-p", user["password"]],
                fail_abort=False)

            if return_code not in (0, 1):
                raise Exception("cmsAddUser failed.\n"
                                "Return code: %s\n"
                                "Stdout: %s\n"
                                "Stderr: %s\n" %
                                (return_code, stdout, stderr))

    def __enter__(self):
        """
        Lock when starting a "with" block.
        """
        self.lock.lock(timeout=self.timeout)
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        """
        Unlock when finishing a "with" block.
        Any exceptions are raised to the caller.
        """
        self.lock.unlock()
        return False

    @staticmethod
    def run(commands, input_string="", fail_abort=True):
        """
        Run the given commands as a subprocess, wait for it to finish.
        If fail_abort is set, then a non-zero return code will trigger
        an exception.
        Return (return_code, stdout, stderr).
        """
        process = subprocess.Popen(commands,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate(input=input_string)
        return_code = process.returncode
        if return_code != 0 and fail_abort:
            raise Exception("Command returned non-zero: %s\n"
                            "Return code: %s\n"
                            "Stdout: %s\n"
                            "Stderr: %s\n" %
                            (commands, return_code, stdout, stderr))
        return (return_code, stdout, stderr)


def main():
    """
    Import/update a contest in the database.
    This is done with SafeUpdater to avoid race conditions.

    Raise an exception on failure.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--contest",
                        help="contest to import",
                        required=True)
    parser.add_argument("--update_repos",
                        help="update relevant clones",
                        action="store_true")
    parser.add_argument("--generate_tasks",
                        help="generate relevant tasks",
                        action="store_true")
    parser.add_argument("--add_users",
                        help="add contest's users",
                        action="store_true")
    args = parser.parse_args()

    with SafeUpdater() as updater:
        updater.update_contest(args.contest, args.update_repos,
                               args.generate_tasks, args.add_users,
                               args.update_repos)

    return 0


if __name__ == "__main__":
    sys.exit(main())
