#!/usr/bin/env python2

"""
Update tasks and contests on the server, safe from race conditions.
Uses a flufl lock in the path indicated by config.

Use the "with" keyword:

    with SafeUpdater() as updater:
        updater.clone_repo("devs/joe/task1")
"""

import os
import subprocess
import yaml

from flufl.lock import Lock

from server_utils.config import LOCK_FILE, LOCK_LIFETIME, LOCK_TIMEOUT, \
    CLONE_DIR, USERS_FILE
from server_utils.tasks.TaskSandbox import TaskSandbox


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
            SafeUpdater.run(["git", "pull"])
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

    def update_contest(self, repo, update, generate_new, update_users):
        """
        Update a contest and its tasks on the database.
        This should be done after generating newly updated tasks
        with TaskSandbox, in order to update CMS.

        Tasks that exist in the contest and are not yet cloned,
        are cloned and generated, if generate_new is true.

        The contest repository itself is updated (cloned if needed),
        if update is true.

        If update_users is true, then self.update_users is called
        before everything else.

        Raise an exception on failure.
        """

        # Update/clone users.
        if update_users:
            self.update_users()

        # Update/clone contest.
        if update:
            self.update_repo(repo, allow_clone=True)

        # Get contest module.
        repo_path = os.path.abspath(os.path.join(CLONE_DIR, repo))
        module_path = os.path.join(repo_path, "module.yaml")

        # Read contest params.
        with open(module_path) as stream:
            contest_params = yaml.safe_load(stream)

        if generate_new:
            # Clone and generate tasks that are not yet present.
            for task in contest_params["tasks"]:
                task_repo = task["path"]
                task_path = os.path.join(CLONE_DIR, task_repo)
                if not os.path.isdir(task_path):
                    self.generate_task(task_repo, update=True,
                                       allow_clone=True)

        # Note: cmsImportContest drops participations when updating
        # a contest. It is important not to give the --update-contest flag.
        SafeUpdater.run(["cmsImportContest",
                         "--import-tasks",
                         "--update-tasks",
                         repo_path])

    def update_users(self):
        """
        Update the users repository. Read the config users file and
        add new users to the database. Note: this never deletes users.

        Raise an exception on failure.
        """

        # If the users repository is not cloned yet, there will be
        # no old users. Otherwise, get them from the file.
        old_users = []
        if os.path.isfile(USERS_FILE):
            with open(USERS_FILE) as stream:
                old_users = yaml.safe_load(stream)

        old_username_set = set(user["username"] for user in old_users)

        # Update/clone the users repository and get all users.
        self.update_repo("users", allow_clone=True)
        with open(USERS_FILE) as stream:
            all_users = yaml.safe_load(stream)

        # For every user that didn't exist previously, add it to CMS.
        for user in all_users:
            if user["username"] in old_username_set:
                continue
            SafeUpdater.run(["cmsAddUser",
                             user["first_name"],
                             user["last_name"],
                             user["username"],
                             "-p",
                             user["password"]])

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
