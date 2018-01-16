#!/usr/bin/env python2

"""
This script places a request to update a task or contest following a gitolite
push. It should be in /var/lib/gitolite3/local/hooks/common, with the name
"post-receive".
See http://gitolite.com/gitolite/cookbook/#adding-other-non-update-hooks

Output is sent to the developer who pushed.

We assume the following variables are present in the environment:
GL_USER: The gitolite user who pushed.
GL_REPO: The full name of the repository, including slashes.
"""

from datetime import datetime, timedelta
import os
import sys
import yaml
import flufl.lock


_requests_dir = "/var/lib/gitolite3/requests"
_lock_lifetime = timedelta(seconds=3)
_lock_timeout = timedelta(seconds=10)


def main():
    """
    Put a request file according to the push that invoked this script.
    """
    env = os.environ
    if ("GL_REPO" not in env) or ("GL_USER" not in env):
        return 1

    repo = env["GL_REPO"]
    user = env["GL_USER"]

    if not repo or not user:
        return 2

    # When receiving an update from developer "joe" to the repository
    # devs/joe/repo, The request name is going to be in this format:
    # 2000-01-01.10.00.00_joe_devs.joe.repo.yaml
    time_string = datetime.strftime(datetime.now(), "%Y-%m-%d.%H.%M.%S")
    repo_name_path = repo.replace("/", ".")
    request_name = time_string + "_" + user + "_" + repo_name_path + ".yaml"
    request_path = os.path.join(_requests_dir, request_name)

    info = {
        "user": user,
        "repo": repo
    }

    try:
        yaml_info = yaml.safe_dump(info)
    except Exception:
        return 3

    lock_path = os.path.join(_requests_dir, ".lock")
    lock = flufl.lock.Lock(lock_path, lifetime=_lock_lifetime)

    # Try to acquire the lock with the defined timeout.
    # We don't use "with lock" because then we can't define custom timeout.
    try:
        lock.lock(timeout=_lock_timeout)
    except Exception:
        return 4

    # Try to write the file. Clean up the lock in any case.
    try:
        with open(request_path, "w") as stream:
            stream.write(yaml_info)
    except Exception:
        return 5
    finally:
        try:
            lock.unlock()
        except Exception:
            # This can happen if the lock expired.
            # We don't care about this for now, because it shouldn't
            # take so much time to write the YAML.
            pass

    return 0


if __name__ == "__main__":
    return_code = main()
    if return_code != 0:
        print "Internal error occurred. " \
              "Please report to the system administrator. " \
              "Return code %s" % return_code
    sys.exit(return_code)
