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

from datetime import datetime
import os
import sys
import yaml


_requests_dir = "/var/lib/gitolite3/requests"


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

    try:
        with open(request_path, "w") as stream:
            stream.write(yaml_info)
    except Exception:
        return 4

    return 0


if __name__ == "__main__":
    return_code = main()
    if return_code != 0:
        print "Internal error occurred. " \
              "Please report to the system administrator. " \
              "Return code %s" % return_code
    sys.exit(return_code)
