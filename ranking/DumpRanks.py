#!/usr/bin/env python2

"""
Dump ranks into a JS file, to be included by index.html.
The JS file contains a global "raw_data" object, with fields:
- contests: list of included contests. Each is an object with:
    - name: name of the contest.
    - tasks: list of tasks to include in this contest.
- scores: map each username to an object with {task_name: score}.
"""


import argparse
import json
import sys
import time
import yaml

from cms.db import SessionGen
from cms.grading import task_score

from server_utils.cms.scripts.DatabaseUtils import get_contests, get_tasks, \
    get_users


def create_ranks_object(included_contests=None, excluded_contests=None,
                        included_tasks=None, excluded_tasks=None,
                        included_users=None, excluded_users=None):
    """
    Create a ranks object with the given parameters.
    """

    with SessionGen() as session:
        # Fetch all relevant data.
        contests = get_contests(session, included_contests, excluded_contests)
        tasks = get_tasks(session, included_tasks, excluded_tasks)
        name_to_task = {task.name: task for task in tasks}
        users = get_users(session, included_users, excluded_users)
        usernames_set = set(user.username for user in users)

        result = {"contests": [], "scores": {}}

        # Initialize users info.
        for username in usernames_set:
            result["scores"][username] = {}

        # Fill the result according to each contest.
        for contest in contests:
            # Relevant tasks only.
            contest_tasks = [task.name for task in contest.tasks
                             if task.name in name_to_task]

            # Don't include empty contests (where all tasks were excluded).
            if not contest_tasks:
                continue

            # Contest information.
            result["contests"] += [{
                "name": contest.name,
                "tasks": contest_tasks
            }]

            # Submission information for each user and each task.
            for participation in contest.participations:
                username = participation.user.username
                # Skip irrelevant users.
                if username not in usernames_set:
                    continue

                # Get the tasks this user submitted to.
                submitted_task_names = set(submission.task.name for submission
                                           in participation.submissions)

                for task_name in contest_tasks:
                    # If the user did not submit to this task, we don't write
                    # anything (this is distinct from getting 0).
                    if task_name not in submitted_task_names:
                        continue

                    task = name_to_task[task_name]
                    score, partial = task_score(participation, task)
                    score = round(score, task.score_precision)
                    score_string = str(score)
                    if partial:
                        score_string += "*"

                    result["scores"][username][task_name] = score_string

        return result


def dump_ranks_js(path, ranks_object):
    """
    Dump the given ranks object to the given path as a JS file.
    The JS file contains only "var raw_data = <object>;".
    """
    js_str = "var raw_data = %s;" % json.dumps(ranks_object) + \
             "var scores_timestamp = %d;" % int(time.time())
    with open(path, "w") as stream:
        stream.write(js_str)


def main():
    """
    Read settings file and dump ranks to a JS file.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("settings", help="settings file to use")
    args = parser.parse_args()
    settings_path = args.settings

    with open(settings_path) as stream:
        settings = yaml.safe_load(stream)

    target_path = settings["target_path"]

    if not target_path.endswith("scores.js"):
        raise Exception("Expected scores.js: %s" % target_path)

    ranks_object = create_ranks_object(
        settings.get("included_contests"),
        settings.get("excluded_contests"),
        settings.get("included_tasks"),
        settings.get("excluded_tasks"),
        settings.get("included_users"),
        settings.get("excluded_users"))

    dump_ranks_js(target_path, ranks_object)


if __name__ == "__main__":
    sys.exit(main())
