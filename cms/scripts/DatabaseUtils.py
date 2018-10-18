#!/usr/bin/env python2

"""
Utilities for interacting with the CMS database.
"""


import logging
import os
import time

from pytz import timezone

import cms.log

from cms import ServiceCoord
from cms.db import Contest, File, Participation, SessionGen, Submission, \
    Task, User, SubmissionResult, Dataset, FSObject
from cms.db.filecacher import FileCacher
from cms.grading.languagemanager import filename_to_language, get_language
from cms.io import RemoteServiceClient
from cmscommon.datetime import make_datetime


logger = logging.getLogger(__name__)


def get_user(session, username):
    """
    Return the User object with the given name.
    Raise an exception if not found.
    """
    user = session.query(User)\
        .filter(User.username == unicode(username))\
        .first()
    if user is None:
        raise Exception("User not found: %s" % username)
    return user


def get_contest(session, contest_name):
    """
    Return the Contest object with the given name.
    Raise an exception if not found.
    """
    contest = session.query(Contest)\
        .filter(Contest.name == unicode(contest_name))\
        .first()
    if contest is None:
        raise Exception("Contest not found: %s" % contest_name)
    return contest


def get_participation(session, contest, user):
    """
    Return the Participation object of the given contest name and username.
    Raise an exception if not found.
    """
    participation = session.query(Participation)\
        .filter(Participation.user_id == user.id)\
        .filter(Participation.contest_id == contest.id)\
        .first()
    if participation is None:
        raise Exception("No participation of %s in %s." %
                        (user.username, contest.name))
    return participation


def get_task(session, task_name, contest=None):
    """
    Return the Task object with the given name.
    Raise an exception if not found.

    If contest is supplied, check whether the task is in
    the given contest, and raise an exception if not.
    """
    task = session.query(Task)\
        .filter(Task.name == unicode(task_name))\
        .first()
    if task is None:
        raise Exception("Task not found: %s" % task_name)
    if contest is not None and task.contest_id != contest.id:
        raise Exception("Task %s not in %s." % (task_name, contest.name))
    return task


def get_user_task_submissions(session, participation, task):
    """
    Return a list of submissions made in the given participation
    to the given task.
    """
    return session.query(Submission)\
        .filter(Submission.participation_id == participation.id)\
        .filter(Submission.task_id == task.id)\
        .all()


def get_contests(session, included=None, excluded=None):
    """
    Get a list of all Contest objects.
    If "included" is given, it is expected to be a list of names,
    and then only those contests would be returned.

    In any case, the contest names in the "excluded" set are omitted
    from the result (default is none).
    """

    contests = session.query(Contest).all()

    if included is not None:
        name_to_contest = {contest.name: contest for contest in contests}
        contests = [name_to_contest[name] for name in included]

    if excluded is not None:
        contests = [contest for contest in contests
                    if contest.name not in excluded]

    return contests


def get_tasks(session, included=None, excluded=None):
    """
    Get a list of all Task objects.
    If "included" is given, it is expected to be a list of names,
    and then only those tasks would be returned.

    In any case, the task names in the "excluded" set are omitted
    from the result (default is none).
    """

    tasks = session.query(Task).all()

    if included is not None:
        name_to_task = {task.name: task for task in tasks}
        tasks = [name_to_task[name] for name in included]

    if excluded is not None:
        tasks = [task for task in tasks
                 if task.name not in excluded]

    return tasks


def get_users(session, included=None, excluded=None):
    """
    Get a list of all User objects.
    If "included" is given, it is expected to be a list of usernames,
    and then only those users would be returned.

    In any case, the usernames in the "excluded" set are omitted
    from the result (default is none).
    """

    users = session.query(User).all()

    if included is not None:
        name_to_user = {user.username: user for user in users}
        users = [name_to_user[name] for name in included]

    if excluded is not None:
        users = [user for user in users
                 if user.username not in excluded]

    return users


def get_contest_tasks(contest_name):
    """
    Get a list of the task names that belong to the given contest name.
    """

    with SessionGen() as session:
        contest = get_contest(session, contest_name)
        tasks = session.query(Task)\
            .filter(Task.contest_id == contest.id)\
            .all()
        return [task.name for task in tasks]


def remove_submissions(contest_name, task_name, username):
    """
    Remove the submissions of the given user in the given task and contest.
    This is intended for the automatic submission system:
    The user must have "autotester" in its name.

    Submissions are not removed if they are still in evaluation.

    Raise exception on failure.

    Return whether the submissions were deleted.
    """

    if "autotester" not in username:
        raise Exception("Not removing submissions of user %s, "
                        "they are not an autotester." % username)

    with SessionGen() as session:
        user = get_user(session, username)
        contest = get_contest(session, contest_name)
        participation = get_participation(session, contest, user)
        task = get_task(session, task_name, contest)
        submissions = get_user_task_submissions(session, participation, task)

        if not submissions:
            return True

        # If any submission is not yet scored, abort.
        for submission in submissions:
            result = submission.get_result()
            if result is None or result.score is None:
                return False

        # All submissions have been scored. Delete them.
        for submission in submissions:
            session.delete(submission)
        session.commit()
        return True


def add_submissions(contest_name, task_name, username, items):
    """
    Add submissions from the given user to the given task
    in the given contest. Each item corresponds to a submission,
    and should contain a dictionary which maps formatted file names
    to paths. For example, in batch tasks the format is "Task.%l",
    so one submission would be {"Task.%l": "path/to/task.cpp"}.
    """

    # We connect to evaluation service to try and notify it about
    # the new submissions. Otherwise, it will pick it up only on
    # the next sweep for missed operations.
    rs = RemoteServiceClient(ServiceCoord("EvaluationService", 0))
    rs.connect()

    with SessionGen() as session:
        user = get_user(session, username)
        contest = get_contest(session, contest_name)
        participation = get_participation(session, contest, user)
        task = get_task(session, task_name, contest)
        elements = set(format_element.filename for format_element in
                       task.submission_format)
        file_cacher = FileCacher()

        # We go over all submissions twice. First we validate the
        # submission format.
        for submission_dict in items:
            for (format_file_name, path) in submission_dict.iteritems():
                if format_file_name not in elements:
                    raise Exception("Unexpected submission file: %s. "
                                    "Expected elements: %s" %
                                    (format_file_name, elements))
                if not os.path.isfile(path):
                    raise Exception("File not found: %s" % path)

        # Now add to database.
        for submission_dict in items:
            if not submission_dict:
                continue

            timestamp = time.time()
            file_digests = {}
            language_name = None

            for (format_file_name, path) in submission_dict.iteritems():
                digest = file_cacher.put_file_from_path(
                    path,
                    "Submission file %s sent by %s at %d."
                    % (path, username, timestamp))
                file_digests[format_file_name] = digest

                current_language = filename_to_language(path)
                if current_language is not None:
                    language_name = current_language.name

            submission = Submission(make_datetime(timestamp), language_name,
                                    participation=participation, task=task)
            for filename, digest in file_digests.items():
                session.add(File(filename, digest, submission=submission))
            session.add(submission)
            session.commit()
            rs.new_submission(submission_id=submission.id)

    rs.disconnect()


def add_users(users_info, contest_name=None):
    """
    Add the given users to the database, if they don't exist.
    If contest_name is given and it exists, participations are created
    (for existing users, too).

    Each user info should be a dictionary with the fields:
    username, password. Optionally:
    first_name (default is empty).
    last_name (default is empty).
    hidden (default is false).
    unrestricted (default is false).
    """

    with SessionGen() as session:
        existing_users = session.query(User).all()
        existing_usernames = {user.username: user for user in existing_users}

        # If the contest does not exist, this raises an exception,
        # and participations will not be created.
        try:
            contest = get_contest(session, contest_name)
            participations = session.query(Participation)\
                .filter(Participation.contest_id == contest.id)\
                .all()
            existing_participations = set(participation.user.username
                                          for participation in participations)
        except Exception:
            contest = None
            existing_participations = set()

        for user_info in users_info:
            username = user_info["username"]

            # If this user exists, fetch the User database object.
            # Otherwise, create one.
            if username in existing_usernames:
                user = existing_usernames[username]
            else:
                first_name = user_info.get("first_name", "")
                last_name = user_info.get("last_name", "")
                password = user_info["password"]
                user = User(first_name=unicode(first_name),
                            last_name=unicode(last_name),
                            username=unicode(username),
                            password=unicode(password))
                session.add(user)

            # If the participation does not exist and the contest is given,
            # add it.
            if contest is not None and \
               username not in existing_participations:
                participation = Participation(
                    user=user,
                    contest=contest,
                    hidden=user_info.get("hidden", False),
                    unrestricted=user_info.get("unrestricted", False))
                session.add(participation)
        session.commit()


def export_submissions(target_dir, contest_names, overwrite=False,
                       make_dir=True):
    """
    Export all submissions from the given contests to the given directory.
    If overwrite is true, existing files are overwritten. Otherwise,
    raise an exception if a file exists.
    If make_dir is true, create all subdirectories needed for the
    following format. Otherwise, assume they exist.

    The files of each submission are put in a directory:
    target_dir/contest_name/task_name/user_name/submission_string/

    Where submission_string includes the date, time, task, user, score.
    For example:
    2018-01-01.10-00.1.task_name.username.score-100
    2018-01-01.10-00.2.task_name.username.compilation-fail
    """

    with SessionGen() as session:
        for contest_name in contest_names:
            contest = session.query(Contest)\
                .filter(Contest.name == unicode(contest_name))\
                .first()
            if contest is None:
                raise Exception("Contest not found: %s" % contest_name)

            logger.info("Querying database for submissions in contest %s...",
                        contest_name)

            submissions = session.query(Submission)\
                .filter(Participation.contest_id == contest.id)\
                .join(Submission.task)\
                .join(Submission.files)\
                .join(Submission.results)\
                .join(SubmissionResult.dataset)\
                .join(Submission.participation)\
                .join(Participation.user)\
                .filter(Dataset.id == Task.active_dataset_id)\
                .with_entities(Submission.id,
                               Submission.language,
                               Submission.timestamp,
                               SubmissionResult.score,
                               SubmissionResult.compilation_outcome,
                               File.filename,
                               File.digest,
                               User.username,
                               Task.name)\
                .all()

            logger.info("Found %d submissions. Saving...", len(submissions))

            for (index, row) in enumerate(submissions, 1):
                logger.info("Contest %s: saving submission (%d / %d)",
                            contest_name, index, len(submissions))

                # Get submission info and target file path.
                sid, language, timestamp, score, comp_outcome, filename,\
                    digest, username, task_name = row
                file_path = _get_submission_file_path(
                    target_dir, sid, language, timestamp, score, comp_outcome,
                    filename, username, task_name, contest_name)

                # Don't overwrite if not allowed.
                if not overwrite and os.path.exists(file_path):
                    raise Exception("File exists: %s" % file_path)

                # Make directories if necessary.
                if make_dir:
                    dir_path = os.path.dirname(file_path)
                    if not os.path.exists(dir_path):
                        os.makedirs(dir_path)

                # Save the file.
                fso = FSObject.get_from_digest(digest, session)
                with fso.get_lobject(mode="rb") as file_obj:
                    data = file_obj.read()
                    with open(file_path, "w") as stream:
                        stream.write(data)


def _get_submission_file_path(target_dir, sid, language, timestamp, score,
                              comp_outcome, filename, username, task_name,
                              contest_name):
    """
    Get the full file path for the given submission.
    See export_submissions.
    Timestamps are hard coded to convert from UTC to Asia/Jerusalem.
    """

    # Time in directory name.
    utc_stamp = timestamp.replace(tzinfo=timezone('UTC'))
    local_stamp = utc_stamp.astimezone(timezone('Asia/Jerusalem'))
    time_str = local_stamp.strftime("%Y-%m-%d.%H-%M")

    # Score in directory name.
    if comp_outcome == "fail":
        score_str = "compilation-failed"
    elif score is None:
        score_str = "score-none"
    else:
        # Round down if there is no need for precision.
        if score - int(score) < 0.01:
            score_str = "score-%d" % int(score)
        else:
            score_str = "score-%.02f" % score

    # Submission directory name.
    submission_string = "%s.%s.%s.%s.%s" % (time_str, sid, task_name,
                                            username, score_str)

    # Replace file name extension if needed.
    if filename.endswith(".%l"):
        filename = filename[:-3] + get_language(language).source_extension

    # Join everything.
    return os.path.join(target_dir, contest_name, task_name, username,
                        submission_string, filename)
