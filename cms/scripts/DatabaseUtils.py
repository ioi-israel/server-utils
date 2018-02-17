#!/usr/bin/env python2

"""
Utilities for interacting with the CMS database.
"""


import os
import time

from cms import ServiceCoord
from cms.db import Contest, File, Participation, SessionGen, Submission, \
    Task, User
from cms.db.filecacher import FileCacher
from cms.grading.languagemanager import filename_to_language
from cms.io import RemoteServiceClient
from cmscommon.datetime import make_datetime


def get_user(session, username):
    """
    Return the User object with the given name.
    Raise an exception if not found.
    """
    user = session.query(User)\
        .filter(User.username == username)\
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
        .filter(Contest.name == contest_name)\
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
        .filter(Task.name == task_name)\
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
