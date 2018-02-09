#!/usr/bin/env python2

"""
Generate a YAML file with users and passwords.

Usage: GenerateUsers.py --names names.txt --old old_file.yaml
                        --target targetfile.yaml

Users are generated from the given list of names: the full name "John Doe"
becomes the username "john.doe". Passwords are generated randomly.

Users that exist in old_file.yaml will keep their passwords (optional).
Their names will be printed for verification.

Duplicates cause an error: if two students have the same name, modify manually
in names.txt.
"""


import argparse
import logging
import os
import random
import string
import sys
import yaml

import cms.log


logger = logging.getLogger(__name__)


def confirm_or_abort():
    """
    Read a line from the terminal.
    If it is anything but "Y" or "y", raise an exception.
    """
    if raw_input().lower() != "y":
        raise Exception("User aborted.")


def validate_paths(names_path, target_path, old_path):
    """
    Validate the given paths:
    names_path must be an existing file.
    If old_path is not none, it must be an existing file.
    target_path must not exist.

    Raise an exception if not valid.
    """

    if old_path is not None:
        logger.info("Using old file: %s", old_path)
    else:
        logger.warning("Not using an old file. Continue? [y/n]")
        confirm_or_abort()

    if not os.path.isfile(names_path):
        raise Exception("Names file does not exist: %s" % names_path)

    if old_path is not None and not os.path.isfile(old_path):
        raise Exception("Old file does not exist: %s" % old_path)

    if os.path.isfile(target_path):
        raise Exception("Target file exists: %s" % target_path)


def names_to_usernames(names):
    """
    Take the given list of names and convert it to usernames.
    "John Doe" -> "john.doe"
    Each name is stripped before conversion, then split by spaces.
    If the name contains anything except letters and spaces,
    raise an exception.
    If duplicate names or invalid characters are found, raise an exception.
    """

    allowed_chars = set(string.ascii_letters + " ")

    usernames = set()
    for name in names:
        name = name.strip()

        # Empty or comment.
        if not name or name.startswith("#"):
            continue

        # Illegal characters.
        if not set(name).issubset(allowed_chars):
            raise Exception("Invalid characters found: %s" % name)

        name_parts = name.lower().split()

        # Invalid name format (expected full name).
        if len(name_parts) <= 1:
            raise Exception("Too few parts: %s" % name_parts)

        # Convert to username.
        username = ".".join(name_parts)
        if username in usernames:
            raise Exception("Duplicate: %s" % username)
        usernames.add(username)

    return list(usernames)


def create_password():
    """
    Create a new password.
    """
    target_length = 10
    full_charset = set(string.ascii_uppercase + string.digits)
    exclude = {'0', 'O', '1', 'I', '7'}
    allowed_chars = list(full_charset - exclude)
    return "".join(random.choice(allowed_chars) for _ in xrange(target_length))


def get_user(username, password=None):
    """
    Create a user dictionary (with fields for username and password).
    If password is None, a new one is created. Otherwise, the given one
    is used.
    """

    if password is None:
        password = create_password()
    return {"username": username, "password": password}


def generate(names_path, target_path, old_path):
    """
    Generate users from the given paths.
    names_path must be an existing file.
    If old_path is not none, it must be an existing file.
    target_path must not exist.
    """

    logger.info("Generating users from %s into %s", names_path, target_path)
    validate_paths(names_path, target_path, old_path)

    # Load old users and create a user->password dictionary.
    if old_path is not None:
        with open(old_path) as stream:
            old_users = yaml.safe_load(stream)
        old_user_to_password = {user["username"]: user["password"]
                                for user in old_users}
        logger.info("Loaded %s old users.", len(old_users))
    else:
        old_user_to_password = {}

    # Load new names.
    with open(names_path) as stream:
        new_names = stream.read().splitlines()

    # Convert names to usernames.
    new_usernames = names_to_usernames(new_names)

    # Report existing usernames for verification.
    existing_usernames = [username for username in new_usernames
                          if username in old_user_to_password]
    if existing_usernames:
        logger.info("Found %s existing usernames:", len(existing_usernames))
        for username in existing_usernames:
            logger.info(username)
        logger.info("Continue? [y/n]")
        confirm_or_abort()
    elif old_path is not None:
        logger.warning("No existing usernames! Continue? [y/n]")
        confirm_or_abort()

    # Create a dictionary with username and password for each username.
    users = [get_user(username, old_user_to_password.get(username))
             for username in new_usernames]

    # Write to the target file.
    with open(target_path, "w") as stream:
        yaml.safe_dump(users, stream)


def main():
    """
    Read arguments and invoke user generation. See file's docstring.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--names",
                        help="list of names to import",
                        required=True)
    parser.add_argument("--target",
                        help="target YAML file to create",
                        required=True)
    parser.add_argument("--old",
                        help="old YAML file for reused passwords",
                        default=None)
    args = parser.parse_args()
    generate(args.names, args.target, args.old)
    return 0


if __name__ == "__main__":
    sys.exit(main())
