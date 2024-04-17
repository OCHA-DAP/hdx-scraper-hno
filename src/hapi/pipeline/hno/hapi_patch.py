import json
import logging
import re
from abc import ABC
from os import getenv
from os.path import basename
from time import sleep
from typing import Any

from github import Auth, Github, UnknownObjectException

logger = logging.getLogger(__name__)


class HAPIPatchError(Exception):
    pass


class HAPIPatch(ABC):
    """Create patch JSON files in teh HAPI GitHub repository.

    Args:
        hapi_repo (str): GitHub repository in the form ORG/REPO
    """

    def __init__(self, hapi_repo: str) -> None:
        self.hapi_repo = hapi_repo
        self.lock_name = "LOCK"
        self.lock_value = "LOCKED"
        self.patch_regex = re.compile(r"hapi_patch_(\d*).*.json")
        self.github = Github(auth=self.get_auth())
        self.repo = self.github.get_repo(self.hapi_repo)
        self.acquire_lock()
        self.sequence_number = self.get_sequence_number_from_repo()

    def __enter__(self) -> "HAPIPatch":
        """Allow usage of with.

        Returns:
            HAPIPatch: PatchGeneration object
        """
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Releases lock and closes GitHub connection.

        Args:
            exc_type (Any): Exception type
            exc_value (Any): Exception value
            traceback (Any): Traceback

        Returns:
            None
        """
        self.release_lock()
        self.github.close()

    @staticmethod
    def get_auth() -> Auth.Token:
        """Get GitHub token.

        Returns:
            Token: GitHub token for use with PyGithub
        """
        github_token = getenv("GITHUB_TOKEN")
        return Auth.Token(github_token)

    def acquire_lock(self) -> None:
        """Acquire lock on GitHub repository. This uses a GitHub Actions
        variable.

        Returns:
            None
        """
        logger.info("Checking for lock on HAPI patch repository...")
        attempts = 0
        while attempts < 100:
            try:
                attempts += 1
                value = self.repo.get_variable(self.lock_name).value
                logger.info(
                    f"Attempt {attempts}: HAPI patch repository is {value}. Trying again in one minute."
                )
                sleep(60)
            except UnknownObjectException:
                break
        if attempts == 100:
            raise HAPIPatchError("Could not lock HAPI patch repository!")
        logger.info("Locking HAPI patch repository.")
        self.repo.create_variable(self.lock_name, self.lock_value)
        value = self.repo.get_variable(self.lock_name).value
        logger.info(f"HAPI patch repository is {value}.")

    def release_lock(self) -> None:
        """Release lock on GitHub repository. Deletes GitHub Actions
        variable.

        Returns:
            None
        """
        self.repo.delete_variable(self.lock_name)
        logger.info("HAPI patch repository is UNLOCKED.")

    def get_sequence_number_from_repo(self) -> int:
        """Get the maximum sequence number of all the patch files in the GitHub
        repository.

        Returns:
            int: Sequence number
        """
        max_sequence_no = 0
        for file in self.repo.get_contents(""):
            if file.type == "dir":
                continue
            filename = basename(file.path)
            match = self.patch_regex.match(filename)
            if match:
                sequence_no = int(match.group(1))
                if sequence_no > max_sequence_no:
                    max_sequence_no = sequence_no
        return max_sequence_no + 1

    def get_sequence_number(self):
        """Get the current sequence number.

        Returns:
            int: Sequence number
        """
        return self.sequence_number

    def create(self, suffix: str, patch: Any, **kwargs) -> None:
        """Create the patch JSON file in teh GitHub repository.

        Args:
            suffix (str): Suffix to append to the filename
            patch (Any): Python patch object
            **kwargs: Additional arguments to pass to PyGithub create_file

        Returns:
            None
        """
        filename = f"hapi_patch_{self.sequence_number}_{suffix}.json"
        message = f"Creating {filename}"
        logger.info(f"{message} in GitHub repository.")
        content = json.dumps(patch, indent=None, separators=(",", ":"))
        self.repo.create_file(filename, message, content, **kwargs)
        self.sequence_number += 1
