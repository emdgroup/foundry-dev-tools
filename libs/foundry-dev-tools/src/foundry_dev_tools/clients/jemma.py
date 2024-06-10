"""Implementation of the jemma API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from foundry_dev_tools.clients.api_client import APIClient

if TYPE_CHECKING:
    import requests

    from foundry_dev_tools.utils.api_types import FoundryPath, Ref, RepositoryRid


class JemmaClient(APIClient):
    """JemmaClient class that implements the 'jemma' api."""

    api_name = "jemma"

    def start_checks_and_builds(
        self,
        repository_id: RepositoryRid,
        ref_name: Ref,
        commit_hash: Ref,
        file_paths: set[FoundryPath],
        **kwargs,
    ) -> dict:
        """Starts checks and builds.

        Args:
            repository_id: the repository id where the transform is located
            ref_name: the git ref_name for the branch
            commit_hash: the git commit hash
            file_paths: a list of python transform files
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            dict: the JSON API response
        """
        return self.api_post_build_jobs(
            [
                {
                    "name": "Checks",
                    "type": "exec",
                    "parameters": {
                        "repositoryTarget": {
                            "repositoryRid": repository_id,
                            "refName": ref_name,
                            "commitHash": commit_hash,
                        },
                    },
                    "reuseExistingJob": True,
                },
                {
                    "name": "Build initialization",
                    "type": "foundry-run-build",
                    "parameters": {
                        "fallbackBranches": [],
                        "filePaths": list(file_paths),
                        "rids": [],
                        "buildParameters": {},
                    },
                    "reuseExistingJob": True,
                },
            ],
            reuse_existing_jobs=True,
            **kwargs,
        ).json()

    def api_post_build_jobs(self, jobs: list[dict], reuse_existing_jobs: bool, **kwargs) -> requests.Response:
        """Post build jobs.

        Args:
            jobs: list of jobs
            reuse_existing_jobs: to reuse existing jobs set to true
            **kwargs: gets passed to :py:meth:`APIClient.api_request`
        """
        return self.api_request(
            "POST",
            "builds",
            json={"jobs": jobs, "reuseExistingJobs": reuse_existing_jobs},
            **kwargs,
        )
