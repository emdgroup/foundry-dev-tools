"""Implementation of the build2 API."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from foundry_dev_tools.clients.api_client import APIClient
from foundry_dev_tools.errors.compass import ResourceNotFoundError
from foundry_dev_tools.errors.handling import ErrorHandlingConfig
from foundry_dev_tools.utils import api_types
from foundry_dev_tools.utils.api_types import assert_in_literal

if TYPE_CHECKING:
    import requests


def _convert_job_spec_selections(
    job_spec_selections: list[api_types.DatasetsJobSpecSelection]
    | list[api_types.JobSpecJobSpecsSelection]
    | list[api_types.UpstreamJobSpecSelection]
    | list[api_types.ConnectingJobSpecSelection]
    | None,
    job_spec_selection_type: Literal["datasets", "jobSpecs", "upstream", "connecting"],
) -> list:
    """Converts existing job spec selections based on the provided 'job_spec_selection_type'."""
    result = []

    if job_spec_selections is None:
        return result

    for job_spec_selection in job_spec_selections:
        converted_job_spec_selection = {job_spec_selection_type: job_spec_selection, "type": job_spec_selection_type}

        result.append(converted_job_spec_selection)

    return result


class Build2Client(APIClient):
    """Build2Client class implements the 'build2' API."""

    api_name = "build2"

    def api_get_build_report(self, build_rid: api_types.Rid, **kwargs) -> requests.Response:
        """Returns the build report.

        Args:
            build_rid: the build RID
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "GET",
            "info/builds2/" + build_rid,
            error_handling=ErrorHandlingConfig({204: ResourceNotFoundError}),
            **kwargs,
        )

    def api_get_job_report(self, job_rid: api_types.Rid, **kwargs) -> requests.Response:
        """Returns the report for a job.

        Args:
            job_rid: the job RID
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        """
        return self.api_request(
            "GET",
            "info/jobs3/" + job_rid,
            error_handling=ErrorHandlingConfig({204: ResourceNotFoundError}),
            **kwargs,
        )

    def api_submit_build(
        self,
        datasets_job_spec_selections: list[api_types.DatasetsJobSpecSelection] | None = None,
        job_spec_job_spec_selections: list[api_types.JobSpecJobSpecsSelection] | None = None,
        upstream_job_spec_selections: list[api_types.UpstreamJobSpecSelection] | None = None,
        connecting_job_spec_selections: list[api_types.ConnectingJobSpecSelection] | None = None,
        submission_id: str | None = None,
        build_group_rid: str | None = None,
        branch: api_types.DatasetBranch = "master",
        branch_fallbacks: set[api_types.DatasetBranch] | None = None,
        force_build: bool | None = None,
        ignore_build_policy: bool | None = False,
        finish_on_failure: bool | None = None,
        num_run_job_attempts: int | None = None,
        force_retry: bool | None = None,
        retry_backoff_duration: int | None = None,
        build_parameters: dict[str, Any] | None = None,
        suppress_notifications: bool | None = None,
        exceeded_duration_mode: api_types.ExceededDurationMode | None = None,
        input_failure_strategies: list[api_types.InputStrategy] | None = None,
        output_queue_strategy: api_types.OutputQueueStrategy | None = None,
        **kwargs,
    ) -> requests.Response:
        """Request to submit a build.

        Args:
            datasets_job_spec_selections: A set of datasets job spec selections for which the build service
                will determine the job specs to run, based on the dataset rids provided
            job_spec_job_spec_selections: A set of job spec job spec selections for which the build service
                will directly run the specified job spec rids
            upstream_job_spec_selections: A set of upstream job spec selections for which downstream dataset rids
                can be defined to start from. A separate list of dataset rids to ignore can be specified as well
                in order to prevent further exploration of the dependency graph on the given dataset rids
            connecting_job_spec_selections: A set of connecting job spec selections for which the build service
                will determine the job specs in between the list of upstream and downstream dataset rids.
                Additionally, a list for ignoring dataset rids can be passed along
                on which to prevent further exploration of the dependency graph
            submission_id: Optional submission identifier to uniquely identify a new build submission
            build_group_rid: Specify in order to join a build group which serves as a collector
                acting as if all the builds sharing the same build group were submitted as a single build
            branch: The branch that all jobs in this build are run on. Defaults to `master` if not specified
            branch_fallbacks: Fallback branches to use when resolving dataset properties for input specs
            force_build: When set to 'True', will ignore staleness checking when running this build
                and runs all job specs regardless of whether they are stale
            ignore_build_policy: When set to 'True', will allow for jobs to be run
                even if their build policy is not satisfied
            finish_on_failure: When set to 'True', will abort all other jobs and finish the build
                if any single job fails
            num_run_job_attempts: The maximum number of attempts a job should be run when it fails
                due to a retryable error. If a value <= 1 is specified, jobs will never be retried.
            force_retry: When set to 'True', all errors will be treated as retryable.
                The maximum number of retry attempts is strictly given by `num_run_job_attempts`
            retry_backoff_duration: The duration in seconds to wait before attempting to run a job again.
                If not present, jobs will be retried immediately
            build_parameters: Parameters to apply to every job in the build
                which typically serve as "configuration" for the workers
            suppress_notifications: When set to 'True', the user will not receive a notification
                when the build is complete
            exceeded_duration_mode: The action taken when job with custom expiration has expired. Defaults to 'CANCEL'
                if not provided at all
            input_failure_strategies: Overwrites the strategy for each input dataset in case its build fails.
            output_queue_strategy: Defines the output queueing behaviour of the jobs that are created by this build.
                Defaults to 'QUEUE_UP' for builds with BatchVariant job specs and 'SUPERSEDE'
                for builds with LongRunningVariant job specs.
            **kwargs: gets passed to :py:meth:`APIClient.api_request`

        Returns:
            requests.Response:
                the response contains a json which returns information to the build submitted previously.
        """
        if exceeded_duration_mode:
            assert_in_literal(exceeded_duration_mode, api_types.ExceededDurationMode, "exceeded_duration_mode")

        for input_strategy in input_failure_strategies or {}:
            assert_in_literal(
                input_strategy["failureStrategy"],
                api_types.InputFailureStrategy,
                "input_failure_strategies['failureStrategy']",
            )

        if output_queue_strategy:
            assert_in_literal(output_queue_strategy, api_types.OutputQueueStrategy, "output_queue_strategy")

        datasets_job_spec_selections = _convert_job_spec_selections(datasets_job_spec_selections, "datasets")
        job_spec_job_spec_selections = _convert_job_spec_selections(job_spec_job_spec_selections, "jobSpecs")
        upstream_job_spec_selections = _convert_job_spec_selections(upstream_job_spec_selections, "upstream")
        connecting_job_spec_selections = _convert_job_spec_selections(connecting_job_spec_selections, "connecting")

        job_spec_selections = (
            datasets_job_spec_selections
            + job_spec_job_spec_selections
            + upstream_job_spec_selections
            + connecting_job_spec_selections
        )

        body = {
            "jobSpecSelections": job_spec_selections,
            "submissionId": submission_id,
            "buildGroupRid": build_group_rid,
            "branch": branch,
            "branchFallbacks": {"branches": list(branch_fallbacks) if branch_fallbacks else []},
            "forceBuild": force_build,
            "ignoreBuildPolicy": ignore_build_policy,
            "finishOnFailures": finish_on_failure,
            "numRunJobAttempts": num_run_job_attempts,
            "forceRetry": force_retry,
            "retryBackoffDuration": retry_backoff_duration,
            "buildParameters": build_parameters or {},
            "suppressNotifications": suppress_notifications,
            "exceededDurationMode": exceeded_duration_mode,
            "inputFailureStrategies": list(input_failure_strategies) if input_failure_strategies else [],
            "outputQueueStrategy": output_queue_strategy,
        }

        return self.api_request(
            "POST",
            "manager/submitBuild",
            json=body,
            **kwargs,
        )

    def submit_dataset_build(
        self,
        dataset_rid: api_types.DatasetRid,
        branch: api_types.DatasetBranch = "master",
        force_build: bool | None = False,
    ) -> dict:
        """Submit a dataset build.

        Args:
            dataset_rid: The resource identifier of the dataset to build
            branch: The branch that all jobs in this build are run on. Defaults to `master` if not specified
            force_build: When set to 'True', will ignore staleness checking when running this build
                and runs all job specs regardless of whether they are stale. Defaults to 'False'

        Returns:
            dict:

        .. code-block:: python

           {
                "isNewSubmission": "<submission-id>",
                "buildRid": "<...>",
                "buildGroupRid": "<build-group-rid>",
                "jobsCreated": {
                    "<job-spec-rid>": "<job-rid>",
                    ...
                },
                "jobsInOtherBuilds": {
                    "<job-spec-rid>": "<job-rid>",
                    ...
                }
           }

        """
        datasets_job_spec_selections = [{"datasetRids": [dataset_rid], "isRequired": True}]

        return self.api_submit_build(
            datasets_job_spec_selections=datasets_job_spec_selections, branch=branch, force_build=force_build
        ).json()
