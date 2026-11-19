"""Helpers to resolve Foundry job information inside a lightweight transform runtime."""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from transforms.api import Output

logger = logging.getLogger(__name__)


def filter_kwargs_for_function(func: Callable[..., Any], kwargs_dict: dict[str, Any]) -> dict[str, Any]:
    """Filter kwargs to only include parameters that the function accepts.

    Args:
        func: the function whose signature is used for filtering
        kwargs_dict: the keyword arguments to filter
    """
    sig = inspect.signature(func)
    return {k: v for k, v in kwargs_dict.items() if k in sig.parameters}


class TransactionPath:
    """Path to a dataset transaction, usable without importing Foundry internal classes."""

    def __init__(self, dataset_rid: str, transaction_rid: str, logical_path: str | None = None):
        self._dataset_rid = dataset_rid
        self._transaction_rid = transaction_rid
        self.logical_path = logical_path

    @property
    def dataset_rid(self) -> str:
        """The dataset rid."""
        return self._dataset_rid

    @property
    def transaction_rid(self) -> str:
        """The transaction rid."""
        return self._transaction_rid

    @property
    def end_ref(self) -> str:
        """The end ref, which is the transaction rid."""
        return self.transaction_rid

    def __str__(self) -> str:
        path = f"datasets/{self.dataset_rid}/transactions/{self.transaction_rid}"
        if self.logical_path:
            path = f"{path}/{self.logical_path}"
        return path


class ViewPath:
    """Path to a dataset view, usable without importing Foundry internal classes."""

    def __init__(
        self,
        dataset_rid: str,
        end_ref: str,
        start_transaction: str | None = None,
        logical_path: str | None = None,
    ):
        self._dataset_rid = dataset_rid
        self._end_ref = end_ref
        self.start_transaction = start_transaction
        self.logical_path = logical_path

    @property
    def dataset_rid(self) -> str:
        """The dataset rid."""
        return self._dataset_rid

    @property
    def end_ref(self) -> str:
        """The end ref of the view."""
        return self._end_ref

    def __str__(self) -> str:
        path = f"datasets/{self.dataset_rid}/views/{self.end_ref}"
        if self.start_transaction:
            path += f"/startTransactionRid/{self.start_transaction}"
        path += "/files"
        if self.logical_path:
            path += f"/{self.logical_path}"
        return path


FoundryPath = TransactionPath | ViewPath


class FoundryPathWithBranchId:
    """A :py:data:`FoundryPath` together with a branch id."""

    def __init__(self, foundry_path: FoundryPath, branch_id: str | None):
        self._foundry_path = foundry_path
        self._branch_id = branch_id

    @property
    def foundry_path(self) -> FoundryPath:
        """The foundry path."""
        return self._foundry_path

    @property
    def branch_id(self) -> str | None:
        """The branch id."""
        return self._branch_id


def load_job_config() -> Any:  # noqa: ANN401
    """Load job configuration from the Foundry transforms runtime.

    Only available inside a Foundry lightweight transform.

    Raises:
        ModuleNotFoundError: if the Foundry transforms runtime is not available
    """
    try:
        from transforms.api._lightweight._job_spec import JobConfigFileStrategy
    except (ModuleNotFoundError, ImportError) as exc:
        msg = (
            "Attempting to resolve aliases in a Transforms runtime, but the transforms library is not installed"
            " or is on an outdated version. Please install or upgrade the transforms library."
            " This is only available inside a Foundry lightweight transform."
        )
        raise ModuleNotFoundError(msg) from exc
    return JobConfigFileStrategy()


def _find_matched_spec(
    alias: str,
    specs: list[dict[str, Any]],
    job_config: Any = None,  # noqa: ANN401
) -> tuple[str, dict[str, Any]]:
    """Find the matched specification for a given alias.

    Args:
        alias: the output alias
        specs: the output specs from the job config
        job_config: an already loaded job config, loaded via :py:func:`load_job_config` if None
    """
    if job_config is None:
        job_config = load_job_config()
    transform_parameter = job_config.get_transform_parameters().get(alias)

    if transform_parameter is None:
        msg = f"Alias '{alias}' not found in job spec"
        raise ValueError(msg)

    dataset_rid = transform_parameter.get("datasetRid")
    matched_spec = next(
        (spec for spec in specs if spec.get("datasetLocator", {}).get("datasetRid") == dataset_rid),
        None,
    )

    if not matched_spec:
        msg = f"Dataset rid {dataset_rid} not found in job config."
        raise ValueError(msg)
    return dataset_rid, matched_spec


def resolve_output_alias_flattened(alias: str) -> tuple[str, str, str | None, str]:
    """Resolve output alias and return the core components directly.

    Args:
        alias: The output alias to resolve.

    Returns:
        tuple: A tuple containing:
            - dataset_rid: The dataset RID
            - transaction_rid: The transaction RID
            - branch_id: The branch ID (can be None)
            - foundry_path_str: String representation of the foundry path
    """
    job_config = load_job_config()

    dataset_rid, matched_spec = _find_matched_spec(alias, job_config.get_output_spec(), job_config)

    transaction_rid = matched_spec["datasetLocator"]["datasetProperties"]["outputTransactionRid"]

    branch_id = job_config._config_data.get("branch")  # noqa: SLF001

    foundry_path_str = f"datasets/{dataset_rid}/transactions/{transaction_rid}"

    return dataset_rid, transaction_rid, branch_id, foundry_path_str


def get_output_foundry_path_components(output: Output) -> tuple[str, str, str | None, str]:
    """Extract core components from an Output object.

    Args:
        output: The Output object to extract components from.

    Returns:
        tuple: A tuple containing:
            - dataset_rid: The dataset RID
            - transaction_rid: The transaction RID
            - branch_id: The branch ID (can be None)
            - foundry_path_str: String representation of the foundry path
    """
    return resolve_output_alias_flattened(output._alias)  # noqa: SLF001


def _set_job_info_value(result: dict[str, Any], key: str, getter: Callable[[], Any]) -> None:
    """Sets ``result[key]`` to ``getter()``, logs a warning naming the key if it fails.

    Job info is only telemetry, a failure must never break the transform.
    """
    try:
        result[key] = getter()
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to extract %s: %s", key, e)


def prepare_job_info(out: Output, **kwargs) -> dict[str, Any]:
    """Build a job information dictionary from the current job config and an Output.

    Args:
        out: The primary Output object for which to collect job information.
        **kwargs: Additional key-value pairs to merge into the result. Keys that
            already exist from the job configuration are not overwritten; a warning
            is logged and the original values are kept.

    Returns:
        dict: A dictionary containing:
            - output_dataset_rid: RID of the output dataset
            - output_transaction_rid: RID of the output transaction
            - branch: name of the branch the code originates from
            - job_rid: RID of the Foundry job
            - sourceProvenance_details_code: Raw sourceProvenance.details.code block
            - inputs: Mapping of input names to dataset RIDs
            - outputs: Mapping of output names to dataset RIDs
            - any additional non-conflicting kwargs

    Example:
        >>> result = {
        ...     "output_dataset_rid": "ri.foundry.main.dataset.00000-00005",
        ...     "output_transaction_rid": "ri.foundry.main.transaction.00000-00123",
        ...     "branch": "master",
        ...     "job_rid": "ri.foundry.main.job.00000-00010",
        ...     "sourceProvenance_details_code": {
        ...         "repositoryRid": "ri.stemma.main.repository.00000-00000",
        ...         "versionId": "84e49aff909709b121250a682193328020f37435",
        ...         "file": "transforms-python/src/myproject/hello_world.py",
        ...         "lineNumber": 2137,
        ...     },
        ...     "inputs": {
        ...         "input_1": "ri.foundry.main.dataset.00000-00001",
        ...         "virtual_table_1": "ri.tables.main.table.00000-00002",
        ...         "input_2": "ri.foundry.main.dataset.00000-00004",
        ...     },
        ...     "outputs": {
        ...         "out": "ri.foundry.main.dataset.00000-00005",
        ...     },
        ...     **kwargs
        ... }
    """
    result: dict[str, Any] = {}

    try:
        dataset_rid, transaction_rid, _, _ = get_output_foundry_path_components(out)
        result["output_dataset_rid"] = dataset_rid
        result["output_transaction_rid"] = transaction_rid

        job_json = load_job_config().get_job_config_yaml()
        logger.info("job_json=%s", job_json)

        def job_configuration() -> dict[str, Any]:
            return (
                job_json.get("job-spec", {})
                .get("computationParameters", {})
                .get("transforms", {})
                .get("containerizedApplicationConfiguration", {})
                .get("standalone", {})
                .get("jobConfiguration", {})
            )

        _set_job_info_value(result, "branch", lambda: job_json.get("branch", ""))
        _set_job_info_value(result, "job_rid", lambda: job_json.get("job-rid", ""))
        _set_job_info_value(
            result,
            "sourceProvenance_details_code",
            lambda: job_json.get("job-spec", {}).get("sourceProvenance", {}).get("details", {}).get("code", {}),
        )
        _set_job_info_value(result, "inputs", lambda: job_configuration().get("inputs", {}))
        _set_job_info_value(result, "outputs", lambda: job_configuration().get("outputs", {}))

    except Exception as e:  # noqa: BLE001
        logger.warning("Unexpected failure in prepare_job_info: %s", e)

    # merged after the job info, so custom tags are kept even if the job config could not be read
    for k, v in kwargs.items():
        if k in result:
            logger.warning(
                "Key collision for '%s' between job-spec-derived value and kwargs. "
                "Keeping job-spec value and ignoring kwargs value.",
                k,
            )
            continue
        result[k] = v

    logger.info("Prepared job info - result=%s", result)

    return result
