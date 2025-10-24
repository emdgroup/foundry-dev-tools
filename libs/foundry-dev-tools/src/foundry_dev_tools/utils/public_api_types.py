"""API Types for the publicly documented Foundry API.

IMPORTANT: These TypedDict definitions are for type-checking purposes only.
They do NOT perform runtime validation. The actual API response is returned
as-is from response.json() without any schema enforcement.

These types are based on the official Palantir API documentation:
https://www.palantir.com/docs/foundry/api/v2/

If the API changes, mypy/pyright will catch mismatches in calling code,
but no runtime errors will occur if the API returns unexpected fields.
"""

from typing import NotRequired, TypedDict

from foundry_dev_tools.utils import api_types

OntologyRid = api_types.Rid


class Ontology(TypedDict):
    """Ontology Object as returned by the API."""

    apiname: str
    displayName: str
    description: str
    rid: api_types.Rid


class Dataset(TypedDict):
    """Dataset metadata from GET /api/v2/datasets/{datasetRid}.

    https://www.palantir.com/docs/foundry/api/v2/datasets-v2-resources/datasets/get-dataset/
    """

    rid: api_types.Rid
    name: str
    parentFolderRid: api_types.FolderRid


class DatasetBranch(TypedDict):
    """Dataset branch from GET /api/v2/datasets/{datasetRid}/branches/{branchName}.

    https://www.palantir.com/docs/foundry/api/v2/datasets-v2-resources/branches/get-branch/
    """

    name: str
    transactionRid: NotRequired[api_types.TransactionRid]


class DatasetTransaction(TypedDict):
    """Dataset transaction from GET /api/v2/datasets/{datasetRid}/transactions.

    https://www.palantir.com/docs/foundry/api/v2/datasets-v2-resources/transactions/get-transaction/
    """

    rid: api_types.TransactionRid
    transactionType: api_types.TransactionType
    status: api_types.TransactionStatus


class DatasetFile(TypedDict):
    """Dataset file from GET /api/v2/datasets/{datasetRid}/files.

    https://www.palantir.com/docs/foundry/api/v2/datasets-v2-resources/files/get-file/
    """

    path: str
    transactionRid: api_types.TransactionRid
    sizeBytes: NotRequired[str]
    updatedTime: str
