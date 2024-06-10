"""Defines types for the Foundry API, for better readability of the code."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal, TypedDict, Union

from foundry_dev_tools.utils.misc import EnumContainsMeta

Rid = str
"""A resource identifier."""

TransactionRid = Rid
"""A transaction resource identifier."""

DatasetRid = Rid
"""A dataset resource identifier."""

RepositoryRid = Rid
"""A repository resource identifier."""

FoundryPath = str
"""A path on foundry."""

PathInDataset = str
"""A path in a dataset."""

PathInRepository = str
"""A path in a repository."""

Ref = str
"""A git ref (branch,commit,tag,...)."""

DatasetBranch = str
"""Name of a branch in a dataset."""

View = Union[DatasetBranch, TransactionRid]
"""A view, which is a :py:attr:`~foundry_dev_tools.utils.api_types.Ref` or a :py:attr:`~foundry_dev_tools.utils.api_types.TransactionRid`."""  # noqa: E501

# TODO further typing?
FoundrySchema = dict[str, Any]


class FoundryTransaction(str, Enum, metaclass=EnumContainsMeta):
    """Foundry transaction types."""

    SNAPSHOT = "SNAPSHOT"
    """only new files are present after transaction"""
    UPDATE = "UPDATE"
    """replace files with same filename, keep present files"""
    APPEND = "APPEND"
    """add files that are not present yet"""

    DELETE = "DELETE"
    """Every file in the transaction will be removed."""

    def __str__(self) -> str:
        return self.value


class SqlDialect(str, Enum, metaclass=EnumContainsMeta):
    """The SQL Dialect for Foundry SQL queries."""

    ANSI = "ANSI"
    SPARK = "SPARK"

    def __str__(self) -> str:
        return self.value


class SQLReturnType(str, Enum, metaclass=EnumContainsMeta):
    """The return_types for sql queries.

    PANDAS, PD: :external+pandas:py:class:`pandas.DataFrame` (pandas)
    ARROW, PYARROW, PA: :external+pyarrow:py:class:`pyarrow.Table` (arrow)
    SPARK, PYSPARK: :external+spark:py:class:`~pyspark.sql.DataFrame` (spark)
    RAW: Tuple of (foundry_schema, data) (raw) (can only be used in legacy)
    """

    PANDAS = "pandas"
    SPARK = "spark"
    ARROW = "arrow"
    RAW = "raw"

    def __str__(self) -> str:
        return self.value


class MultipassClientType(str, Enum, metaclass=EnumContainsMeta):
    """Multipass client types."""

    CONFIDENTIAL = "CONFIDENTIAL"
    PUBLIC = "PUBLIC"


class MultipassGrantType(str, Enum, metaclass=EnumContainsMeta):
    """Multipass grant types."""

    AUTHORIZATION_CODE = "AUTHORIZATION_CODE"
    CLIENT_CREDENTIALS = "CLIENT_CREDENTIALS"
    REFRESH_TOKEN = "REFRESH_TOKEN"  # noqa: S105


ResourceDecoration = Literal[
    "description",
    "favorite",
    "branches",
    "defaultBranch",
    "defaultBranchWithMarkings",
    "branchesCount",
    "hasBranches",
    "hasMultipleBranches",
    "backedObjectTypes",
    "path",
    "longDescription",
    "inTrash",
    "collections",
    "namedCollections",
    "tags",
    "namedTags",
    "alias",
    "collaborators",
    "namedAncestors",
    "markings",
    "projectAccessMarkings",
    "linkedItems",
    "contactInformation",
    "classification",
    "disableInheritedPermissions",
    "propagatePermissions",
    "resourceLevelRoleGrantsAllowed",
]

ResourceDecorationSet = set[ResourceDecoration]
ResourceDecorationSetAll = Union[ResourceDecorationSet, Literal["all"]]
ALL_RESOURCE_DECORATIONS: ResourceDecorationSet = {
    "description",
    "favorite",
    "branches",
    "defaultBranch",
    "defaultBranchWithMarkings",
    "branchesCount",
    "hasBranches",
    "hasMultipleBranches",
    "backedObjectTypes",
    "path",
    "longDescription",
    "inTrash",
    "collections",
    "namedCollections",
    "tags",
    "namedTags",
    "alias",
    "collaborators",
    "namedAncestors",
    "markings",
    "projectAccessMarkings",
    "linkedItems",
    "contactInformation",
    "classification",
    "disableInheritedPermissions",
    "propagatePermissions",
    "resourceLevelRoleGrantsAllowed",
}


class Attribution(TypedDict):
    """User and timestamp for resource creation."""

    time: str
    user_id: str


class Branch(TypedDict):
    """Resource branches."""

    name: str
    rid: Rid
    urlVariables: dict[str, Any]
    classificationRids: set[Rid] | None


class ClassificationBanner(TypedDict):
    """Classification information for banners."""

    classificationString: str


CategoryType = Literal["DISJUNCTIVE", "CONJUNCTIVE"]


class MarkingInfo(TypedDict):
    """Marking representation."""

    markingId: str
    markingName: str
    categoryId: str
    categoryname: str
    categoryType: CategoryType
    organizationRid: Rid | None
    isOrganization: bool
    isDirectlyApplied: bool | None
    disjunctiveInheritedConjunctively: bool | None
    isCbac: bool


class BranchWithMarkings(TypedDict):
    """Resource branch with markings."""

    branch: Branch
    markings: set[MarkingInfo]
    satisfiesConstraints: bool
    classificationBanner: ClassificationBanner | None


BackedObjectTypeId = str


class BackedObjectTypeInfo(TypedDict):
    """Object type backed by dataset."""

    id: BackedObjectTypeId


class Deprecation(TypedDict):
    """Deprecation info for resource."""

    message: str | None
    alternativeResourceRid: Rid | None


class NamedResourceIdentifier(TypedDict):
    """Represantion for name/rid."""

    rid: Rid
    name: str


class LinkedItem(TypedDict):  # noqa: D101
    type: str
    rid: Rid
    attribution: Attribution


PrincipalId = str


class Contact(TypedDict):
    """Contact for a resource."""

    groupId: str | None
    principalId: PrincipalId


class ContactInformation(TypedDict):
    """Information about resource contact."""

    primaryContact: Contact


class Classification(TypedDict):
    """Classification for resource."""

    userConstraintClassificationBanner: ClassificationBanner | None
    resourceClassificationBanner: ClassificationBanner | None


DisableInheritedPermissionsType = Literal["NONE", "ALL", "ALL_WITHOUT_MANDATORY"]


TransactionType = Literal["UPDATE", "APPEND", "DELETE", "SNAPSHOT", "UNDEFINED"]
TransactionStatus = Literal["OPEN", "COMMITTED", "ABORTED"]
FilePathType = Literal["NO_FILES", "MANAGED_FILES", "REGISTERED_FILES"]


class TransactionMetadata(TypedDict):
    """Foundry transaction metadata API object."""

    fileCount: int
    totalFileSize: int  # SafeLong
    hiddenFileCount: int
    totalHiddenFileSize: int  # SafeLong


MarkingId = str


class TransactionRange(TypedDict):
    """Foundry transaction range API object."""

    startTransactionRid: Rid
    endTransactionRid: Rid


class ProvenanceRecord(TypedDict):
    """Foundry provenance record API object."""

    datasetRid: Rid
    transactionRange: TransactionRange | None
    schemaBranchId: str | None
    schemaVersionId: str | None
    nonCatalogResources: list[Rid]
    assumedMarkings: set[MarkingId]


DependencyType = Literal["SECURED", "UNSECURED"]


class NonCatalogProvenanceRecord(TypedDict):
    """Foundry NonCatalogProvenanceRecord API object."""

    resources: set[Rid]
    assumedMarkings: set[MarkingId]
    dependencyType: DependencyType | None


class TransactionProvenance(TypedDict):
    """Foundry TransactionProvenance API object."""

    provenanceRecords: set[ProvenanceRecord]
    nonCatalogProvenanceRecords: set[NonCatalogProvenanceRecord]


class Transaction(TypedDict):
    """Foundry transaction API object."""

    type: TransactionType
    status: TransactionStatus
    filePathType: FilePathType
    startTime: str
    closeTime: str | None
    permissionPath: str | None
    record: dict[str, Any] | None
    attribution: Attribution | None
    metadata: TransactionMetadata | None
    isDataDeleted: bool
    isDeletionComplete: bool
    rid: Rid
    provenance: TransactionProvenance | None
    datasetRid: Rid


class SecuredTransaction(TypedDict):
    """Foundry SecuredTransaction API object."""

    transaction: Transaction
    rid: TransactionRid


class DatasetIdentity(TypedDict):
    """Foundry DatasetIdentity API object."""

    dataset_path: FoundryPath
    dataset_rid: DatasetRid
    last_transaction_rid: DatasetRid | None
    last_transaction: SecuredTransaction | None
