"""Defines types for the Foundry API, for better readability of the code."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypedDict, get_args

if TYPE_CHECKING:
    from datetime import datetime


def assert_in_literal(option, literal, variable_name) -> None:  # noqa: ANN001
    """Raise a TypeError when the passed option is not contained in the literal.

    Args:
        option: An option from the list of options from `literal`
        literal: The literal variable defining all the valid options
        variable_name: The name of the literal variable
    """
    options = get_args(literal)

    if option not in options:
        msg = f"'{option}' is not a valid option for {variable_name}, valid options are {options}"
        raise TypeError(msg)


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

View = DatasetBranch | TransactionRid
"""A view, which is a :py:attr:`~foundry_dev_tools.utils.api_types.Ref` or a :py:attr:`~foundry_dev_tools.utils.api_types.TransactionRid`."""  # noqa: E501

RoleId = str
"""A role in foundry (Owner, Editor, Viewer, Discoverer,...)"""

FolderRid = str
"""A compass folder resource identifier."""

JobSpecRid = str
"""A jobspec resource identifier."""

OrganizationRid = str
"""An organization resource identifier."""

ProjectRid = str
"""A compass project root folder resource identifier."""

SourceRid = str
"""A magritte source resource identifier."""

TableRid = str
"""A virtual table resource identifier."""

NetworkEgressPolicyRid = str
"""A virtual table resource identifier."""

# TODO further typing?
FoundrySchema = dict[str, Any]

FoundryTransaction = Literal["SNAPSHOT", "UPDATE", "APPEND", "DELETE"]
"""Foundry transaction types."""

GroupId = str
"""A group identifier on Foundry"""

SqlDialect = Literal["ANSI", "SPARK"]
"""The SQL Dialect for Foundry SQL queries."""

SQLReturnType = Literal["pandas", "spark", "arrow", "raw"]
"""The return_types for sql queries.

pandas: :external+pandas:py:class:`pandas.DataFrame`
arrow: :external+pyarrow:py:class:`pyarrow.Table`
spark: :external+spark:py:class:`~pyspark.sql.DataFrame`
raw: Tuple of (foundry_schema, data) (can only be used in legacy)
"""

MultipassClientType = Literal["CONFIDENTIAL", "PUBLIC"]
"""Multipass client types."""

MultipassGrantType = Literal["AUTHORIZATION_CODE", "CLIENT_CREDENTIALS", "REFRESH_TOKEN"]
"""Multipass grant types."""

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
ResourceDecorationSetAll = ResourceDecorationSet | Literal["all"]
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

TokenId = str
"""An identifier for a token issued by foundry."""

TokenType = Literal["USER_GENERATED"]
"""The type of a token."""

UserId = str
"""An identifier for an user on foundry."""

CodeResourceType = Literal["stemmaRepository", "computeModule", "eddiePipeline"]
"""Code Resource type."""

PrincipalTypes = Literal["USER", "GROUP"]
"""A type for a principle on foundry."""


class Attribution(TypedDict):
    """User and timestamp for resource creation."""

    time: str
    user_id: str


class CompassBranch(TypedDict):
    """Resource branches."""

    name: str
    rid: Rid
    urlVariables: dict[str, Any]
    classificationRids: set[Rid] | None


class Branch(TypedDict):
    """Dataset branches."""

    id: str
    rid: Rid
    ancestorBranchIds: list[str]
    creationTime: str
    transactionRid: TransactionRid | None


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


PatchOperation = Literal["ADD", "REMOVE"]
"""Foundry PatchOperation enum."""

FieldType = Literal["NAME", "LAST_MODIFIED"]
"""Foundry FieldType enum."""

SortDirection = Literal["ASC", "DESC"]
"""Foundry SortDirection enum."""


class SortSpec(TypedDict):
    """Foundry SortSpec API object."""

    field: FieldType
    direction: SortDirection


PrincipalType = Literal["EVERYONE", "GROUP", "USER"]
"""Foundry PrincipalType enum."""


class Principal(TypedDict):
    """Foundry Principal API object."""

    id: PrincipalId
    type: PrincipalType


class RoleGrant(TypedDict):
    """Foundry RoleGrant API object."""

    role: RoleId
    principal: Principal


class RoleGrantPatch(TypedDict):
    """Foundry RoleGrantPatch API object."""

    roleGrant: RoleGrant
    patchOperation: PatchOperation


UserGroupPrincipalType = Literal["GROUP", "USER"]
"""Foundry UserGroupPrincipalType enum."""


class UserGroupPrincipal(TypedDict):
    """Foundry UserGroupPrincipal API object."""

    id: str
    type: UserGroupPrincipalType


class UserGroupPrincipalPatch(TypedDict):
    """Foundry UserGroupPrincipalPatch API object."""

    principal: UserGroupPrincipal
    patchOperation: PatchOperation


class ResourceGrantsResult(TypedDict):
    """Foundry ResourceGrantsResult API object."""

    grants: set[RoleGrant]
    disableInheritedPermissionsForPrincipals: set[UserGroupPrincipal]
    disableInheritedPermissions: bool
    disableInheritedPermissionsType: DisableInheritedPermissionsType


RoleContext = Literal["MARKETPLACE_INSTALLATION", "ONTOLOGY", "PROJECT", "TABLES", "TELEMETRY", "USE_CASE"]
"""Foundry RoleContext."""

RoleSetId = str
"""Foundry RoleSet identifier."""


class RoleSetUpdate(TypedDict):
    """Foundry RoleSetUpdate API object."""

    currentRoleSet: RoleSetId
    targetRoleSet: RoleSetId
    rolesMap: dict[RoleId, set[RoleId]]


class MarkingPatch(TypedDict):
    """Foundry MarkingPatch API object."""

    markingId: MarkingId
    patchOperation: PatchOperation


class CbacMarkingConstraint(TypedDict):
    """Foundry CbacMarkingConstraint API object."""

    markingIds: set[MarkingId]


MandatoryMarkingConstraintPatchType = Literal["ALLOWED", "DENIED", "NONE"]
"""Foundry MandatoryMarkingConstraintPatchType."""


class MandatoryMarkingConstraintPatches(TypedDict):
    """Foundry MandatoryMarkingConstraintPatches API object."""

    markingPatches: list[MarkingPatch]
    type: MandatoryMarkingConstraintPatchType


class ProjectFolderDisplaySettingsUpdate(TypedDict):
    """Foundry ProjectFolderDisplaySettingsUpdate API object."""

    showLinkedOntologyEntitiesTab: bool | None
    showReleaseAndPublishTab: bool | None


MavenProductId = str
"""Foundry MavenProduct identifier."""

MoveResourcesOption = Literal["ALLOW_MOVING_TO_HIDDEN", "REMOVE_ROLE_GRANTS", "DECONFLICT_NAME"]
"""Foundry MoveResourcesOption to be used when moving resources"""

ImportType = Literal["EXTERNAL", "FILE_SYSTEM"]
"""Foundry import types as indicator for Compass-tracked (file-system) respectively non-Compass-tracked (external) resources."""  # noqa: E501


class User(TypedDict):
    """Foundry multipass user."""

    id: UserId
    username: str
    attributes: dict[str, list[str]]


class Group(TypedDict):
    """Foundry multipass group."""

    id: GroupId
    name: str
    attributes: dict[str, list[str]]


class GroupMemberExpirationSettings(TypedDict):
    """Foundry GroupMemberExpirationSettings API object."""

    maxExpiration: datetime | None
    """The time in the future on which all memberships will be automatically expired."""

    maxDurationInSeconds: int | None
    """The time after which membership expires when adding a principal to a group
    or updating their membership expiration time."""


class DatasetsJobSpecSelection(TypedDict):
    """Foundry DatasetsJobSpecSelection API object."""

    datasetRids: list[DatasetRid]
    isRequired: bool


class JobSpecJobSpecsSelection(TypedDict):
    """Foundry JobSpecJobSpecsSelection API object."""

    jobSpecRids: list[JobSpecRid]
    isRequired: bool


class UpstreamJobSpecSelection(TypedDict):
    """Foundry UpstreamJobSpecSelection API object."""

    datasetRids: list[DatasetRid]
    datasetRidsToIgnore: list[DatasetRid]


class ConnectingJobSpecSelection(TypedDict):
    """Foundry ConnectingJobSpecSelection API object."""

    upstreamDatasetRids: list[DatasetRid]
    downstreamDatasetRids: list[DatasetRid]
    datasetRidsToIgnore: list[DatasetRid]


ExceededDurationMode = Literal["CANCEL", "RETRY"]
"""Foundry ExceededDurationMode."""

InputFailureStrategy = Literal["CONTINUE", "FAIL"]
"""Foundry InputFailureStrategy.

CONTINUE: Will attempt to continue running the job, requesting that the input manager resolve the dataset properties
    of the input dataset as if the failed job had never run
FAIL: will fail the job when one of its inputs fails
"""


class InputStrategy(TypedDict):
    """Foundry InputStrategy API object."""

    inputDatasetRid: DatasetRid
    failureStrategy: InputFailureStrategy


OutputQueueStrategy = Literal["QUEUE_UP", "SUPERSEDE"]
"""Foundry OutputQueueingStrategy.

QUEUE_UP: The new job is placed into the queue for the output
SUPERSEDE: The new job is placed into the queue for the output and a cancellation signal is sent to the running job
    and the other queued jobs to pop them off the queue and stop execution
"""
