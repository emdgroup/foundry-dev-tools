"""Resource helper class."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from foundry_dev_tools.clients.compass import get_decoration
from foundry_dev_tools.errors.compass import WrongResourceTypeError

if TYPE_CHECKING:
    import sys

    from foundry_dev_tools.config.context import FoundryContext
    from foundry_dev_tools.utils import api_types

    if sys.version_info < (3, 11):
        from typing_extensions import Self
    else:
        from typing import Self


class Resource:
    """Helper class for Foundry Resources."""

    _context: FoundryContext
    # always included (at least what I can see)
    rid: api_types.Rid
    name: str
    path: api_types.FoundryPath  # not optional for this class, normally only included with extra decorations
    created: api_types.Attribution | None
    modified: api_types.Attribution | None
    last_modified: float | None
    directly_trashed: bool
    is_autosave: bool
    is_hidden: bool
    operations: set[str] | None
    url_variables: dict[str, str]

    # included with extra decoration
    description: str | None = None
    favorite: bool | None = None
    branches: set[api_types.CompassBranch] | None = None
    default_branch: api_types.CompassBranch | None = None
    default_branch_with_markings: api_types.BranchWithMarkings | None = None
    branches_count: int | None = None
    has_branches: bool | None = None
    has_multiple_branches: bool | None = None
    backed_object_types: set[api_types.BackedObjectTypeInfo] | None = None
    long_description: str | None = None
    in_trash: bool | None = None
    deprecation: api_types.Deprecation | None = None
    collections: set[str] | None = None  # Assuming RID is a string
    named_collections: set[api_types.NamedResourceIdentifier] | None = None
    tags: set[str] | None = None  # Assuming RID is a string
    named_tags: set[api_types.NamedResourceIdentifier] | None = None
    alias: str | None = None
    collaborators: set[api_types.Attribution] | None = None
    named_ancestors: list[api_types.NamedResourceIdentifier] | None = None
    markings: set[api_types.MarkingInfo] | None = None
    project_access_markings: set[api_types.MarkingInfo] | None = None
    linked_items: set[api_types.LinkedItem] | None = None
    contact_information: api_types.ContactInformation | None = None
    classification: api_types.Classification | None = None
    disable_inherited_permissions: api_types.DisableInheritedPermissionsType | None = None
    propagate_permissions: bool | None = None
    resource_level_role_grants_allowed: bool | None = None
    __created__: bool | None = None

    _default_decorations: ClassVar[api_types.ResourceDecorationSet] = {"inTrash", "path"}
    _decoration: api_types.ResourceDecorationSet
    rid_start: ClassVar[str] = ""

    def __init__(self, *args, **kwargs):
        """Not intended to be initialized directly. Use :py:meth:`Resource.from_rid` or :py:meth:`Resource.from_path` instead."""  # noqa: E501
        self._from_json(*args, **kwargs)

    def _from_json(
        self,
        rid: api_types.Rid,
        name: str,
        path: api_types.FoundryPath,
        directlyTrashed: bool,  # noqa: N803
        isAutosave: bool,  # noqa: N803
        isHidden: bool,  # noqa: N803
        created: api_types.Attribution | None,
        modified: api_types.Attribution | None,
        lastModified: float | None,  # noqa: N803
        operations: set[str] | None,
        urlVariables: dict[str, str],  # noqa: N803
        description: str | None = None,
        favorite: bool | None = None,
        branches: set[api_types.CompassBranch] | None = None,
        defaultBranch: api_types.CompassBranch | None = None,  # noqa: N803
        defaultBranchWithMarkings: api_types.BranchWithMarkings | None = None,  # noqa: N803
        branchesCount: int | None = None,  # noqa: N803
        hasBranches: bool | None = None,  # noqa: N803
        hasMultipleBranches: bool | None = None,  # noqa: N803
        backedObjectTypes: set[api_types.BackedObjectTypeInfo] | None = None,  # noqa: N803
        longDescription: str | None = None,  # noqa: N803
        inTrash: bool | None = None,  # noqa: N803
        deprecation: api_types.Deprecation | None = None,
        collections: set[api_types.Rid] | None = None,
        namedCollections: set[api_types.NamedResourceIdentifier] | None = None,  # noqa: N803
        tags: set[api_types.Rid] | None = None,
        namedTags: set[api_types.NamedResourceIdentifier] | None = None,  # noqa: N803
        alias: str | None = None,
        collaborators: set[api_types.Attribution] | None = None,
        namedAncestors: list[api_types.NamedResourceIdentifier] | None = None,  # noqa: N803
        markings: set[api_types.MarkingInfo] | None = None,
        projectAccessMarkings: set[api_types.MarkingInfo] | None = None,  # noqa: N803
        linkedItems: set[api_types.LinkedItem] | None = None,  # noqa: N803
        contactInformation: api_types.ContactInformation | None = None,  # noqa: N803
        classification: api_types.Classification | None = None,
        disableInheritedPermissions: api_types.DisableInheritedPermissionsType | None = None,  # noqa: N803
        propagatePermissions: bool | None = None,  # noqa: N803
        resourceLevelRoleGrantsAllowed: bool | None = None,  # noqa: N803
        **kwargs,
    ):
        self.rid = rid
        self.name = name
        self.path = path
        self.created = created
        self.modified = modified
        self.last_modified = lastModified
        self.directly_trashed = directlyTrashed
        self.is_autosave = isAutosave
        self.is_hidden = isHidden
        self.operations = operations
        self.url_variables = urlVariables
        self.description = description
        self.favorite = favorite
        self.branches = branches
        self.default_branch = defaultBranch
        self.default_branch_with_markings = defaultBranchWithMarkings
        self.branches_count = branchesCount
        self.has_branches = hasBranches
        self.has_multiple_branches = hasMultipleBranches
        self.backed_object_types = backedObjectTypes
        self.long_description = longDescription
        self.in_trash = inTrash
        self.deprecation = deprecation
        self.collections = collections
        self.named_collections = namedCollections
        self.tags = tags
        self.named_tags = namedTags
        self.alias = alias
        self.collaborators = collaborators
        self.named_ancestors = namedAncestors
        self.markings = markings
        self.project_access_markings = projectAccessMarkings
        self.linked_items = linkedItems
        self.contact_information = contactInformation
        self.classification = classification
        self.disable_inherited_permissions = disableInheritedPermissions
        self.propagate_permissions = propagatePermissions
        self.resource_level_role_grants_allowed = resourceLevelRoleGrantsAllowed
        self._kwargs = kwargs

    @classmethod
    def _create_class(
        cls,
        ctx: FoundryContext,
        resource_json: dict,
        decoration: api_types.ResourceDecorationSet,
    ) -> Self:
        if cls is Resource:
            for _rid_start, rid_cls in RID_CLASS_REGISTRY.items():
                if resource_json["rid"].startswith(_rid_start):
                    cls = rid_cls
                    break
        elif not resource_json["rid"].startswith(cls.rid_start):
            raise WrongResourceTypeError(resource_json["rid"], resource_json["path"], cls)

        instance = cls.__new__(cls)
        instance.__created__ = False
        instance._context = ctx  # noqa: SLF001
        instance._decoration = decoration  # noqa: SLF001
        cls.__init__(instance, **resource_json)
        return instance

    @classmethod
    def from_path(
        cls,
        context: FoundryContext,
        path: api_types.FoundryPath,
        /,
        *,
        decoration: api_types.ResourceDecorationSetAll | None = None,
        **_kwargs,
    ) -> Self:
        """Returns resource at path.

        Args:
            context: the foundry context for the resource
            path: the path where the resource is located on foundry
            decoration: extra decoration for this resource
            **_kwargs: not used in base class
        """
        _decoration = get_decoration(decoration, False)
        _decoration = cls._default_decorations if _decoration is None else cls._default_decorations.union(_decoration)
        resource_json = context.compass.api_get_resource_by_path(
            path,
            decoration=_decoration,
        ).json()
        return cls._create_class(context, resource_json=resource_json, decoration=_decoration)

    @classmethod
    def from_rid(
        cls,
        context: FoundryContext,
        rid: api_types.Rid,
        /,
        *,
        decoration: api_types.ResourceDecorationSetAll | None = None,
        **_kwargs,
    ) -> Self:
        """Returns resource from rid.

        Args:
            context: the foundry context for the resource
            rid: the rid of the resource on foundry
            decoration: extra decoration for this resource
            **_kwargs: not used in base class
        """
        _decoration = get_decoration(decoration, False)
        _decoration = cls._default_decorations if _decoration is None else cls._default_decorations.union(_decoration)
        resource_json = context.compass.api_get_resource(
            rid,
            decoration=_decoration,
        ).json()
        return cls._create_class(context, resource_json=resource_json, decoration=_decoration)

    def add_to_trash(self) -> Self:
        """Adds resource to trash."""
        self._context.compass.api_add_to_trash({self.rid})
        return self.sync()

    def restore(self) -> Self:
        """Restores resource from trash."""
        self._context.compass.api_restore({self.rid})
        return self.sync()

    def delete_permanently(self) -> Self:
        """Deletes resource permanently."""
        self._context.compass.api_delete_permanently({self.rid})
        return self

    def sync(self) -> Self:
        """Fetches the attributes again."""
        resource_json = self._context.compass.api_get_resource(self.rid, decoration=self._decoration).json()
        self._from_json(**resource_json)
        return self

    def _get_repr_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if v is not None and not k.startswith("_")}
        if c := d.get("created"):
            d["created"] = c.get("time")
        if c := d.get("modified"):
            d["modified"] = c.get("time")
        del d["operations"]
        del d["url_variables"]
        del d["last_modified"]  # already as a formatted date in "modified"
        del d["name"]  # already visible in the "path"
        return d

    def __repr__(self):
        return f"<{self.__class__.__name__}({self._get_repr_dict()!s})>"


RID_CLASS_REGISTRY: dict[str, type[Resource]] = {}
