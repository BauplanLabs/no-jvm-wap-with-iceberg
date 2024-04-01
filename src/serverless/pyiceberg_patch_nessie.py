"""

Monkeypatch for PyIceberg to support Nessie as a catalog backing Iceberg tables. As Nessie
is not officially supported, this patch (to the best of our knowledge) is the first
open example of how to use Nessie with PyIceberg.

"""


import re
from typing import (
    Any,
    List,
    Optional,
    Set,
    Union,
)

import pynessie
import pynessie.model
from pyiceberg.catalog import (
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableRequest, CommitTableResponse, Table, update_table_metadata
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT

PYNESSIE_TABLE_TYPES = [pynessie.model.ICEBERG_TABLE_TYPE_NAME]
PYNESSIE_NAMESPACE_TYPES = [pynessie.model.NAMESPACE_TYPE_NAME]


class NessieIdentifier:
    
    _ref: pynessie.model.Reference
    content_key: pynessie.model.ContentKey
    identifier: Identifier

    def __init__(
        self,
        identifier: Union[str, Identifier],
        nessie_client: Optional[pynessie.NessieClient] = None,
    ) -> None:
        _identifier = Catalog.identifier_to_tuple(identifier)
        if not len(_identifier):
            self._ref = pynessie.model.Reference('main')
            self.content_key = pynessie.model.ContentKey([])
            self.identifier = _identifier
        else:
            ref_str = _identifier[0].split('@', 1)
            branch_name = ref_str[0]
            branch_hash = ref_str[1] if len(ref_str) > 1 else None
            self._ref = pynessie.model.Reference(branch_name, branch_hash)
            self.content_key = pynessie.model.ContentKey(list(_identifier[1:]))
            self.identifier = _identifier

        if nessie_client is not None:
            self.resolve(nessie_client)

    def resolve(self, nessie_client: pynessie.NessieClient) -> None:
        self._ref = nessie_client.get_reference(self.branch_name)
        self.identifier = (self.branch_name_with_hash, *self.identifier[1:])

    @property
    def ref(self) -> pynessie.model.Reference:
        return self._ref

    @property
    def branch_name(self) -> str:
        return self._ref.name

    @property
    def branch_hash(self) -> Optional[str]:
        return self._ref.hash_

    @property
    def branch_name_with_hash(self) -> str:
        return f'{self.branch_name}@{self.branch_hash}' if self.branch_hash else self.branch_name


class NessieCatalog(Catalog):
    
    
    def __init__(self, name: str, **properties: Any) -> None:
        super().__init__(name, **properties)

        self.nessie = pynessie.init(
            config_dict={
                'endpoint': f'{properties["endpoint"]}/api/v1',
                'verify': True,
                'default_branch': properties.get('default_branch', 'main'),
            },
        )

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Union[Schema, 'pa.Schema'],
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        nessie_identifier = NessieIdentifier(identifier)

        iceberg_schema = self._convert_schema_if_needed(schema)
        location = self._resolve_table_location(location, 'database_name', 'table_name')

        metadata_location = self._get_metadata_location(location=location)
        metadata = new_table_metadata(
            location=location,
            schema=iceberg_schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )
        io = load_file_io({**self.properties, **properties}, location=location)
        self._write_metadata(metadata, io, metadata_location)

        new_iceberg_table = pynessie.model.IcebergTable(
            id=None,
            metadata_location=metadata_location,
            snapshot_id=metadata.current_snapshot_id,
            schema_id=metadata.current_schema_id,
            spec_id=metadata.default_spec_id,
            sort_order_id=metadata.default_sort_order_id,
        )

        # Get the HEAD reference
        nessie_identifier.resolve(self.nessie)
        assert nessie_identifier.branch_hash is not None, 'No hash'

        self.nessie.commit(
            nessie_identifier.branch_name,
            nessie_identifier.branch_hash,
            f'CREATE TABLE {nessie_identifier.content_key.to_string()}',
            'jacopo.tagliabue@bauplanlabs.com',
            pynessie.model.Put(nessie_identifier.content_key, new_iceberg_table),
        )

        return self.load_table(identifier)

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        raise NotImplementedError('Not yet implemented')

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        nessie_identifier = NessieIdentifier(
            tuple([*table_request.identifier.namespace.root, table_request.identifier.name]),
            self.nessie,
        )
        assert nessie_identifier.branch_hash is not None, 'No hash'

        current_table = self.load_table(nessie_identifier.identifier)

        old_table = self.nessie.get_content(
            nessie_identifier.branch_name_with_hash, nessie_identifier.content_key
        )
        assert isinstance(old_table, pynessie.model.IcebergTable), 'Not a table'

        base_metadata = current_table.metadata
        for requirement in table_request.requirements:
            requirement.validate(base_metadata)

        updated_metadata = update_table_metadata(base_metadata, table_request.updates)
        if updated_metadata == base_metadata:
            return CommitTableResponse(
                metadata=base_metadata,
                metadata_location=current_table.metadata_location,
            )

        # write new metadata
        new_metadata_version = self._parse_metadata_version(current_table.metadata_location) + 1
        new_metadata_location = self._get_metadata_location(
            current_table.metadata.location, new_metadata_version
        )
        self._write_metadata(updated_metadata, current_table.io, new_metadata_location)

        new_iceberg_table = pynessie.model.IcebergTable(
            id=old_table.id,
            metadata_location=new_metadata_location,
            snapshot_id=updated_metadata.current_snapshot_id,
            schema_id=updated_metadata.current_schema_id,
            spec_id=updated_metadata.default_spec_id,
            sort_order_id=updated_metadata.default_sort_order_id,
        )

        self.nessie.commit(
            nessie_identifier.branch_name,
            nessie_identifier.branch_hash,
            f'APPEND TABLE {nessie_identifier.content_key.to_string()}',
            'jacopo.tagliabue@bauplanlabs.com',
            pynessie.model.Put(nessie_identifier.content_key, new_iceberg_table),
        )

        return CommitTableResponse(
            metadata=updated_metadata,
            metadata_location=new_metadata_location,
        )

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        nessie_identifier = NessieIdentifier(identifier, self.nessie)

        content = self.nessie.get_content(
            ref=nessie_identifier.branch_name_with_hash,
            content_key=nessie_identifier.content_key,
        )
        assert isinstance(content, pynessie.model.IcebergTable), 'Not an Iceberg table'

        metadata_location = content.metadata_location

        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)

        return Table(
            identifier=nessie_identifier.identifier,
            metadata_location=metadata_location,
            metadata=metadata,
            io=self._load_file_io(metadata.properties, metadata_location),
            catalog=self,
        )

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError('Not yet implemented')

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError('Not yet implemented')

    def rename_table(
        self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]
    ) -> Table:
        raise NotImplementedError('Not yet implemented')

    def create_namespace(
        self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT
    ) -> None:
        raise NotImplementedError('Not yet implemented')

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        raise NotImplementedError('Not yet implemented')

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        return self._list_keys(namespace, PYNESSIE_TABLE_TYPES)

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        raise NotImplementedError('Not yet implemented')

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        raise NotImplementedError('Not yet implemented')

    def update_namespace_properties(
        self,
        namespace: Union[str, Identifier],
        removals: Optional[Set[str]] = None,
        updates: Properties = EMPTY_DICT,
    ) -> PropertiesUpdateSummary:
        raise NotImplementedError('Not yet implemented')

    #
    # Custom methods

    def create_branch(self, name: str, from_identifier: Union[str, Identifier]) -> None:
        nessie_identifier = NessieIdentifier(from_identifier, self.nessie)
        assert isinstance(nessie_identifier.ref, pynessie.model.Branch), 'This reference is not a branch'
        assert nessie_identifier.branch_hash is not None, 'No hash'

        self.nessie.create_branch(
            branch=name,
            ref=nessie_identifier.branch_name,
            hash_on_ref=nessie_identifier.branch_hash,
        )

    def drop_branch(self, name: str) -> None:
        nessie_identifier = NessieIdentifier(name, self.nessie)
        assert nessie_identifier.branch_hash is not None, 'No hash'

        self.nessie.delete_branch(nessie_identifier.branch_name, nessie_identifier.branch_hash)

    def merge(self, from_name: str, to_name: str) -> None:
        self.nessie.merge(
            from_ref=from_name,
            onto_branch=to_name,
        )

    #
    # Helpers

    def _list_keys(self, namespace: Union[str, Identifier], kinds: List[str]) -> List[Identifier]:
        nessie_identifier = NessieIdentifier(namespace, self.nessie)
        assert nessie_identifier.branch_hash is not None, 'No hash'

        query_filters: list[str] = []
        if len(nessie_identifier.content_key.elements) > 0:
            nessie_regex = re.escape(nessie_identifier.content_key.to_string()).replace('\\', '\\\\')
            query_filters.append(f"entry.namespace.matches('^{nessie_regex}(\\\\.|$)')")
        if len(kinds) > 0:
            query_filters.append(f'entry.contentType in {kinds}')

        identifiers: List[Identifier] = []
        next_token: Optional[str] = ''
        while next_token is not None:
            res = self.nessie.list_keys(
                ref=nessie_identifier.branch_name_with_hash,
                page_token=next_token,
                query_filter=' && '.join(query_filters),
            )
            next_token = res.token if res.has_more else None
            for entry in res.entries:
                identifiers.append(tuple([nessie_identifier.branch_name_with_hash, *entry.name.elements]))
        return identifiers

    def _drop_content(
        self, nessie_identifier: NessieIdentifier
    ) -> tuple[pynessie.model.IcebergTable, pynessie.model.Branch]:
        raise NotImplementedError('Not yet implemented')