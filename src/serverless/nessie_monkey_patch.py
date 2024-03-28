"""

Since at the time of writing there is official pyIceberg support for Nessie, we need to monkey patch it.
This is a quick patch that allows us to use Nessie as a catalog for Iceberg tables, while re-using the rest
of PyIceberg functionalities for scans, projections etc..

"""

from enum import Enum
from typing import Any, Union
from urllib.parse import urljoin

import pyiceberg.catalog as catalog
import requests
from pyiceberg.catalog import Catalog, Identifier, Properties
from pyiceberg.table import StaticTable, Table
from requests import Session


def load_nessie(name: str, conf: Properties) -> catalog.Catalog:
    return NessieCatalog(name, **conf)


class CatalogType(Enum):
    REST = 'rest'
    HIVE = 'hive'
    GLUE = 'glue'
    DYNAMODB = 'dynamodb'
    NESSIE = 'nessie'
    SQL = 'sql'


catalog.CatalogType = CatalogType

catalog.AVAILABLE_CATALOGS[CatalogType.NESSIE] = load_nessie


class NessieCatalog(Catalog):
    uri: str
    session: Session
    properties: Properties

    def __init__(self, name: str, **properties: str):
        """
        Nessie Catalog
        """
        self.name = name
        self.properties = properties
        self.verbose = self.properties.get('verbose', False)
        self.base_url = self.properties.get('url', False)
        self.url = urljoin(self.base_url, 'api/v2/trees/{}/contents/{}')
        super().__init__(name, **properties)

    def _get_from_nessie_endpoint(self, identifier: str, branch: str) -> Any:
        final_nessie_url = self.url.format(branch, identifier)
        if self.verbose:
            print('Calling Nessie endpoint: ', final_nessie_url)
        r = requests.get(final_nessie_url, timeout=30)
        if r.status_code != 200:
            raise Exception('Error in calling nessie endpoint')

        return r.json()

    def load_table(self, identifier: Union[str, Identifier], branch: str = 'main') -> Table:
        table_from_nessie = self._get_from_nessie_endpoint(identifier, branch)
        if self.verbose:
            print(table_from_nessie, '\n')
        metadata_location = table_from_nessie['content']['metadataLocation']
        # TODO: this does not seem to be a general way to do it ;-)

        return StaticTable.from_metadata(metadata_location=metadata_location)

    def create_namespace(self):
        pass

    def create_table(self):
        pass

    def drop_namespace(self):
        pass

    def drop_table(self):
        pass

    def list_namespaces(self):
        pass

    def list_tables(self):
        pass

    def load_namespace_properties(self):
        pass

    def rename_table(self):
        pass

    def update_namespace_properties(self):
        pass

    def register_table(self):
        pass

    def _commit_table(self):
        pass
