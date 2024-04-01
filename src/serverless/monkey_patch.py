"""

This is a utility script that monkey patches PyIceberg to support the Nessie catalog (yay Python!).
For all intents and purposes, you can ignore these details and just treat the catalog in the app.py script 
as your main abstraction.

"""


from enum import Enum
from typing import Callable
import pyiceberg.catalog as pic
from pyiceberg.exceptions import NotInstalledError
from pyiceberg.typedef import Properties


class CatalogType(Enum):
    REST = 'rest'
    HIVE = 'hive'
    GLUE = 'glue'
    DYNAMODB = 'dynamodb'
    SQL = 'sql'
    NESSIE = 'nessie'


pic.CatalogType = CatalogType


def load_nessie(name: str, conf: Properties) -> pic.Catalog:
    try:
        from pyiceberg_patch_nessie import NessieCatalog

        return NessieCatalog(name, **conf)
    except ImportError as exc:
        raise NotInstalledError("Nessie support not installed: pip install 'pynessie'") from exc


AVAILABLE_CATALOGS: dict[CatalogType, Callable[[str, Properties], pic.Catalog]] = {
    CatalogType.REST: pic.load_rest,
    CatalogType.HIVE: pic.load_hive,
    CatalogType.GLUE: pic.load_glue,
    CatalogType.DYNAMODB: pic.load_dynamodb,
    CatalogType.SQL: pic.load_sql,
    CatalogType.NESSIE: load_nessie,
}

pic.AVAILABLE_CATALOGS = AVAILABLE_CATALOGS