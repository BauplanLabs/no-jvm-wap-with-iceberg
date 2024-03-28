import os
import pyarrow as pa
from pyiceberg.catalog import load_catalog
import pyiceberg_patch
from pyiceberg_patch_nessie import NessieCatalog
from time import time


# decorator
def measure_func(func):
    """
    
    This function shows the execution time of the function object passed -
    this is convenient when debugging to keep track of where the program
    spends its time.
    
    """
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func


catalog: NessieCatalog = load_catalog(
    name='default',
    type='nessie',
    # 'endpoint': 'https://nessie.use1.private.adev.bauplanlabs.com',
    endpoint=os.environ['NESSIE_BASEURL'],
    default_branch=os.environ.get('NESSIE_DEFAULT_BRANCH', 'main'),
)  # type: ignore


def lambda_handler(event, context):
    # print a copy of the event in cloudwatch
    # for debugging purposes
    print(event)
    datalake_location = f'file://{os.getcwd()}/datalake'
    # datalake_location = 's3://big-pyiceberg-test-dev/datalake'
    print(f'\n# create_branch: {branch_name}')
    catalog.create_branch(branch_name, 'main')

    print('\n# create_table\n')
    namespace_name = ''
    rt_0 = catalog.create_table(
        identifier=(branch_name, namespace_name, table_name_0),
        schema=pa_table_0.schema,
        location=datalake_location,
    )
    print('\n# append 0')
    rt_0.append(pa_table_0)

    

    return None



