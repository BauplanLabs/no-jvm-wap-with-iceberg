from time import time
import nessie_monkey_patch
from pyiceberg.catalog import load_catalog
import pyarrow as pa


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


@measure_func
def load_nessie_catalog(nessie_base_url, catalog, verbose=True):
    prop = {'type': "nessie", "verbose": verbose, 'url': nessie_base_url}
    return load_catalog(catalog, **prop)
    
@measure_func
def load_nessie_table(catalog, table_name, branch_name='main'):
    return catalog.load_table(table_name, branch_name)

@measure_func
def nessie_table_scan_to_arrow_table(table, filter, projections, with_files=False, limit=None):
    scan = None
    scan = table.scan(
        row_filter=filter,
        selected_fields=projections
    )
    _files = []
    if with_files:
        _files = [task.file.file_path for task in scan.plan_files()]
        print("Found number of files: ", len(_files))
        # remove s3a:// prefix to make sure arrow dataset could work
        _files = [f.replace('s3a://', '').replace('s3://', '') for f in _files]
        
    return scan.to_arrow(), _files
