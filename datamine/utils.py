import logging

from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

MAX_WORKERS = 4

logger = logging.getLogger(__name__.rsplit('.', 1)[0])

# If we're in a Jupyter notebook, we need to play some tricks
# in order to get the logger output to show up in the notebook.
try:
    from IPython import get_ipython
    if 'IPKernelApp' in get_ipython().config:
        import sys
        logger.handlers = [logging.StreamHandler(sys.stderr)]
        logger.setLevel(logging.INFO)
except Exception:
    pass

def tqdm_execute_tasks(fn, keys, desc, max_workers=MAX_WORKERS, mode='process'):
    """
    Equivalent to executor.map(fn, values), but uses a tqdm-based progress bar
    """
    if max_workers == 1:
        return [fn(key) for key in tqdm(keys, desc=desc)]
    # Processes are better for the dataframe loading tasks, but
    # threads are significantly better for downloads
    Executor = ThreadPoolExecutor if mode == 'thread' else ProcessPoolExecutor
    with Executor(max_workers=max_workers) as executor:
        futures = [executor.submit(fn, key) for key in keys]
        for f in tqdm(as_completed(futures), total=len(keys), desc=desc):
            pass
        return [f.result() for f in futures]
