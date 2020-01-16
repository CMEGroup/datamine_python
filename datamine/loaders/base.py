import pandas as pd
import os
import glob
import sys

from importlib import import_module
from importlib import reload
from ..utils import tqdm_execute_tasks, logger

__all__ = ['Loader']


class Loader(object):
    columns = None
    dtypes = None
    dataset = None
    dataset_args = None
    fileglob = '*.csv'
    index = None

    _by_name = None

    @classmethod
    def _load_datasets(cls):
        cls._by_name = {}
        pkg = __name__.rsplit('.', 1)[0]
        fpath, base = os.path.split(__file__)
        for fname in glob.glob(os.path.join(fpath, '*.py')):
            fname = os.path.basename(fname)
            if fname in (base, '__init__.py'):
                continue
            module = import_module('.' + fname[:-3], pkg)
            for key, value in module.__dict__.items():
                if isinstance(value, cls):
                    if not isinstance(value.dataset, str):
                        raise RuntimeError('Invalid Loader: dataset must be a string, not {}'.format(type(value.dataset)))
                    elif value.dataset in cls._by_name:
                        raise RuntimeError('Invalid Loader: duplicate loader for {} dataset'.format(value.dataset))
                    else:
                        cls._by_name[value.dataset] = value
                        # {'BLOCK' : <datamine.loaders.block.BlockLoader object at 0x0000026E3AE01DD8>}

    @classmethod
    def datasets(cls):
        if cls._by_name is None:
            cls._load_datasets()
        return list(cls._by_name.keys())
    
    @classmethod
    def by_name(cls, dataset, dataset_args = {}):
        cls.dataset_args = dataset_args
        if cls._by_name is None:
            cls._load_datasets()
        if dataset not in cls._by_name:
            raise RuntimeError('Dataset not found: {}'.format(dataset))
        return cls._by_name[dataset]
    
    def _set_dtypes(self, df):
        if self.dtypes is None:
            return
        
        column_check = []
        for k, v in self.dtypes.items():
            for value in v:
                column_check.append(value)
        if self.columns is not None:
            if set(self.columns).difference(column_check):
                print("Mismatched column names & dtypes. Mismatches:", set(self.columns).difference(column_check))
                logger.error(("Mismatched column names & dtypes. Mismatches:", set(self.columns).difference(column_check)))
        for dtype, cols in self.dtypes.items():
            for col in ((cols,) if isinstance(cols, str) else cols):
                if col in df:
                    if dtype.startswith('date'):
                        format = None if dtype == 'date' else dtype[5:]
                        df[col] = pd.to_datetime(df[col], format=format, utc=True, errors='ignore')
                    else:
                        df[col] = df[col].astype(dtype, errors='ignore')

    def _glob(self, path):
        return glob.glob(os.path.join(path, self.fileglob))

    def _load(self, filename):
        '''Return a raw, unprocessed dataframe.'''
        return pd.read_csv(filename, low_memory=False)

    def _load_single(self, filename):
        '''Use _load to read a dataframe from disk, then assign new column
           names and coerce the datatypes, as appropriate.'''
        df = self._load(filename)
        if self.columns is not None:
            df.columns = self.columns
        self._set_dtypes(df)
        if self.index is not None:
            df = df.set_index(self.index)
        return df

    def _finalize(self, df):
        return df

    def load(self, filenames, limit=None, max_workers=None):
        '''Load a composite dataframe by concatenating individual files.'''
        if isinstance(filenames, str):
            if os.path.isdir(filenames):
                filenames = self._glob(filenames)
            elif '*' in filenames:
                filenames = glob.glob(filenames)
            else:
                filenames = [filenames]
        nframes = len(filenames)
        if limit and nframes > limit:
            logger.info('limiting to {}/{} files'.format(limit, nframes))
            filenames = filenames[-limit:]
            nframes = limit
        if nframes == 0:
            result = pd.DataFrame(columns=self.columns)
            self._set_dtypes(result)
        elif nframes == 1:
            result = self._load_single(filenames[0])
        else:
            result = tqdm_execute_tasks(self._load_single, filenames,
                                        'reading {} data'.format(self.dataset), max_workers)
            logger.info('concatenating {} dataframes'.format(nframes))
            result = pd.concat(result, ignore_index=self.index is None)
            # Set the categorical columns again, because concatenation often
            # results in a reversion to object dtype
            cols = self.dtypes.get('category', ())
            for col in ((cols,) if isinstance(cols, str) else cols):
                if col in result:
                    result[col] = result[col].astype('category', errors='ignore')
        return self._finalize(result)
