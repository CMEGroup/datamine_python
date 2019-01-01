import pandas as pd
import os
import glob

from ..utils import tqdm_execute_tasks, logger

__all__ = ['Loader']


class Loader(object):
    columns = None
    dtypes = None
    dataset = None
    fileglob = '*.csv'
    index = None

    def _set_dtypes(self, df):
        if self.dtypes is None:
            return
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
            result = pd.DataFrame(columns=self.names)
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
