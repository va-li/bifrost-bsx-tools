from pathlib import Path
import io
import zipfile as zf
import json
from typing import IO, List, Dict, Any, Union, Optional
import warnings
import functools

# non-standard library imports
import yaml
import pandas as pd
import bifrost_common_py.selectors as select
from bifrost_common_py.safepointer import safepointer

class BsxException(Exception):
    pass

class DynamicTimeseriesNotFoundError(BsxException):
    '''
    Dynamic timeseries not found in BSX archive
    
    Parameters
    ----------
    run_id : str
        The id of the run that was searched
    dynamic_id : str
        The id of the dynamic that was searched
    '''
    
    def __init__(self, run_id: str, dynamic_id: str):
        self.run_id = run_id
        self.dynamic_id = dynamic_id
        super().__init__(f"Dynamic timeseries for dynamic {dynamic_id} not found in run {run_id}")
        
class DynamicTimseriesParsingError(BsxException):
    '''
    Dynamic timeseries CSV could not be parsed from BSX archive
    
    Parameters
    ----------
    run_id : str
        The id of the run that was searched
    dynamic_id : str
        The id of the dynamic that was searched
    '''
    
    def __init__(self, run_id: str, dynamic_id: str):
        self.run_id = run_id
        self.dynamic_id = dynamic_id
        super().__init__(f"Dynamic timeseries for dynamic {dynamic_id} could not be parsed from run {run_id}")

class BsxArchive:
    '''
    A Bifrost Super Import/Export (BSX) ZIP archive.
    
    This class is a wrapper around a zipfile.ZipFile object that provides
    some convenience methods for extracting and parsing the contents of
    a Bifrost Super Import/Export ZIP archive.
    '''
    def __init__(self, bsx_archive: Union[str, zf.ZipFile, bytes]):
        if isinstance(bsx_archive, str):
            self.bsx_archive = zf.ZipFile(bsx_archive)
        elif isinstance(bsx_archive, zf.ZipFile):
            self.bsx_archive = bsx_archive
        elif isinstance(bsx_archive, bytes):
            self.bsx_archive = zf.ZipFile(io.BytesIO(bsx_archive))
        else:
            raise TypeError(f'bsx_archive must be a string, zipfile.ZipFile or bytes, but is {type(bsx_archive)}')
        
        self._state_at_export = self.get_state()
        
    @staticmethod
    def _id_to_filesystem_name(id: str) -> str:
        return id.replace(':', '_')
        
    def get_settlement_id(self) -> str:
        '''
        Returns the id of the settlement that was exported.
        
        Returns
        -------
        str
            The id of the settlement that was exported.
        '''
        return safepointer.get(self._state_at_export, select.settlementName(), '')
    
    def get_state(self, run_id:Optional[str]=None) -> Dict[str, Any]:
        '''
        Returns the state object, either from a specific run or, when `run_id` is `None`
        the state of the settlement when the BSX archive was created.
        
        Parameters
        ----------
        run_id : Optional[str], optional
            The id of the run to get the state for, by default None
        
        Returns
        -------
        Dict[str, Any]
            The state object, either from a specific run or, when `run_id` is `None`
            the state of the settlement when the BSX archive was created.
        '''
        
        state_file = 'state.json'
        
        if run_id is not None:
            state_file = f'{self._id_to_filesystem_name(run_id)}/{state_file}'
        
        return json.loads(self.bsx_archive.read(state_file))
    
    def get_directory_fragment(self) -> Dict[str, Any]:
        '''
        Returns the directory fragment object.
        
        Returns
        -------
        Dict[str, Any]
            The directory fragment object.
        '''
        
        directory_fragment_file = 'directory_fragment.yaml'
        
        return yaml.safe_load(self.bsx_archive.read(directory_fragment_file))
    
    def get_runs_metadata(self, named_runs_only:bool=False) -> Dict[str, Dict[str, Any]]:
        '''
        Returns the metadata for runs, keyed by run id. When `named_runs_only` is `True`,
        only runs having a name are returned.
        
        Parameters
        ----------
        named_runs_only : bool, optional
            When `True`, only runs having a name are returned, by default False
        
        Returns
        -------
        Dict[str, Dict[str, Any]]
            The metadata for runs, keyed by run id.
        '''
        
        runs_metadata = safepointer.get(self._state_at_export, select.runsById(), {})
        
        if named_runs_only:
            runs_metadata = { id: runs_metadata[id] for id in runs_metadata if runs_metadata[id].get('description') is not None and runs_metadata[id].get('description') != '' }
            
        return runs_metadata
    
    @functools.lru_cache
    def get_dynamics_metadata(self, run_id: str) -> Dict[str, Dict[str, Any]]:
        '''
        Returns the metadata for all dynamics in the specified run, keyed by dynamic id.
        
        Parameters
        ----------
        run_id : str
            The id of the run to get dynamics metadata for.
            
        Returns
        -------
        Dict[str, Dict[str, Any]]
            The metadata for all dynamics in the specified run, keyed by dynamic id.
        '''
        
        # the folder name in the archive is the run id with colons replaced by underscores (to be a valid folder name on Windows)
        run_directory_name = self._id_to_filesystem_name(run_id)
        
        # [:-1] to remove trailing slash
        run_directories = [ p.filename[:-1] for p in self.bsx_archive.infolist() if p.is_dir() and p.filename.startswith('RUN') ]
        
        directory_matches = [ d for d in run_directories if d == run_directory_name ]
        if len(directory_matches) == 0:
            return []
        elif len(directory_matches) > 1:
            warnings.warn(f"Multiple run directories with name {run_directory_name} found in BSX archive, using first match.")
        
        run_directory = directory_matches[0]
        
        dynamics_metadata_file = f'{run_directory}/dynamics_metadata.json'
        
        dynamics_metadata = json.loads(self.bsx_archive.read(dynamics_metadata_file))
        
        return dynamics_metadata
    
    @functools.lru_cache
    def dynamic_timeseries_exists(self, run_id: str, dynamic_id: str) -> bool:
        '''
        Returns `True` if the specified dynamic exists in the specified run, `False` otherwise.
        
        Parameters
        ----------
        run_id : str
            The id of the run to check.
        dynamic_id : str
            The id of the dynamic to check.
        
        Returns
        -------
        bool
            `True` if the specified dynamic exists in the specified run, `False` otherwise.
        '''
        dynamics_metadata = self.get_dynamics_metadata(run_id)
        
        if dynamic_id not in [ dynamic['id'] for dynamic in dynamics_metadata ]:
            return False
        
        run_directory = self._id_to_filesystem_name(run_id)
        dynamic_id_file = self._id_to_filesystem_name(dynamic_id)
        
        timeseries_path = f'{run_directory}/dynamics_timeseries/{dynamic_id_file}.csv'
        
        try:
            if self.bsx_archive.getinfo(timeseries_path).is_dir():
                return False
        except KeyError:
            return False
        
        return True
    
    def get_dynamic_timeseries(self, run_id: str, dynamic_id: str) -> pd.DataFrame:
        '''
        Returns a pandas DataFrame containing the timeseries data for the specified dynamic.
        
        Columns
        -------
        - `Time` (datetime, index): The time of the data point in the simulation in seconds UTC
        - `Timestep` (int): The timestep of the data point in the simulation
        - `[0-9]+` (unknown):  The value of the dynamic, in one to multiple columns, if the dynamic is an array
        
        Parameters
        ----------
        run_id : str
            The id of the run to get the dynamic timeseries from
        dynamic_id : str
            The id of the dynamic to get the timeseries for
            
        Raises
        ------
        DynamicTimeseriesNotFoundError
            If the specified dynamic does not exist in the specified run.
        DynamicTimeseriesParsingError
            If the timeseries file for the specified dynamic could not be parsed.
        '''
        run_directory = self._id_to_filesystem_name(run_id)
        dynamic_id_file = self._id_to_filesystem_name(dynamic_id)
        
        timeseries_path = f'{run_directory}/dynamics_timeseries/{dynamic_id_file}.csv'
        
        try:
            
            with self.bsx_archive.open(timeseries_path) as f:
            
                try:
                    df = pd.read_csv(f, header=0)
                    
                    # rename the timestep column
                    df.rename(columns={'SimulationTime[s]': 'Timestep'}, inplace=True)
                    
                    # if there are more than two columns, then the dynamic is an array
                    if len(df.columns) > 2:
                        # rename the columns to keep only the index in the name
                        pattern = r".*_(?P<idx>\d+)"
                        replace = lambda m: m.group('idx')
                        
                        # replace the column names with the index
                        df.columns = df.columns.str.replace(pattern, replace, regex=True)
                    else:
                        df.columns = ['Timestep', '0']
                        
                    df['Time'] = pd.to_datetime(df['Timestep'], unit='s')
                    df.set_index('Time', inplace=True)
                    
                    # sort the columns by their index
                    # but the columns are strings so we need to convert to int first
                    # but some columns are not integers, so we need to handle that
                    
                    # first, get the columns that are not integers
                    non_integer_columns = [ c for c in df.columns if not c.isdigit() ]
                    
                    # then, get the columns that are integers
                    integer_columns = [ c for c in df.columns if c.isdigit() ]
                    
                    # then, sort the integer columns
                    integer_columns = sorted(integer_columns, key=lambda c: int(c))
                    
                    # then, put the columns back together
                    df = df[non_integer_columns + integer_columns]
                    
                    return df
                
                except pd.errors.EmptyDataError as e:
                    raise DynamicTimseriesParsingError(run_id, dynamic_id) from e
        
        except KeyError as e:
            raise DynamicTimeseriesNotFoundError(run_id, dynamic_id) from e
