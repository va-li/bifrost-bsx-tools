from pathlib import Path
import io
import zipfile as zf
import json
from typing import IO, List, Dict, Any, Union
import warnings

import pandas as pd
import bifrost_common_py.selectors as select
from bifrost_common_py.safepointer import safepointer

class BsxException(Exception):
    pass

class DynamicTimeseriesNotFoundError(BsxException):
    """Dynamic timeseries not found in BSX archive
    
    Parameters
    ----------
    run_id : str
        The id of the run that was searched
    dynamic_id : str
        The id of the dynamic that was searched
    """
    
    def __init__(self, run_id: str, dynamic_id: str):
        self.run_id = run_id
        self.dynamic_id = dynamic_id
        super().__init__(f"Dynamic timeseries for dynamic {dynamic_id} not found in run {run_id}")
        
class DynamicTimseriesParsingError(BsxException):
    """Dynamic timeseries CSV could not be parsed from BSX archive
    
    Parameters
    ----------
    run_id : str
        The id of the run that was searched
    dynamic_id : str
        The id of the dynamic that was searched
    """
    
    def __init__(self, run_id: str, dynamic_id: str):
        self.run_id = run_id
        self.dynamic_id = dynamic_id
        super().__init__(f"Dynamic timeseries for dynamic {dynamic_id} could not be parsed from run {run_id}")

class BsxArchive:
    '''
    A Bifrost Super Import/Export ZIP archive.
    
    This class is a wrapper around a zipfile.ZipFile object that provides
    some convenience methods for extracting and parsing the contents of
    a Bifrost Super Import/Export ZIP archive.
    '''
    def __init__(self, bsx_archive: Union[zf.ZipFile, bytes]):
        if isinstance(bsx_archive, zf.ZipFile):
            self.bsx_archive = bsx_archive
        else:
            self.bsx_archive = zf.ZipFile(io.BytesIO(bsx_archive))

        self.state = self._get_state(self.bsx_archive)
        self.runs_metadata = self._get_all_runs_metadata(self.state)
        self.dynamics_metadata = self._get_dynamics_metadata_from_state(self.state)
        self.settlement_id = self._get_settlement_id(self.state)
    
    @staticmethod
    def from_file(path: Union[str, Path]) -> 'BsxArchive':
        '''
        Creates a BsxArchive from a file.
        
        Parameters
        ----------
        path : Union[str, Path]
            The path to a BSX ZIP file.
            
        Returns
        -------
        BsxArchive
            A BsxArchive loaded from the specified file.
        '''
        return BsxArchive(zf.ZipFile(path))
    
    @staticmethod
    def _get_state(bsx_archive: zf.ZipFile) -> Dict[str, Any]:
        return json.loads(bsx_archive.read('state.json'))
    
    @staticmethod
    def _get_settlement_id(state: Dict[str, Any]) -> str:
        return safepointer.get(state, select.settlementName(), 'unnamed settlement')
    
    @staticmethod
    def _get_all_runs_metadata(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        return safepointer.get(state, select.runsById(), {})
    
    @staticmethod
    def _get_dynamics_metadata_from_state(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        allDynamicRefs: List[str] = safepointer.get(state, select.allDynamics(), {})
        allDynamicMetadata = [ safepointer.get(state, dynamicRef, {}) for dynamicRef in allDynamicRefs ]
        return dict(sorted({ dynamic['id']: dynamic for dynamic in allDynamicMetadata }.items()))
    
    def get_named_runs(self) -> Dict[str, Dict[str, Any]]:
        '''
        Returns the metadata for all runs that have a name assigned, keyed by run id.
        
        Returns
        -------
        Dict[str, Dict[str, Any]]
            The metadata for all runs that have a name assigned, keyed by run id.
        '''
        runs = self.runs_metadata
        return { id: runs[id] for id in runs if runs[id].get('description') is not None and runs[id].get('description') != '' }
    
    def get_runs(self) -> Dict[str, Dict[str, Any]]:
        '''
        Returns the metadata for all runs, keyed by run id.
        
        Returns
        -------
        Dict[str, Dict[str, Any]]
            The metadata for all runs, keyed by run id.
        '''
        return self.runs_metadata

    @staticmethod
    def _get_run_directories(bsx_archive: zf.ZipFile) -> List[zf.Path]:
        return [ p for p in zf.Path(bsx_archive).iterdir() if p.is_dir() and p.name.startswith('RUN') ]

    @staticmethod
    def _get_dynamics_metadata_from_archive(bsx: zf.ZipFile, run_id: str) -> List[Dict[str, Any]]:
        
        # the folder name in the archive is the run id with colons replaced by underscores (to be a valid folder name on Windows)
        run_directory_name = run_id.replace(':', '_')
        
        run_directories = BsxArchive._get_run_directories(bsx)
        
        directory_matches = [ d for d in run_directories if d.name == run_directory_name ]
        if len(directory_matches) == 0:
            return []
        elif len(directory_matches) > 1:
            warnings.warn(f"Multiple run directories with name {run_directory_name} found in BSX archive, using first match.")
        
        run_directory = directory_matches[0]
        
        dynamics_metadata = json.loads((run_directory / 'dynamics_metadata.json').read_bytes())
        
        return dynamics_metadata
    
    def get_run_state(self, run_id: str) -> Dict[str, Any]:
        '''
        Returns the state of the specified run.
        
        Parameters
        ----------
        run_id : str
            The id of the run to get the state of.
        
        Returns
        -------
        Dict[str, Any]
            The state of the specified run.
        '''
        run_directory_name = run_id.replace(':', '_')
        state_path = zf.Path(self.bsx_archive) / run_directory_name / 'state.json'
        return json.loads(state_path.read_bytes())
    
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
        dynamics_metadata = BsxArchive._get_dynamics_metadata_from_archive(self.bsx_archive, run_id)
        
        if dynamic_id not in [ dynamic['id'] for dynamic in dynamics_metadata ]:
            return False
        
        run_directory_name = run_id.replace(':', '_')
        dynamic_id_name = dynamic_id.replace(':', '_')
        
        timeseries_path = zf.Path(self.bsx_archive) / run_directory_name / 'dynamics_timeseries' / (dynamic_id_name + '.csv')
        if not timeseries_path.exists() or not timeseries_path.is_file():
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
        run_directory_name = run_id.replace(':', '_')
        dynamic_id_name = dynamic_id.replace(':', '_')
        
        timeseries_path = zf.Path(self.bsx_archive) / run_directory_name / 'dynamics_timeseries' / (dynamic_id_name + '.csv')
        
        if not timeseries_path.exists() or not timeseries_path.is_file():
            raise DynamicTimeseriesNotFoundError(run_id, dynamic_id)
        try:
            df = pd.read_csv(timeseries_path.open(), header=0)
            column_names = ['Timestep'] + [str(i) for i in range(len(df.columns) - 1)]
            df.columns = column_names
            
            df['Time'] = pd.to_datetime(df['Timestep'], unit='s')
            df.set_index('Time', inplace=True)
        except pd.errors.EmptyDataError as e:
            raise DynamicTimseriesParsingError(run_id, dynamic_id) from e
        
        return df
