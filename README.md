# Bifrost BSX Tools

Extract time series from [Bifrost](https://bifrost.siemens.com) BSX files as pandas DataFrame.

Compatible with Bifrost v6 "Sindri".

## Installation

```bash
pip install bifrost_bsx_tools
```

## Usage

### Load a BSX file and get the settlement ID

```python
from bifrost_bsx_tools import BsxArchive

bsx = BsxArchive('scenario_a.bsx')
bsx.get_settlement_id()
```

Output:

```python
'MyFancySettlement'
```

---

### List the runs in the archive

Optinally, add the keyword argument `named_runs_only=True` to get only runs with names.

```python
bsx.get_runs_metadata()
```

Output:

```python
{
  'RUN:10601920-ac83-11ed-b4f1-0bee431b5633': {
    'startTime': 5097600,
    'timeHorizon': 7948800,
    'prefetchStep': 900,
    'description': 'run #1', # <- this is the name
    'timestamp': 1676391443,
    'tags': [],
    'scenarioHash': 'e2889fdc93ffba3511598322cc13252f3e768c4826f31c400e739ee62a66475d',
    'complete': True,
    'persisted': True,
    'historic': False
  },
  ...
}
```

---

### Get the metadata of dynamics present in a run

```python
run_id = 'RUN:10601920-ac83-11ed-b4f1-0bee431b5633'
bsx.get_dynamics_metadata(run_id)
```

Output:

```python
[
  {
    'id': 'SUN-ALTITUDE:0f2bf2d1-ac6b-11ed-9a57-2d85e0d4252e',
    'cardinality': 1,
    'type': 'number'
  },
  ...
]
```

---

### Does a timeseries exist for a dynamic?

Checks if a file with the name `SUN-ALTITUDE_0f2bf2d1-ac6b-11ed-9a57-2d85e0d4252e.csv` exists in the run folder. Bifrost sometimes does not create a file for every dynamic when it has not been stored in the InfluxDB.

```python
dynamic_id = 'SUN-ALTITUDE:0f2bf2d1-ac6b-11ed-9a57-2d85e0d4252e'
bsx.dynamic_timeseries_exists(run_id, dynamic_id)
```

Output:

```python
True
```

### Get the time series for a run

Returns a pandas DataFrame with the time series for the dynamic.

```python
bsx.get_dynamic_timeseries(run_id, dynamic_id)
```

Output:

| Time | Timestep | 0 |
| --- | ---: | ---: |
| 1970-03-01 00:00:00 | 5097600 | 138 |
| 1970-03-01 00:15:00 | 5098500 | 137 |
| 1970-03-01 00:30:00 | 5099400 | 135 |
| 1970-03-01 00:45:00 | 5100300 | 134 |
| 1970-03-01 01:00:00 | 5101200 | 132 |
| ... | ... | ... |
| 1970-05-31 23:00:00 | 13042800 | 109 |
| 1970-05-31 23:15:00 | 13043700 | 109 |
| 1970-05-31 23:30:00 | 13044600 | 109 |
| 1970-05-31 23:45:00 | 13045500 | 108 |
| 1970-06-01 00:00:00 | 13046400 | 108 |




