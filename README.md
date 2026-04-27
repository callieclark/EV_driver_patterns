# Using large scale GPS data to reveal EV driver activity patterns beyond charging sessions



## Data access and confidentiality

**Raw mobility data are not included in this repository.**

This repo is structured for a public or semi-public release **without restricted data**. The mobility data were obtained through Cuebiq/Spectus under restricted access and are therefore not publicly available. 
Only non-sensitive derived outputs, code, documentation, and optionally small metadata tables or schemas should be committed.


## Repository structure

```text
code/

├── Results notebooks/
│   ├── run.ipynb
│   ├── EV00_settings.py
│   ├── EV01_pull_ping_data.ipy	
│   ├── EV02_process_ping_data.py
│   ├── EV03_pull_stop_data.ipy		
│   ├── EV04A_user_ping_rate_stats.ipy
│   ├── EV04C_filter_users.py	
│   ├── EV05_combine_EV_stations.py
│   └── EV05B_create_evcs_nearby_dict.py
│   ├── EV06_create_EVCS_slow_points.py
│   ├── EV07_create_EVCS_sessions.py
│   ├── EV08_create_gas_sessions.py
│   ├── EV09_create_model_inputs.py
│   ├── EV10_filter_EV_drivers_model.py
│   ├── EV11_identify_session_stops.py
│   ├── EV11A_identify_poi_stops.py
│   ├── EV99_fill_unobserved_points.py        
│
├── validation/
    ├── 00_create_sessions_panel.R
    └── demographic_validation.ipynb
├── behavior_analysis/
│   ├── EVCS_charging_behavior.ipynb
│   ├── POI_visit_analysis.ipynb
│   ├── POI_bundling_data_prep.ipynb
│   └── POI_daily_visits.ipynb
│   └── POI_bundling_analysis.ipynb
```

## Notebook workflow

### Processing notebooks

0. **run.ipynb**
   Runs the entire pipeline from EV01 to EV99. EV00_settings.py installs all dependencies and sets all variables. 
   
### Validation notebooks

1. **00_create_sessions_panel.R**  
   Cleans EV Watts data and creates a comparable panel of EVCS sessions.

2. **demographic_validation.ipynb**  
   Compares demographics of EV driver subset to all user subset to benchmark against surveys. 

### Behavior Analysis  notebooks

1. **EVCS_charging_behavior.ipynb
2. **POI_visit_analysis.ipynb
3. ** POI_bundling_data_prep.ipynb
4. ** POI_daily_visits.ipynb
5. **POI_bundling_analysis.ipynb



## Reproducibility notes

This analysis was conducted in Python and that the code for reproducing the main results from aggregated data is intended to be public. In practice, complete reproduction will depend on restricted mobility data access and on derived intermediate datasets that may need to be regenerated internally.



