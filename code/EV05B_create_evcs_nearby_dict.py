################################################################################
# Pull stop data from server
# Created on: 12/18/2025
# Created by:  Callie

#This script creates a dictionary for each EVCS port of all other EVCS ports in the data within 100m 
################################################################################


################################################################################
# prep
################################################################################

# bring in all settings
exec(open('EV00_settings.py').read())

from scipy.spatial import cKDTree
import pickle


################################################################################
# functions
################################################################################from scipy.spatial 
def find_evcs_within_distance(df_base, evcs_gdf,max_distance):
    """
    For each point in df_base, find all EVCS points within max_distance (meters).

    Returns
    -------
    dict
        { df_base_ID : [evcs_ID_1, evcs_ID_2, ...] }
    """

    # Prepare GeoDataFrames
    df = prepare_gdf(df_base).dropna(subset=["geometry"])
    evcs = prepare_gdf(evcs_gdf).dropna(subset=["geometry"])

    # Ensure CRS match
    assert df.crs == evcs.crs, "Coordinate reference systems do not match!"

    # Extract coordinates
    df_coords = np.column_stack((df.geometry.x, df.geometry.y))
    evcs_coords = np.column_stack((evcs.geometry.x, evcs.geometry.y))

    # Build KD-tree on EVCS locations
    tree = cKDTree(evcs_coords)

    # Query ALL EVCS within max_distance for each df point
    indices_list = tree.query_ball_point(df_coords, r=max_distance)

    # Build output dictionary
    output = {}
    for base_id, evcs_indices in zip(df["ID"], indices_list):
        output[base_id] = evcs.loc[evcs_indices, "ID"].tolist()

    return output
################################################################################
# run
################################################################################

evcs_df=pd.read_csv(f"{root}data/evcs_locations/{current_city}/evcs_combined_{max_distance_combine_stations}m_buffer.csv")
evcs_df=evcs_df[['ID','Latitude','Longitude','geometry']].copy()

result_dict=find_evcs_within_distance(evcs_df, evcs_df, max_distance=max_distance_multiple_station_visits)

with open(f"{data_path}evcs_locations/{current_city}/evcs_within_{max_distance_multiple_station_visits}m_dict.pkl", "wb") as f:
    pickle.dump(result_dict, f)

# To pull in

# with open(f"{data_path}evcs_locations/{current_city}/evcs_within_{max_distance_multiple_station_visits}m_dict.pkl", "rb") as f:
#     output_dict = pickle.load(f)
# output_dict