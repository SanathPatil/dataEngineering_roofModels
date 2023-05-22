import json
import os

import pandas as pd
# pd.options.display.max_columns = None
import numpy as np
import glob
import logging


def flatten_json(df, col_name, record_path, meta):
    """
    A generic function which flattens the json file using pandas json normalize()
    :param df: df to be flattened
    :param col_name: explode the df based on the col_name specified
    :return: flattened dataframe final_df
    """
    logging.info(f"Starting flatten_json for {df}")
    try:
        final_df = pd.DataFrame()
        if (len(df[col_name].values[0]) > 0):
            explode_df = df.explode(col_name)
            # print(df.explode(col_name))

            if record_path and meta:
                final_df = pd.json_normalize(data=explode_df[col_name],
                                             record_path=record_path,
                                             record_prefix=col_name + '_' + '_'.join(record_path) + "_",
                                             meta=meta,
                                             errors='ignore')
            else:
                final_df = pd.json_normalize(data=explode_df[col_name], errors='ignore')
    except Exception as e:
        logging.error(f"failed to flatten due to {e}")
    return final_df


def get_mounting_dfs(df_main):
    """
    Calls flatten_json() to extract MountingPlane values.
    :return: 3 dfs penetration, exterior and interior_rings all under Buildings->MountingPlanes with appropriate prefixes
    """
    groupby_col_name = 'siteModel_buildings_mountingPlanes_id'
    gdf = df_main.groupby(groupby_col_name)
    penetration_edges_df = pd.DataFrame()
    polygon_exterior_ring_edges_df = pd.DataFrame()
    polygon_interior_ring_edges_df = pd.DataFrame()
    try:
        for plane_id, plane_id_df in gdf:
            ###############################################################
            ############# mountingPlanes->penetrations->edges #############
            ###############################################################

            col_name = 'siteModel_buildings_mountingPlanes_penetrations'
            record_path = ['ring', 'edges']
            meta = ['id', 'obstructionId']
            # if the dataframe contains values for the col=siteModel_buildings_mountingPlanes_penetrations only proceed
            if pd.notna(plane_id_df[col_name]).all():  # len(plane_id_df[col_name].values[0]) > 0:
                temp_pen_df = flatten_json(plane_id_df, col_name, record_path, meta)
                temp_pen_df = temp_pen_df.drop('siteModel_buildings_mountingPlanes_penetrations_ring_edges_id', axis=1)
                temp_pen_df.rename(columns={'id': 'siteModel_buildings_mountingPlanes_penetrations_ring_edges_id'}, \
                                   inplace=True)
                temp_pen_df.rename(
                    columns={'obstructionId': 'siteModel_buildings_mountingPlanes_penetrations_ring_edges_obstructionId'}, \
                    inplace=True)
                temp_pen_df.insert(0, groupby_col_name, plane_id)
                penetration_edges_df = pd.concat([temp_pen_df, penetration_edges_df])

            #     ###############################################################
            #     ################ Polygon->exteriorRing->edges #################
            #     ###############################################################


            col_name = 'siteModel_buildings_mountingPlanes_polygon.exteriorRing.edges'
            record_path = ''
            meta = ''
            if pd.notna(plane_id_df[col_name]).all():
                temp_exter_df = flatten_json(plane_id_df, col_name, record_path, meta)
                temp_exter_df = temp_exter_df.add_prefix('siteModel_buildings_mountingPlanes_polygon_exteriorRing_edges_')
                temp_exter_df.insert(0, groupby_col_name, plane_id)
                polygon_exterior_ring_edges_df = pd.concat([temp_exter_df, polygon_exterior_ring_edges_df])

            #     ###############################################################
            #     ################ Polygon->interiorRing->edges #################
            #     ###############################################################

            col_name = 'siteModel_buildings_mountingPlanes_polygon.interiorRings'
            record_path = ['edges']
            meta = 'windingDirection'

            if pd.notna(plane_id_df[col_name]).all():
                temp_inter_df = flatten_json(plane_id_df, col_name, record_path, meta)
                temp_inter_df.insert(0, groupby_col_name, plane_id)
                temp_inter_df.rename(columns={
                    'windingDirection': 'siteModel_buildings_mountingPlanes_polygon_interiorRing_edges_windingDirection'}, \
                                     inplace=True)
                polygon_interior_ring_edges_df = pd.concat([temp_inter_df, polygon_interior_ring_edges_df])
    except Exception as e:
        logging.error(f"Failed to get Mounting Plane dfs due to {e}")


    return penetration_edges_df, polygon_interior_ring_edges_df, polygon_exterior_ring_edges_df


def get_mounting_planes(data):
    col_name = 'siteModel_buildings_mountingPlanes_penetrations'
    try:
        df_main = pd.json_normalize(data=data,
                                    record_path=['siteModel', 'buildings', 'mountingPlanes']
                                    , record_prefix="siteModel_buildings_mountingPlanes_"
                                    ,
                                    meta=['id', 'installationId', 'dateCreated', 'version', 'externalSiteModelSourceId', \
                                          ['siteModel', 'buildings', 'isPrimaryBuilding'], \
                                          ['siteModel', 'buildings', 'totalRoofArea']
                                          ], errors='ignore')

        df_main = df_main.replace(['', list, None, 'None', 'none'], np.nan)
        ################## Code base supported versions- V1,V2 or V3 ###################
        if df_main['version'].unique() not in ['v1', 'v2', 'v3']:
            raise Exception("Only, 1 to 3 versions are supported")

        if pd.notna(df_main[col_name]).all():
            df_main[col_name] = df_main[col_name].apply(lambda y: np.nan if len(y) == 0 else y)
        pen_df, poly_inter_df, poly_exter_df = get_mounting_dfs(df_main)

    except Exception as e:
        logging.error("failure to extract get_mounting_planes!!!", e)

    return pen_df, poly_inter_df, poly_exter_df, df_main


def concat_mounting_plane_dfs(pen_df,poly_inter_df,poly_exter_df,df_main):
    """
    Concatenates Mounting Plane dataframes with the main dataframe
    :return: concatenated  final dataframe
    """
    try:
        polygon_exter_inter_df = pd.concat([poly_exter_df,poly_inter_df])
        polygon_main_df = df_main.merge(polygon_exter_inter_df,how='left')
        if pen_df.shape[0]!=0:
            final_penetration_df = polygon_main_df.merge(pen_df, how='left')
        else:
            final_penetration_df=polygon_main_df
        ### Drop unwanted columns
        final_penetration_df = final_penetration_df.\
            drop(['siteModel_buildings_mountingPlanes_penetrations',\
                  'siteModel_buildings_mountingPlanes_polygon.exteriorRing.edges',
                 'siteModel_buildings_mountingPlanes_polygon.interiorRings'], axis=1)
    except Exception as e:
        logging.error(f"Failed to concatenate dfs for mounting planes due to {e}")
    return final_penetration_df

def get_bldngs_polygons():
    """
    :return: values for Sitemodel->Buildings->Polygon
    """
    try:
        buildings_polygon_df =pd.json_normalize(data=data
                ,record_path=['siteModel','buildings','polygon','exteriorRing','edges']
                ,record_prefix="siteModel_buildings_polygon_exteriorRing_edges_"
                ,meta = ['id', 'installationId']
                ,errors='ignore')
    except Exception as e:
        logging.error(f"Failed to extract building Polygon data due to {e}")
    return buildings_polygon_df


def get_obs_df(df_observation_main):
    try:
        obstructions_ring_edges_df = pd.DataFrame()
        if df_observation_main.shape[0] != 0:

            groupby_col_name = 'siteModel_obstructions_id'
            gdf_obs = df_observation_main.groupby(groupby_col_name)

            for plane_id, plane_id_df in gdf_obs:
                ###############################################################
                ##################### Obstructions->ring->edges ###############
                ###############################################################

                col_name = 'siteModel_obstructions_ring.edges'
                record_path = ''
                meta = ''
                # if the dataframe contains values for the col=siteModel_buildings_mountingPlanes_penetrations only proceed
                if pd.notna(plane_id_df[col_name]).all():
                    temp_obs_df = flatten_json(plane_id_df, col_name, record_path, meta)
                    temp_obs_df = temp_obs_df.add_prefix('siteModel_obstructions_edges_')

                    temp_obs_df.insert(0, groupby_col_name, plane_id)
                    obstructions_ring_edges_df = pd.concat([temp_obs_df, obstructions_ring_edges_df])
    except Exception as e:
        logging.error(f'Failed to get Obstructions due to {e}')
    return obstructions_ring_edges_df


def merge(obstructions_ring_edges_df, df_observation_main):
    logging.info("Begin merging of obstructions dfs")
    try:
        if obstructions_ring_edges_df.shape[0] != 0:
            final_obs_df = df_observation_main.merge(obstructions_ring_edges_df, how='left').drop(
                columns=['siteModel_obstructions_ring.edges'])
            final_obs_df.head()
        else:
            final_obs_df = obstructions_ring_edges_df
    except Exception as e:
        logging.error(f" Failed to merge Obstructions df due to {e}")
    logging.info("Merging of obstructions dfs completed!!!")
    return final_obs_df


def get_obstruction_df():
    """
    :return: Returns Building->Obstruction df
    """
    try:
        obs_df = pd.DataFrame()
        col_name = 'siteModel_obstructions_ring.edges'
        df_observation_main = pd.json_normalize(data=data
                                                , record_path=['siteModel', 'obstructions']
                                                , record_prefix="siteModel_obstructions_"
                                                , meta=['id', 'installationId', 'dateCreated', 'version',
                                                        'externalSiteModelSourceId']
                                                , errors='ignore')

        df_observation_main = df_observation_main.replace(['', list, None, 'None', 'none'], np.nan)
        if df_observation_main.shape[0] != 0:
            df_observation_main[col_name] = df_observation_main[col_name].apply(lambda y: np.nan if len(y) == 0 else y)
            obstructions_ring_edges_df = get_obs_df(df_observation_main)
            obs_df = merge(obstructions_ring_edges_df, df_observation_main)
    except Exception as e:
        logging.error(f"Failed to get_obstruction df due to {e}")
    return obs_df

def get_Statistics(final_mount_df):
    rooftype_grp = final_mount_df.groupby(['siteModel_buildings_mountingPlanes_roofMaterialType', 'id'])
    # summary statistic based on rooftype - ANALYST requirement
    stats_df = rooftype_grp['siteModel.buildings.totalRoofArea', 'siteModel_buildings_mountingPlanes_area'].describe()
    print(stats_df)

def angel_fix(val,angle_name):
    if angle_name=='siteModel_buildings_mountingPlanes_pitchAngle':
        if val >45:
            return 45
        elif val < 30:
            return 30
        else:
            return val
    else:
        if val >270:
            return 270
        elif val < 90:
            return 90
        else:
            return val

def angle_precession(final_mount_df):
    """
    Based on understanding ensuring pitch angle is between 30-45 deg and Azimuth Angle between 90-270 deg
    """
    try:
        df = final_mount_df.copy()
        pitch_col_name='siteModel_buildings_mountingPlanes_pitchAngle'
        azimuth_col_name='siteModel_buildings_mountingPlanes_azimuthAngle'
        df[pitch_col_name] = df[pitch_col_name].astype(float)
        df[azimuth_col_name] = df[azimuth_col_name].astype(float)
        ################## PitchAngle Fix ##############################
        df[pitch_col_name]=df[pitch_col_name].apply(lambda x: angel_fix(x, pitch_col_name))

        ################## AzimuthAngle Fix ##############################
        df[azimuth_col_name] = df[azimuth_col_name].apply(lambda x: angel_fix(x, azimuth_col_name))
    except Exception as e:
        logging.error(f"Failed to fix angles due to {e}")
    return df


if __name__=='__main__':

    final_mount_df = pd.DataFrame()
    final_obs_df = pd.DataFrame()
    final_poly_df = pd.DataFrame()
    data = ''

    for path in os.listdir('roof_models'):
        print(path)
        with open('roof_models/'+path) as f:
            data = json.load(f)
        ############## MountingPlanes -> Penetrations ###########
        pen_df, poly_inter_df, poly_exter_df, df_main = get_mounting_planes(data)
        temp_mounting_df = concat_mounting_plane_dfs(pen_df, poly_inter_df, poly_exter_df, df_main)
        final_mount_df = pd.concat([temp_mounting_df, final_mount_df])
        final_mount_df.to_csv("outputfiles/mountingPlanes.csv")
        ############### Buildings -> Obstructions ###############
        temp_obs_df = get_obstruction_df()
        final_obs_df = pd.concat([temp_obs_df, final_obs_df])
        final_obs_df.to_csv("outputfiles/obstructions.csv")
        ############ Buildings -> Polygons ######################
        temp_poly_df = get_bldngs_polygons()
        final_poly_df = pd.concat([temp_poly_df, final_poly_df])
        final_poly_df.to_csv("outputfiles/buildingPolygon.csv")
    # print(final_mount_df)
    print(final_mount_df.shape)

    ##########################################################
    ############## Stats for Data Analyst ###################
    ##########################################################
    get_Statistics(final_mount_df)

    ##########################################################
    ############## Precise Mounting Angles ###################
    ##########################################################
    precise_angle_df = angle_precession(final_mount_df)
    print(final_mount_df.shape)
    print(final_obs_df.shape)
    print(final_poly_df.shape)
    print(precise_angle_df.shape)