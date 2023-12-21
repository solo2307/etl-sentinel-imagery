from imagery_store import CopernicusHubOperator
import hydra
from omegaconf import DictConfig
import pandas as pd
import geopandas as gpd
import shapely

from code.dataset import AreaDataset

import logging
log = logging.getLogger(__name__)

import warnings
warnings.filterwarnings('ignore')

def read_file_as_gdf(aoi_path:str, out_crs:str='epsg:4326')-> gpd.GeoDataFrame:
    if aoi_path.endswith('.csv'):
        region_df = pd.read_csv(aoi_path)
        region_gdf = gpd.GeoDataFrame(region_df,
                                      geometry=[shapely.from_wkt(geom) for geom in region_df.geometry],
                                      crs=out_crs)
    elif aoi_path.endswith(tuple(['.geojson', '.gpkg', '.shp'])):
        region_gdf = gpd.read_file(aoi_path)
        # Reproject CRS of aoi to WGS84
        if region_gdf.crs != out_crs:
            region_gdf = region_gdf.to_crs(out_crs)
        return region_gdf
    else:
        log.error("Error: File of AOI is in a wrong format!")
        return None



@hydra.main(version_base=None, config_path="../conf", config_name='config')
def main(cfg:DictConfig):
    if len(cfg.data.tile_ids)==0:
        gdf_aoi = pd.DataFrame(read_file_as_gdf(aoi_path='data/toulouse_bbox_wgs84.geojson'))
    else:
        df_tiles = pd.DataFrame(cfg.data.tile_ids, columns=['tile_id'])


    dataset = AreaDataset(area_descriptor=gdf_aoi, imagery_directory=cfg.cache.feature_dir, config=cfg)

    # # Process product from selected dataset
    # for idx, row in dataset.area_descriptor.iterrows():
    #     try:
    #         process_product(idx, row, dataset, cfg, sds, db, down_df)
    #     except Exception as e:
    #         log.info(f"Failed to process tile = {row['tile_id']}")




    log.info('Data is retrieved')

if __name__ == "__main__":
    main()