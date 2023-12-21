from pathlib import Path
import rasterio
import pandas as pd
import geopandas as gpd
import shapely
from omegaconf import DictConfig
from imagery_store import CopernicusHubOperator
from typing import Optional, Union
import logging
log = logging.getLogger(__name__)

import warnings
warnings.filterwarnings('ignore')


class AreaDataset():
    """Dataset for area of interest"""
    def __init__(self,
                 area_descriptor: Optional[Union[pd.DataFrame,gpd.GeoDataFrame]],
                 imagery_directory: str,
                 config: DictConfig
                 ):
        self.config = config
        self.imagery_dir = Path(imagery_directory)
        self.imagery_store = CopernicusHubOperator(api_url=self.config.api_url,
                 api_id=self.config.api_id,api_secret=self.config.api_secret, cache_dir=self.imagery_dir)
        self.area_descriptor = area_descriptor
        if not self.imagery_dir.exists():
            self.imagery_dir.mkdir(parents=True, exist_ok=True)

    def __len__(self):

        return len(self.area_descriptor)

    def __getitem__(self, idx):
        area = self.area_descriptor.iloc[idx]
        if isinstance(area['geometry'], str):
            area_coords = tuple(shapely.from_wkt(area['geometry']).bounds)
        else:
            area_coords = tuple(area['geometry'].bounds)

        # Select and download S2 tile
        self.imagery_store.imagery(area_coords=area_coords,
                              tile_id=str(area['tile_id']),
                              cfg=self.config,
                              start_date=str(self.config.start_date),
                              end_date=str(self.config.end_date),
                              resolution=int(self.config.resolution))

        product = self.imagery_store.product

        # Append a new to DB
        if len(product.items())>0:
            item_path = f"{self.imagery_dir}/{product['uuid']}.tif"
            product.update({ 'crs': 'epsg:4326',
                             'local_dir': item_path,
            })

        return product







