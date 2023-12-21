"""
 Transformation of images
"""
import shutil
import os
import pathlib
import logging
from typing import List
import glob
import uuid
import numpy as np
import geopandas as gpd
import rasterio
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject, Resampling

log = logging.getLogger(__name__)

def normilize_s2(arr:np.array)->np.array:
    new_arr = np.clip(arr/10000, 0, 1)
    new_arr = new_arr*255
    return new_arr.astype(np.uint8)

def clip_by_polygon(input_img:str, gdf_bbox:gpd.GeoDataFrame, output_img:str)->str:
    with rasterio.open(input_img) as src:
        clip_image, out_transform = mask(src, gdf_bbox.geometry, crop=True)
        out_meta = src.meta.copy()
        out_meta.update({"driver": "GTiff",
                         "height": clip_image.shape[1],
                         "width": clip_image.shape[2],
                         "transform": out_transform})
        with rasterio.open(output_img, "w+", **out_meta) as dest:
            dest.write(clip_image)
    return output_img

def band_stack(imgs: List[str], output_img:str, normalize:bool=False):
    meta_out = rasterio.open(imgs[0]).meta.copy()
    meta_out.update({'driver':'GTiff', 'count':len(imgs)})
    with rasterio.open(output_img, 'w+', **meta_out) as dest:
        for band_nr, band in enumerate(imgs, start=1):
            with rasterio.open(band) as src_bnd:
                if normalize:
                    dest.write(normilize_s2(src_bnd.read(1)), band_nr)
                else:
                    dest.write(src_bnd.read(1), band_nr)
    return output_img

def reproject_to_wgs84(input_img:str, output_img:str, dst_crs:str='epsg:4326')-> str:
    with rasterio.open(input_img, 'r') as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds)
        kwargs = src.meta.copy()
        kwargs.update({
            "driver": "GTiff",
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height
        })
        with rasterio.open(output_img, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest)
    return output_img

def mosaic_images(input_imgs:List[str], output_img:str):
    raster_to_mosaic = [rasterio.open(img) for img in input_imgs]
    mosaic, output = merge(datasets=raster_to_mosaic,
                           resampling=Resampling.bilinear,
                           nodata=0,
                           method='first' #['first', 'last', 'min', 'max']
                           )
    output_meta = raster_to_mosaic[0].meta.copy()
    output_meta.update(
        {"driver": "GTiff",
         "height": mosaic.shape[1],
         "width": mosaic.shape[2],
         "transform": output,
         }
    )
    with rasterio.open(output_img, "w+", **output_meta) as dest:
        dest.write(mosaic)
    return output_img

def copy_remote(local_path:str, remote_path:str):
    try:
        shutil.copyfile(local_path, remote_path)
    except Exception as e:
        log.exception(f"Failed to upload file to SDS: {e}")

class Tx():
    def __init__(self, sample:List[str], uuid, local_dir,
                 tile:str, date: str, format:str, reproject_4326:bool=False):
        self.sample = sample
        self.bands = len(sample)
        self.tile = tile
        self.date = date
        self.uuid = str(uuid)
        self.format = format
        self.cache = local_dir
        self.wgs84 = reproject_4326

    def etl_process_tile(self, tempfolder:str):
        if self.format == 'UINT8':
            norm_img = True
        else:
            norm_img = False
        self.stack = band_stack(imgs=self.sample, output_img=os.path.join(tempfolder, f'{self.tile}_{self.date}.tif'), normalize=norm_img)
        if self.wgs84:
            self.wgs84 = reproject_to_wgs84(self.stack, os.path.join(tempfolder, pathlib.Path(self.stack).name))
            copy_remote(self.wgs84, os.path.join(self.cache, f'{self.uuid}.tif'))
        else:
            copy_remote(self.stack, os.path.join(self.cache, f'{self.uuid}.tif'))


    def etl_process_by_polygon(self, tempfolder:str, gdf:gpd.GeoDataFrame):
        if self.format == 'UINT8':
            norm_img = True
        else:
            norm_img = False
        self.stack = band_stack(imgs=self.sample, output_img=os.path.join(tempfolder, f'{self.tile}_{self.date}.tif'),
                                normalize=norm_img)

        self.stack = band_stack(imgs=self.sample, output_img=os.path.join(tempfolder, f'{self.tile}_{self.date}.tif'),
                                normalize=norm_img)
        self.clip = clip_by_polygon(self.stack, gdf_bbox=gdf,output_img=os.path.join(tempfolder, f'{self.tile}_{self.date}_clip.tif'))
        if self.wgs84:
            self.wgs84 = reproject_to_wgs84(self.clip, os.path.join(tempfolder, pathlib.Path(self.stack).name))
            copy_remote(self.wgs84, os.path.join(self.cache, f'{self.uuid}.tif'))
        else:
            copy_remote(self.clip, os.path.join(self.cache, f'{self.uuid}.tif'))






