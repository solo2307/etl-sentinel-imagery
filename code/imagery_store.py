import datetime
from shapely.geometry import box, shape
from pathlib import Path
from typing import Tuple, Dict
import numpy as np
import pandas as pd
import geopandas as gpd
import tempfile
import os
import glob
import re
import logging
import requests
import tqdm
import xml.etree.ElementTree as ET
from omegaconf import DictConfig
from code.exception import OperatorInteractionException
from code.tx import Tx

log = logging.getLogger(__name__)
logging.getLogger('rasterio._filepath').setLevel(logging.ERROR)

class CopernicusHubOperator():
    def __init__(self,
                 api_url:str,
                 api_id: str,
                 api_secret: str,
                 cache_dir: Path):
        self.config = {'user': api_id,
                       'password': api_secret,
                       'api_url':  api_url
                       }
        self.data_folder = cache_dir
        self.data_folder.mkdir(parents=True, exist_ok=True)
        self.session_starttime = datetime.datetime(2000,1,1,0,0,0)

    def imagery(self,
                area_coords: Tuple,
                start_date: str,
                end_date: str,
                cfg: DictConfig,
                tile_id:str,
                resolution: int = 10) -> tuple[Dict[str, np.ndarray], tuple[int, int]]:

        # Scan of S2 products for search period: max output selection is 20 products
        self.bbox_aoi = box(*area_coords)
        self.bands = cfg.bands

        #Select product to download
        self.select_product_by_tile(api_url=self.config['api_url'],
                                    start_date=start_date,
                                    end_date=end_date,
                                    platform_name=cfg.platform_name,
                                    product_type=cfg.product_type,
                                    tile_id=tile_id,
                                    cloud_coverage_max=cfg.cloud_coverage_max)

        # Download product and process bands
        if len(self.product)>0:
            with tempfile.TemporaryDirectory(dir=self.data_folder) as tmpfolder:
                self.download_product(product_id=self.product['uuid'],
                                      product_name=self.product['name'],
                                      local_dir=tmpfolder,
                                      resolution=resolution)
                if len(os.listdir(tmpfolder)) > 1:
                    try:
                        sample = glob.glob(f'{tmpfolder}/*.jp2')
                        sample.sort()
                        tx = Tx(sample, uuid=self.product['uuid'], local_dir=self.data_folder,
                           tile=self.product['tile'], date=self.product['product_date'], format=cfg.format)
                        tx.etl_process(tmpfolder)
                    except Exception:
                        log.exception("Sample transformation failed")
                else:
                    log.info("Download request failed")
                    raise OperatorInteractionException(
                        'CopernicusHub operator interaction not possible. Please check the account devices activity : https://dataspace.copernicus.eu/')
    def read_product_metadata(self, uuid):
        try:
            meta_url = f"{self.config['api_url']}/Products({uuid})"
            metadata = requests.get(meta_url).json()
            name = metadata['Name']
            return {'uuid': uuid,
                    'name': name,
                    'geometry_wkt': metadata['Footprint'] .split(';')[-1].replace("'", ""),
                    "crs" : metadata['Footprint'] .split(';')[0].replace("'", "").split('=')[-1],
                    'date' :metadata['OriginDate'].split('T')[0]}
        except Exception as e:
            log.info(f"Product uuid is wrong: {e}")
            return None

    def download_product(self, product_id:str, product_name:str, local_dir:str, resolution:str):
        # Download XML metadata file of the product
        self.product.update(self.read_product_metadata(product_id))
        try:
            meta_url = f"{self.config['api_url']}/Products({product_id})/Nodes({product_name})/Nodes(MTD_MSIL2A.xml)/$value"
            response = self.get_session().get(meta_url, allow_redirects=False)

            while response.status_code in (301, 302, 303, 307):
                meta_url = response.headers["Location"]
                response = self.get_session().get(meta_url, allow_redirects=False)

            #Save XML file into tempfolder
            outfile = Path(f"{local_dir}/MTD_MSIL2A.xml")
            outfile.write_bytes(response.content)
        except Exception :
            log.info("Request to download product metadat is failed")

        # Read XML metadat file
        try:
            xml_file = ET.fromstring(response.content)
        except Exception as e:
            log.info(f"{e}. \n Rerun token access")
            xml_file = ET.parse(str(outfile))
            xml_file = xml_file.getroot()

        # Product meta data
        band_location = [f"{product_name}/{f.text}.jp2".split("/") for f in xml_file.iter() for i in self.bands if f.tag == "IMAGE_FILE" and re.match(f".*_{i}_{str(resolution)}m", f.text)]
        product_date = product_name.split("_")[2][:4] + "-" + product_name.split("_")[2][4:6] + "-" + product_name.split("_")[2][6:8]
        bands = [f[-1].split("_")[2] for f in band_location]
        self.product.update({ 'uuid':product_id,
                        'product_date': product_date,
                        'platform': product_name.split("_")[0],
                        'product_type': product_name.split("_")[1],
                        'orbit_number': product_name.split("_")[4],
                        'cloudcoverage': [float(f.text) for f in xml_file.iter() if f.tag == 'Cloud_Coverage_Assessment'][0],
                        'orbitdirection': [f.text for f in xml_file.iter() if f.tag == 'SENSING_ORBIT_DIRECTION'][0],
                        'tile': product_name.split("_")[5][1:],
                        'nodata': [int(f.text) for f in xml_file.iter() if f.tag == 'SPECIAL_VALUE_INDEX'][0],
                        "bands":  bands,
                        'num_bands': len(bands),
                        })

        # Build the url for each file using Nodes() method
        for band_file in tqdm(band_location):
            url = f"{self.config['api_url']}/Products({product_id})/Nodes({product_name})/Nodes({band_file[1]})/Nodes({band_file[2]})/Nodes({band_file[3]})/Nodes({band_file[4]})/Nodes({band_file[5]})/$value"
            response = self.get_session().get(url, allow_redirects=False)

            while response.status_code in (301, 302, 303, 307):
                url = response.headers["Location"]
                response = self.get_session().get(url, stream=True)

            # Download the product into a local directory
            with open(Path(local_dir)/band_file[5],'wb') as output_img:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        output_img.write(chunk)

    def get_session(self):
        current_time = datetime.datetime.now()

        if (current_time - self.session_starttime).seconds >= 600:
            response = self.get_access_token(username=self.config['user'], password=self.config['password'])
            self.tokens = response
            self.session_starttime = current_time

        access_token = self.tokens["access_token"]
        # resfresh_token = response["refresh_token"]

        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {access_token}"

        return session

    def select_product(self, api_url, platform_name, start_date, end_date, product_type, cloud_coverage_max,
                       out_crs='epsg:4326', bbox_aoi=None, tile_id=None):
        if tile_id is None:
            self.select_product_by_aoi( api_url, platform_name, start_date, end_date, product_type, cloud_coverage_max,
                                  bbox_aoi, out_crs)
        if bbox_aoi is None:
            self.select_product_by_tile(
                               api_url,
                               platform_name,
                               start_date,
                               end_date,
                               product_type,
                               tile_id,
                               cloud_coverage_max,
                               out_crs)

    def select_product_by_aoi(self, api_url,platform_name, start_date, end_date, product_type, out_crs, cloud_coverage_max, bbox_aoi):
        search_query = f"{api_url}/Products?$filter=Collection/Name eq '{platform_name}'" \
                       f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{product_type}')" \
                       f" and ContentDate/Start gt {str(start_date)} and ContentDate/Start lt {str(end_date)}" \
                        f" and OData.CSC.Intersects(area=geography'SRID=4326;{bbox_aoi}')" \
                       f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {float(cloud_coverage_max)})" \
                       f"&$expand=Attributes"
        response = requests.get(search_query).json()
        df = pd.DataFrame.from_dict(response['value'])
        try:
            # Unpack Attributes
            att = df['Attributes'].values
            attrs = []
            for item in att:
                attrs.append([{i['Name']: i['Value']} for i in item])

            df0 = pd.DataFrame([{k: v for d in i for k, v in d.items()} for i in attrs])
            df1 = pd.concat([df, df0], axis=1)
            self.products = df1.drop(['Attributes'], axis=1)

        except Exception:
            self.products = pd.DataFrame()
            log.info(f"No products to download. Please change dates or Cloud coverage max level")

    def select_product_by_tile(self,
                               api_url,
                               platform_name,
                               start_date,
                               end_date,
                               product_type,
                               tile_id,
                               cloud_coverage_max,
                               out_crs='epsg:4326'):
        # NOTE: to add selection by geometry add the following line -
        # Read attributes
        search_query = f"{api_url}/Products?$filter=Collection/Name eq '{platform_name}'" \
                       f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{product_type}')" \
                       f" and ContentDate/Start gt {str(start_date)} and ContentDate/Start lt {str(end_date)}" \
                       f" and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'tileId' and att/OData.CSC.StringAttribute/Value eq '{str(tile_id)}')" \
                       f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {float(cloud_coverage_max)})" \
                       f"&$expand=Attributes"

        response = requests.get(search_query).json()
        df = pd.DataFrame.from_dict(response['value'])

        try:
            # Unpack Attributes
            att = df['Attributes'].values
            attrs = []
            for item in att:
                attrs.append([{i['Name']: i['Value']} for i in item])

            df0 = pd.DataFrame([{k: v for d in i for k, v in d.items()} for i in attrs])
            df1 = pd.concat([df, df0], axis=1)
            self.products = df1.drop(['Attributes'], axis=1)

        except Exception:
            self.products = pd.DataFrame()
            log.info(f"No products to download. Please change dates or Cloud coverage level: {tile_id}")

        if not self.products.empty:
            products = gpd.GeoDataFrame(self.products, crs=out_crs,
                                        geometry=[shape(geo) for geo in self.products['GeoFootprint']])

            aoi = gpd.GeoDataFrame(geometry=[self.bbox_aoi], crs=out_crs)
            aoi['area_aoi'] = aoi.area

            # Intersection of product and polygon of AOI
            gdf_joined = gpd.overlay(df1=products, df2=aoi, how='union')
            gdf_joined['area_joined'] = gdf_joined.area
            gdf_joined['area_ratio'] = gdf_joined['area_joined'] / gdf_joined['area_aoi']
            results = gdf_joined.groupby(['Id']).agg({'area_ratio': 'sum'}).reset_index()
            results = results.sort_values('area_ratio', ascending=False)
            results = products[products['Id'] == results['Id'].iloc[0]]
            results = results.sort_values('OriginDate', ascending=False)

            # Select the first row
            try:
                self.product = {'uuid': results['Id'].iloc[0],
                                'name': results["Name"].iloc[0],
                                's3path': results["S3Path"].iloc[0],
                                'tile': results['tileId'].iloc[0],
                                'product_date': results['OriginDate'].iloc[0][:10],
                                'cloudcoverage': results['cloudCover'].iloc[0],
                                'bands': self.bands,
                                'num_bands': len(self.bands),
                                'orbit': results['relativeOrbitNumber'].iloc[0],
                                'geom': results["geometry"].iloc[0].wkt
                                }
            except Exception:
                self.product = {}
        else:
            self.product = {}


    def get_access_token(self, username: str, password: str) -> str:
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        try:
            r = requests.post("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                              data=data,
                              )
            r.raise_for_status()
        except Exception:
            log.exception(
                f"Access token creation failed. Response from the server was: {r.json()}"
            )
        return r.json()

