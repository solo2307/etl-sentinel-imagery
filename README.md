# ETL pipeline of Sentinel Imagery

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT) [![status: experimental](https://github.com/GIScience/badges/raw/master/status/experimental.svg)](https://github.com/GIScience/badges#experimental)
## Overview

Within this repository, you'll find an ETL (Extract, Transform, Load) pipeline designed for the retrieval of Sentinel imagery. 
The pipeline seamlessly processes Sentinel data, executing essential transformations.
Leveraging the ODATA protocol, it efficiently downloads Sentinel images from the `dataspace.copernicus.eu` API.

## Installation
1. Clone the repository:

```bash
git clone https://github.com/solo2307/etl-sentinel-imagery.git
cd etl-sentinel-imagery
```
2. Create mamba environemt and install requierments from _yaml_ file
```bash
mamba env create -f environment.yaml
```
**Note**: To install mamba please visit  -  https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html
## Configuration
1. Create and add credentials to `.env` file. _CopernicusHub_ username and password.Please create an account in https://dataspace.copernicus.eu/:
   + COPERNICUSHUB_API_URL=https://catalogue.dataspace.copernicus.eu/odata/v1
   + COPERNICUSHUB_API_ID=your_user_name
   + COPERNICUSHUB_API_SECRET=your_user_password
2.  Configure Area of Interest to retrieve from Copernicus in `conf/config.yaml`. 
Furthermore, for Sentinel-2, there is an option to retrieve data based on the tile_id.
```yaml
# AOI acquisition and preprocessing descriptor parameters.
data:
  aoi: data/aoi.geojson
  crs: epsg:4326
  global_dataset: data/area/s2_tiles.csv
  tile_ids: []
```
3. Set up credentials for the Copernicus Data Space Catalogue and configure image acquisitions.
```yaml
# CopernicusHub credentials and Sentinel imagery configurations
imagery:
  # CopernicusHub API credentials
  api_url: ${oc.env:COPERNICUSHUB_API_URL}
  api_id: ${oc.env:COPERNICUSHUB_API_ID}
  api_secret: ${oc.env:COPERNICUSHUB_API_SECRET}

  # Image acquisition and preprocessing descriptor name.
  start_date: 2023-05-01
  end_date: 2023-09-05
  platform_name: SENTINEL-2
  processing_level: Level-2A
  product_type: S2MSI2A
  cloud_coverage_max: 4.0
  resolution: 10.0
  bands: [ 'B02', 'B03', 'B04', 'B08' ]
  format: UINT8
```
4. Configure a local storage.
```yaml
cache:
  feature_dir: cache/s2
```

## Limitations and Quotas for General Users*
1. Monthly transfer limit = 6Tb;
2. Max number of activate sessions = 100;
3. Number of concurrent connections limit = 4;
4. A token stays active for 10 min;
* https://documentation.dataspace.copernicus.eu/Quotas.html