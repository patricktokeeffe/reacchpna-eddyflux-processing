# -*- coding: utf-8 -*-
"""Paths to important local directories

    Conventions:
        - don't use trailing slash
        - prefix strings with 'r' to avoid escaping slashes
"""

import os.path as osp

from version import version as __version__

#### Valid path substitions
# These are the canonical available path substitutions. Not every script will
# use all of them--the only exception may be `TOWERDATA`
#
# Dict-style named substitutions:
#
#   >>>> code = 'CFNT'
#   >>>> RAW_ASCII % {'site' : code}
#   D:\proj\2011_REACCH\tower_CFNT\L0_raw_ascii
#
# %(site)s      Monitoring site 4-char representation (CFNT, LIND, MSLK, ...)
# %(table)s     Full name of data table associated with file
#               ("tsdata", "site_info", "stats30", ...)


# Base directory for project
HOME = r'B:\proj\2011_REACCH' # for lar-d216-share.cee.wsu.edu

# Path to recently downloaded telemetry files
TELEMETRY_SRC = r'C:\Campbellsci\Loggernet' # for lar-d216-share.cee.wsu.edu

#### Agweathernet data storage location
AGWEATHERNET = osp.join(HOME, 'agweathernet_data')

# Destination directory mask for monitoring towers
TOWERDATA = osp.join(HOME, r'tower_%(site)s')

# Monitoring tower data files
RAW_BINARY = osp.join(TOWERDATA, r'L0_raw_binary') # copies from cards
RAW_ASCII = osp.join(TOWERDATA, r'L0_raw_ascii') # plain-text conversions
RAW_STDFMT = osp.join(TOWERDATA, r'L0_standard_format') # after standardizing
TELEMETRY = osp.join(TOWERDATA, r'L0_telemetry') # standardized telemetry data

# Monitoring tower timelapse cameras
TIMELAPSE_PHOTOS = osp.join(TOWERDATA, 'photos_timelapsecam') # captured images
TIMELAPSE_VIDEOS = osp.join(TOWERDATA, 'videos_timelapse') # produced videos

# Script log files
LOGDIR = osp.join(HOME, r'scripts\logs')
TELEMETRY_LOG = osp.join(LOGDIR, 'process_new_telemetry_data.log')

