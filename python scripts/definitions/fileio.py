# -*- coding: utf-8 -*-
"""Mindful file access



@author: Patrick O'Keeffe <pokeeffe@wsu.edu>
"""

import os

from pandas import read_csv
from warnings import warn

from sites import sn2code


MAX_RAW_FILE_SIZE = 200 * 1024 * 1024  #split raw data files > this, bytes


class HeaderMismatchError(Exception): pass


def get_table_name(toa5_file):
    """Return name of table given rel. or abs. file path to TOA5 file

    Reads header of Campbell Scientific long-header (TOA5) formatted data
    files and returns name of data table.

    Parameters
    ----------
    toa5_file : file-like object
        Source data file in CSI long-header (TOA5) format

    Returns
    -------
    str : name of data table or None if not a valid table file
    """
    with open(toa5_file, mode='r') as f:
        l = f.readline().strip().split(',')
    try:
        assert l[0] == '"TOA5"' # 1st item, 1st row must be "TOA5"
        tblname = l[-1].strip('"') #last item, first row
    except:
        tblname = None
    return tblname


def get_site_code(toa5_file):
    """Return text code of site where TOA5 data file was created

    Reads header of Campbell Scientific long-header (TOA5) formatted data
    files and returns four-character site code (e.g. CFNT, LIND, MMTN, etc)

    Parameters
    ----------
    toa5_file : file-like object
        Source data file in CSI long-header (TOA5) format

    Returns
    -------
    str : four character site code or None if not a valid table file
    """
    with open(toa5_file, mode='r') as f:
        l = f.readline().strip()
    sn = l.split(',')[3].strip('"') #fourth item, first row
    try:
        sitecode = sn2code[sn]
    except KeyError:
        sitecode = None
    return sitecode


def open_toa5(fname):
    """Opens CSI TOA5-formatted data files in standard fashion

    Loads data from TOA5-formatted data file into pandas.DataFrame object. The
    timestamp column is used as the dataframe index (axis 0). Column names
    are used for names along DF axis 1. Progam info (line 0), column units
    (line 2) and record type (line 3) are ignored. Several null values are
    recognized and set to np.nan ("NAN")

    Other special values which aren't recognized: "INF", "+INF", +INF, 65535,
    7999, -7999, 2147483647, -2147483648, 2.147484e+09, and -2.147484e+09

    See Campbell Scientific, Inc. CR3000 User Manual (rev7/11) for details

    Parameters
    ----------
    fname : str
        path to TOA5 file to open

    Returns
    -------
    pandas.DataFrame

    """
    df = read_csv(fname,
                  header=1,
                  skiprows=[2,3],
                  index_col=0,
                  parse_dates=True,
                  na_values=['"NAN"'],
                  keep_default_na=False)

    if len(df.index.get_duplicates()):
        warn('open_toa5 removed duplicate indices (%s)' % fname)
        df = df.groupby(level=0).last()

    if not df.index.is_monotonic:
        warn('open_toa5 sorted non-monotonic timestamps (%s)' % fname)
        df.sort_index(inplace=True)

    if len(df[:'20110818']) or len(df['20161231':]):
        warn(('open_toa5 detected and removed data from outside duration of '
             'the REACCH field study (before Aug 18, 2011 or after Dec 31, '
             '2016) (%s)') % fname)
        df = df['20110818':'20161231']

    df.index.name = 'TIMESTAMP'
    return df




