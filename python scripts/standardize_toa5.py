# -*- coding: utf-8 -*-
"""TOA5 file homogenizer

- Columns appear in different tables (files) at different times so it must
    evaluate across files and/or be aware of missing columns and able to
    properly assemble them

- Columns might be merged across intervals as well: sonic_azimuth being moved
    from site_daily to site_info for example!

- Shouldn't the los gatos columns be rounded up into their own data files?

Created on Wed Mar 06 08:47:59 2013

@author: pokeeffe
"""

import os
import sys
import logging as log

from csv import QUOTE_NONE
from datetime import datetime as dt
from glob import glob
from argparse import ArgumentParser

from pandas import read_csv, Series, DataFrame, DatetimeIndex, infer_freq
from pandas.tseries.offsets import Second, Day
from pandas.tseries.frequencies import to_offset

from definitions.sites import site_list
from definitions.fileio import (get_table_name, get_site_code,
                                HeaderMismatchError)
from definitions.tables import (current_names, table_definitions,
                                table_baleinfo, historical_table_names,
                                ColumnNotFoundError)
from version import version as __version__


def standardize_toa5(fname, dest_path=None, baled=True):
    """Re-write TOA5 file in standard format

    Opens eddy covariance tower data files from REACCH (2011-2016) project
    in TOA5 format and re-write to CSV file using standard format presecribed
    in `definitions.py`. Write output file(s) to directory specified by
    `dest_path` or to current directory if `dest_path=None` (default). See
    Details below.

    Parameters
    ----------
    fname : file-like
        Path to data file
    dest_path : str or None, optional
        Path to directory output file(s) will be saved or current directory
        if None. Defaults to None
    baled : bool, optional
        If true, output files will be broken up into files of consistent
        length of time; otherwise, output files will be be cumulative.
        Defaults to True

    Returns
    -------
    Nothing is returned.

    Details
    -------
    The standardization process involves:

    - interpreting null values ("NAN") to more recognizable form (NAN)
    - translating table & column names to current definitions
    - dropping columns which are no longer recorded
    - back-filling added columns with null values
    - padding with null rows to create monotonic increasing timestamps
    - if `baled=True`, padding with null rows to consistent length of time

    The translation process preserves the integrity of the data set by
    interpreting values as text. This affects performance minimally since
    only timestamps, not data values, need to be evaluated and it avoids
    introducing discrepancies caused by floating-point conversions.

    One or several output files may be written depending on the current
    table definitions. If `baled=True` (default), then output files will be
    a consistent length of time and file names will be generated using the
    monitoring site acronym and starting timestamp of data within each output
    file. (The length of each baled output file is in pre-determined for
    each data table; see dict `table_baleinfo` in the file`definitions.py`
    for more information.) If `baled=False` then only one file is output
    per table and no date is included in the file name.

        **Note: if a file of the target output filename already exists, then
        the existing file is loaded and the output file is appended to it.
        If the existing file has a different header then the output file will
        be skipped (not written). Non-NAN values in the existing file will
        take precedence over values in the appended file.**

    Output files are written to a temporary file first, then renamed to help
    prevent existing data files from being corrupted by aborted routines.

    The parameter `dest_path` may be a relative or absolute path. There are
    three available substitutions, which may be used multiple times. These
    substitutions are the same as used when generating the output file name.

    +-----------+----------------------------------------------------------+
    |           | ... is replaced with:                                    |
    +-----------+----------------------------------------------------------+
    | %(site)s  | four-character monitoring site acronym (e.g. CFNT, CFCT, |
    |           | LIND, ...)                                               |
    +-----------+----------------------------------------------------------+
    | %(table)s | table's name (e.g. tsdata, stats5, site_info, ...)       |
    +-----------+----------------------------------------------------------+
    | %(date)s  | date of first timestamp in file                          |
    +-----------+----------------------------------------------------------+

    Example: `dest_path = r'output\%(site)s\%(table)s'` causes output files
    to be placed in a folder named "output" in the current working directory.
    Files are further segregated into folders named after the relevant
    monitoring site and subfolders named after the file's data table name.

    """
    _homogenize(fname, dest_path=dest_path, baled=baled)


def _safe_open_toa5(fname):
    """Opens CSI TOA5-formatted data files preserving data exactly

    Load data from TOA5-formatted data file into pandas.DataFrame object.
    Values are loaded as strings and preserved exactly for output.
    Timestamp column is used as the dataframe index (axis 0). Column names
    become names along DF axis 1. Instances of "NAN" are set to `np.nan`."""
    df = read_csv(fname,
                  header=1,
                  skiprows=[2,3],
                  index_col=0,
                  parse_dates=True,
                  #na_values=['"NAN"'],
                  na_values=['NAN', 7999, -7999, 65535, 2147483647, -2147483648],
                  keep_default_na=False,
                  dtype=str)

    dups = len(df.index.get_duplicates())
    if dups:
        df = df.groupby(level=0).last()
        log.warning('Removed %d duplicate indices (%s)' % (dups, fname))

    if not df.index.is_monotonic:
        df.sort_index(inplace=True)
        log.warning('Sorted non-monotonic timestamps (%s)' % fname)

    if len(df[:'20110818']) or len(df['20161231':]):
        # XXX should this be aware of TODAY's date too?
        df = df['20110818':'20161231']
        log.warning(('Detected and removed data from outside duration of REACCH '
              'study duration (before Aug 18, 2011 or after Dec 31, 2016) '
              '(%s)') % fname)

    df.index.name = 'TIMESTAMP'
    return df


def _safe_read_csv(file_name):
    """Read DataFrame previously written to CSV file in standard format"""
    df = read_csv(file_name,
                  index_col=0,
                  parse_dates=True,
                  na_values=['NAN'],
                  keep_default_na=False,
                  quoting=QUOTE_NONE,
                  dtype=str)
    df.index.name = 'TIMESTAMP'
#    freq = infer_freq(df.index, warn=False)
#    if freq is None:
#        log.warning('safely read CSV file had indeterminate frequency')
#        raise Exception
    return df


def _safe_write_csv(df, file_name):
    """Write DataFrame to CSV file in standard format"""
    der = os.path.dirname(file_name)
    if der:
        # this illogically logical try-except block brought to you by:
        #    http://stackoverflow.com/a/14364249
        try:
            os.makedirs(der)
        except OSError:
            if not os.path.isdir(der):
                raise

    # express timestamps as string to achieve consistent formatting
    df_freq = to_offset(df.index.inferred_freq)
    if df_freq is None:    #include 2 decimal places of subseconds
        str_fmt = lambda x: dt.strftime(x, '%Y-%m-%d %H:%M:%S.%f')[:22]
    elif df_freq < Second():    #only 1 decimal place of subseconds
        str_fmt = lambda x: dt.strftime(x, '%Y-%m-%d %H:%M:%S.%f')[:21]
    else:
        str_fmt = lambda x: dt.strftime(x, '%Y-%m-%d %H:%M:%S')

    df.index.name = 'TIMESTAMP' # <-- BIG HAMMER SOLUTION
    df = df.reset_index()
    df['TIMESTAMP'] = df['TIMESTAMP'].apply(str_fmt)
    df.set_index('TIMESTAMP', inplace=True)
    df.to_csv(file_name,
               na_rep='NAN',
               quoting=QUOTE_NONE, # since treating all values as strings be
                                   # explicit about no quoting
               quotechar="'", # specify alternate quote to avoid triggering
                              # QUOTE_NONE/escapechar errors when writing
                              # fields with double-quotes (CompileResults and
                              # CardStatus columns)
               index_label='TIMESTAMP')


def _standardize_df(rawdf, was_tblname):
    """Return data conformed to current definitions

    Parameters
    ----------
    rawdf : pandas.DataFrame
        data from reacch TOA5 file with original column names intact
    tblname : str
        name of data table as specified in associated file header

    Returns
    -------
    Dict with current table names as key and pandas.DataFrame\ s, with
    current column names, as values.
    """
    newdfs = {}
    for was_colname in rawdf.columns:
        try:
            is_tblname, is_colname = current_names(was_tblname, was_colname)
        except ColumnNotFoundError:
            __msg(' * Cannot find alias for column "%s" of table "%s" \n'
                    % (was_colname, was_tblname))
            continue
        if is_colname is None:
            __msg(' - Discarding dropped column: {col} ({tbl})\n'.format(
                col=was_colname, tbl=was_tblname))
            continue
        if is_tblname is None:
            __msg(' - Discarding dropped table: {t}\n'.format(t=was_tblname))
            continue
        tbl = newdfs.setdefault(is_tblname, {})
        tbl[is_colname] = Series(rawdf[was_colname])
    outdfs = {}
    for tbl in newdfs:
        try:
            colnames = table_definitions[tbl][1:] #omit TIMESTAMP b/c it's Index
        except KeyError:
            msg = ' * No table definition found for "{n}" ... skipping.\n'
            __msg(msg.format(n=tbl))
            continue
        outdf = DataFrame(newdfs[tbl], columns=colnames)
        outdf.index.name = 'TIMESTAMP'

        ##### 'site_info' table is single special case of having 2 text
        ##### columns so preserve double-quotes around legitimate strings
        if tbl == 'site_info':
            quote = lambda x: '"{s}"'.format(s=x.strip())
            outdf['CompileResults'] = outdf['CompileResults'].apply(quote)
            outdf['CardStatus'] = outdf['CardStatus'].apply(quote)
        #####

        outdfs[tbl] = outdf
    return outdfs


def _prep_df(std_df, tbl_name, baled):
    """Return list of write-ready, baled dataframes with padded indices

    Parameters
    ----------
    std_df : pandas.DataFrame
        standardized data table to be broken up into bales according to time
    tbl_name : string
        current name of data table
    baled : boolean
        if True, break up certain larger tables into monthly or daily files

    Returns
    -------
    List of dataframes
    """
    dflist = []
    try:
        grpbykeys, start_func, offset, freq = table_baleinfo[tbl_name]
    except KeyError:
        __msg(' ! no baling info for table "{n}". Skipping.\n'.format(n=tbl_name))
        return dflist

    __msg('   Preparing table "{n}"\n'.format(n=tbl_name))

    if not baled or grpbykeys is None:
        dflist = [std_df]
    else:
        groups = std_df.groupby(grpbykeys)
        for grp, df in groups:
            start = start_func(df)
            tstamps = DatetimeIndex(start=start, end=start+offset, freq=freq)
            df.index.name = std_df.index.name
            dflist.append(df.reindex(tstamps[:-1]))
    return dflist


def _merge_with_existing(to_merge, existing, tbl_name):
    """Combine dataframe w/ existing data read from file, w/ error checking"""
    if not to_merge.columns.equals(existing.columns):
        __msg('    % could not merge tables: column headers differ\n')
        raise HeaderMismatchError
    try:
        a, start_func, offset, freq = table_baleinfo[tbl_name]
    except:
        __msg('    ! no baling info for table "{n}"\n'.format(n=tbl_name))
        raise Exception # TODO find better recovery option
    if freq is not None:
        sfreq = infer_freq(existing.index[:3])
        efreq = infer_freq(existing.index[-3:])
        if sfreq != freq or efreq != freq:
            __msg('    ! inconsistent freq [%s / %s, %s] resetting indices\n'
                    % (freq, sfreq, efreq))
            existing = existing.asfreq(freq, method=None)
        combined = existing.combine_first(to_merge).asfreq(freq)
    else:
        combined = existing.combine_first(to_merge)
    return combined


def _make_out_fname(df, site_code, dest_path, tbl_name, baled):
    """Make file names for output files using standard formula"""
    if site_code not in [site.code for site in site_list]:
        raise Exception('Nonexistant site code referenced: {n}'.format(n=site_code))
    if tbl_name not in table_definitions.keys():
        raise Exception('Nonexistant table referenced: {n}'.format(n=tbl_name))
    try:
        grpbykeys, offset, balesize, freq = table_baleinfo[tbl_name]
    except KeyError:
        raise Exception('Could not look up table baling size!!!!!')

    fname = '%(site)s_%(table)s'
    start = ''
    if baled and grpbykeys is not None: # include timestamp if not cumulative
        fname = fname+'_%(date)s'
        start = df.index[0].isoformat().split('T')[0]
    if baled and balesize == Day(): # only include hour/min for daily files
        fname = fname+'_0000'
    fname = fname+'.dat'
    # XXX this would choke if invalid path was provided:
    fname = os.path.join(dest_path, fname) if dest_path else fname

    return fname % {'site':site_code, 'table':tbl_name, 'date':start}


def _homogenize(fname, dest_path=None, baled=True):
    """The actual legwork of standardizing a raw data file"""
    __msg('   Checking file format ... ')
    site_code = get_site_code(fname)
    was_tblname = get_table_name(fname)
    if not was_tblname:
        __msg('invalid file format. Skipping file.\n')
        return
    elif was_tblname not in historical_table_names:
        __msg('unrecognized table: {n}. Skipping file.\n'.format(n=was_tblname))
        return
    else:
        __msg('table "{n}" from {s} site.\n'.format(n=was_tblname, s=site_code))

    __msg('   Reading file ... ')
    try:
        rawdf = _safe_open_toa5(fname)
    except:
        __msg('error occurred during read. Skipping file.')
        return
    __msg('read {n} rows\n'.format(n=len(rawdf)))

    __msg('   Applying standard format ... \n')
    stdfs = _standardize_df(rawdf, was_tblname)

    for newname, newtbl in stdfs.iteritems():
        tables = _prep_df(newtbl, newname, baled)
        if not tables:
            __msg(' * no output for table "{n}"\n'.format(n=newname))
            continue
        for table in tables:
            if not len(table):
                __msg(' * no data in "{n}" \n'.format(n=newname))
                continue
            outpath = _make_out_fname(table, site_code, dest_path, newname, baled)
            typ = 'Writing'
            if os.path.isfile(outpath):
                try:
                    existing = _safe_read_csv(outpath)
                    table = _merge_with_existing(table, existing, newname)
                except HeaderMismatchError:
                    __msg((' % existing file has different header - unable to'
                           'merge! Skipping {f}\n').format(f=outpath))
                    continue # TODO write output file to different name instead
                typ = 'Appending'
            __msg('   {a} to {f} \n'.format(a=typ, f=outpath))
            tempname = outpath+"~0"
            _safe_write_csv(table, tempname)
            if os.path.isfile(outpath):
                try:
                    os.remove(outpath)
                except WindowsError:
                    __msg(' * unable to delete existing file (%s)\n' % outpath)
                    outpath = outpath+'.new'
            try:
                os.rename(tempname, outpath)
            except WindowsError:
                __msg(' ! unable to rename to destination (%s)\n' % outpath)


def __msg(msg):
    """handles verbosity; semi-magic because it touches argparser results"""
    # TODO replace this with logging
    if __name__ == '__main__': # don't use in modules
        if args.verbose:
            sys.stdout.write(str(msg))


def __get_filelist():
    """semi-magic function uses argparser results to search for files"""
    flist = []
    searchdir = os.getcwd() if args.dir is None else args.dir
    if args.toa5file:
        flist.extend(args.toa5file)
    if os.path.isdir(searchdir):
        filt = args.infilt if args.infilt else '*'
        found = glob(os.path.join(searchdir, filt))
        flist.extend([f for f in found if os.path.isfile(f)])
    if args.exfilt:
        for test in args.exfilt:
            flist[:] = [f for f in flist if test not in f]
    return flist


def __show_settings():
    """semi-magic function displays settings, uses argparser results"""
    print 'Settings\n--------'
    print 'Verbose output: {v}'.format(v=('On' if args.verbose else 'Off'))
    print 'Working directory: {d}'.format(d=os.getcwd())
    print 'Search filter: {f}'.format(f=args.infilt)
    print 'Exclusion filter: {f}'.format(f=args.exfilt)
    print 'Search directory: ',
    if args.dir is not None:
        if os.path.isdir(args.dir):
            print args.dir
        else:
            print '<cannot use invalid path>\n\t({d})>'.format(d=args.dir)
    else:
        print '<using working directory>'
    print 'Output directory: ',
    if args.out is not None:
        if os.path.isdir(args.out):
            print args.out
        else:
            print '<TO BE MADE> {d}'.format(d=args.out)
    else:
        print '<not specified; using working directory>'
    if args.nobale:
        print 'Time-based baling: disabled'
    else:
        print 'Time-based baling: enabled'


def __show_filelist(listall=''):
    """semi-magic function uses global vars to print list of files"""
    if flist:
        num = len(flist)
        if num > 10:
            while listall not in ['Y', 'N']:
                msg = 'List contains {n} files. List all? (Y/N)'.format(n=num)
                listall = raw_input(msg)
                listall = listall.strip().upper()
            print
        print 'File(s) to process ({n})\n------------------'.format(n=num)
        if num > 10 and listall in 'N':
            for f in flist[:5]:
                print '    '+os.path.basename(f)
            print '      ... ({n} not shown)'.format(n=len(flist)-10)
            for f in flist[-5:]:
                print '    '+os.path.basename(f)
        else:
            for f in flist:
                print '    '+os.path.basename(f)
    else:
        print '<File processing list is empty>'


_splashscreen = """\
----------------------------------------------------------------------------
|  EC tower raw data file homogenizer [interactive mode]                   |
|                                                                          |
|  Regional Approaches to Climate Change Objective 2                       |
|  Laboratory for Atmospheric Research                                     |
|  Washington State University                                             |
----------------------------------------------------------------------------
"""

_menu = """\
Available options
-----------------
  [1] Change working directory        [M] Display this menu
  [2] Change search directory         [V] {v} verbose output
  [3] Change output directory         [R] Review current settings
  [4] Change search filter            [F] View file list
  [5] Change exclusion filters        [C] Clear file list
  [6] {b} time-baled output {s}      [S] Search & build file list

  [Q] Quit program (or use Ctrl-C)    [B] Begin processing """
def __show_menu():
    """semi-magic function prints menu, relies on argparser results"""
    print _menu.format(v='Disable' if args.verbose else 'Enable',
                       b='Enable' if args.nobale else 'Disable',
                       s=' ' if args.nobale else '')

_descrip = """\
This program reads data files, in TOA5 format, made by dataloggers at REACCH
Objective 2 monitoring sites and re-writes them in a standard format:

    1) Translates historical table/column names to current definitions
    2) Replaces known null values ("NAN") with (unquoted) 'NAN'
    3) Pads timeseries with null records (unquoted NANs) to achieve
       consistent record spacing in output files.
    4) Reduces headers to a single row (the first) representing column
       names. Extra rows containing program information, column units, etc.
       are removed.
    5) Writes output files with a consistent naming scheme based on site,
       table name and starting timestamp of data file.

Please note, however, this program will NOT:

    1) Correct known errors in data files such as
        a)  incorrect sensor calibration factor, anemometer azimuth or other
            static parameter
        b)  timestamp shifts due to clock resets
"""


if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('toa5file', nargs='*',
                   help='file(s) to process')
    p.add_argument('-i', '--interactive', action='store_true',
                   help='interactive mode')
    p.add_argument('-d', '--dir', nargs='?',
                   help='directory to search, full or rel. path')
    p.add_argument('-o', '--out', nargs='?',
                   help=('output directory, full or rel. path; can use '
                         '"%(site)s", "%(table)s" and "%(date)s" to get site '
                         '4-char code, data table name, or starting date of '
                         'data table in output path'))
    p.add_argument('-v', '--verbose', action='store_true',
                   help='verbose output')
    p.add_argument('--nobale', action='store_true',
                   help=('output all files cumulatively instead of breaking '
                         'larger files up into monthly or daily blocks'))
    p.add_argument('--infilt', nargs='?',
                   help='restrict to files matching this inclusion filter')
    p.add_argument('--exfilt', nargs='*',
                   help=('remove file names matching this exclusion filter; '
                         'this argument must not be last or it will consume '
                         'any provided file names'))
    args = p.parse_args()

    flist = __get_filelist()

    if not args.interactive:
        if not args.dir and not args.toa5file:
            msg = ('EC tower raw data file homogenizer: No file(s) or search '
                   'directory specified.\n\nWould you like to enter '
                   'interactive mode? [Y]es else no: ')
            ans = raw_input(msg)
            if ans.strip().upper() in ['Y']:
                args.interactive = True
            else:
                sys.exit(0) ## exit program
        if args.dir:
            # TODO
            # 1. ensure directory exists
            # 2. if so, search for files given cmdline provided filters
            # 3. generate list of files
            # 4. continue, skip interactive mode
            print '** Using directory argument:', args.dir
        if args.toa5file:
            # TODO
            # 1. ensure each file on list exists; remove if doesn't
            # 2. apply infilt/exfilt if provided
            # 3. continue, skip interactive mode
            print '** Using file list arg:', [ea for ea in args.toa5file]

    if args.interactive:
        print _splashscreen
        print _descrip
        __show_settings()
        print
        __show_filelist(listall='N')
        print
        __show_menu()
        skip_prompt = False
        while 1: # Menu loop
            if not skip_prompt:
                mc = raw_input('\nSelection: ').strip()
            skip_prompt = False
            if not mc.isdigit():
                mc = mc.upper()
                if mc == 'B':
                    if flist:
                        # exit interactive mode
                        break
                    else:
                        print ' ? No source files; Did you perform search [S]?'
                elif   mc == 'Q':
                    sys.exit(0)
                elif mc == 'M':
                    __show_menu()
                elif mc == 'V':
                    args.verbose = not args.verbose
                    if args.verbose:
                        print ' + Verbose output on'
                    else:
                        print ' - Verbose output off'
                elif mc == 'R':
                    __show_settings()
                elif mc == 'F':
                    __show_filelist()
                elif mc == 'C':
                    if args.toa5file:
                        sure = raw_input('Warning: some file names were '
                            'specified on the command line. They will\n'
                            'be removed too. It is not trivial to add them '
                            'again without restarting \nthe program. Enter '
                            '[Y] to continue, anything else to cancel: ')
                        if not sure.upper() == 'Y':
                            print ' . File list unchanged.'
                            continue
                    args.toa5file = []
                    flist = []
                    print ' % Cleared file list.'
                elif mc == 'S':
                    flist = __get_filelist()
                    if flist:
                        print ' + Found {n} files.'.format(n=str(len(flist)))
                    else:
                        print ' - No files found.'
                else:
                    print " * Invalid input; try 'M' for menu"
                continue
            mc = int(mc) # wait to avoid converting str->char
            if mc < 1 or mc > 6:
                print ' * Selection out of range'
                continue

            print
            if mc == 1: # change CWD
                print 'CWD: {d}'.format(d=os.getcwd())
                cwd = raw_input('>>> New working directory: ')
                if not cwd or cwd.isspace():
                    print ' . Working directory unchanged.'
                    continue
                try:
                    os.chdir(cwd)
                    print ' + Switched. CWD: {d}'.format(d=os.getcwd())
                    if not args.dir:
                        ans = ''
                        while ans not in ['Y', 'y', 'N', 'n']:
                            ans = raw_input('Re-search now? Y/N: ').strip()
                            if ans.upper() == 'Y':
                                mc = 'S'
                                skip_prompt = True
                except OSError:
                    print (' * Unable to switch to directory (OSError)\n'
                           '   Does directory exist?')

            if mc == 2: # change search dir
                print 'Seach directory: {d}'.format(d=args.dir)
                print ('Relative paths are relative to working directory. '
                       'Enter <space> to clear or nothing to abort.')
                nsd = raw_input('>>> New search directory: ')
                if not nsd:
                    print ' . Search directory unchanged.'
                    continue
                if nsd.isspace():
                    args.dir = None
                    print (' * Search directory cleared, using '
                           'current working directory.')
                    continue
                if os.path.isdir(nsd):
                    args.dir = nsd
                    print ' + Changed. Search in: {d}'.format(d=args.dir)
                    ans = ''
                    while ans not in ['Y', 'y', 'N', 'n']:
                        ans = raw_input('Re-search now? Y/N: ').strip()
                        if ans.upper() == 'Y':
                            mc = 'S'
                            skip_prompt = True
                else:
                    print ' * Unable to locate directory'

            if mc == 3: # change output dir
                print 'Output directory: {d}'.format(d=args.out)
                print ('Relative paths are relative to working directory. '
                       'Use "%(site)s", \n"%(table)s" or "%(date)s" to access '
                       '4-character site code, data table name, \nor starting '
                       'date of table in output path. \nEnter <space> to '
                       'clear or nothing to abort.')
                nod = raw_input('>>> New output directory: ')
                if not nod:
                    print ' . Output directory unchanged.'
                    continue
                if nod.isspace():
                    args.out = None
                    print (' * Output directory cleared, using '
                           'current working directory.')
                    continue
                if not os.path.isdir(nod):
                    chk = raw_input(' ! New directory may not exist. '
                                    'Create? [Y]es else no: ')
                if os.path.isdir(nod) or chk.upper() == 'Y':
                    args.out = nod
                    print ' + Changed. Output directory: {d}'.format(d=args.out)
                else:
                    print ' * Unable to switch to new output dir'

            if mc == 4: # change search filter
                if args.infilt:
                    print 'Existing: {f}'.format(f=args.infilt)
                else:
                    print 'Existing: <no search filter is defined>'
                print ('Provide glob-compatible filter. File names which '
                        'do not match will be \nomitted from search results. '
                        'Enter <space> to clear or nothing to abort.')
                sf = raw_input('>>> New search filter: ')
                if not sf:
                    print ' . Search filter unchanged.'
                    continue
                if sf.isspace():
                    args.infilt = None
                    print ' * Cleared search filter.'
                    continue
                args.infilt = sf
                print ' * Changed search filter.'
                ans = ''
                while ans not in ['Y', 'y', 'N', 'n']:
                    ans = raw_input('Re-search now? Y/N: ').strip()
                    if ans.upper() == 'Y':
                        mc = 'S'
                        skip_prompt = True

            if mc == 5: # change exclusion filter
                if args.exfilt:
                    print 'Existing: {f}'.format(f=', '.join(args.exfilt))
                else:
                    print 'Existing: <no exclusion filters defined>'
                print ('Provide comma-separated list of fnmatch-compatible '
                        'filters. File names \nmatching these filters will '
                        'not be processed. Enter <space> to clear \nlist or '
                        'nothing to abort.')
                ef = raw_input('>>> New filter list: ')
                if not ef:
                    print ' . Exclusion filters unchanged.'
                    continue
                if ef.isspace():
                    args.exfilt = None
                    print ' * Cleared exclusion filter set.'
                    continue
                args.exfilt = [s.strip() for s in ef.split(',')]
                print ' * Changed exclusion filter set.'
                ans = ''
                while ans not in ['Y', 'y', 'N', 'n']:
                    ans = raw_input('Re-search now? Y/N: ').strip()
                    if ans.upper() == 'Y':
                        mc = 'S'
                        skip_prompt = True

            if mc == 6: # enable/disable file baling
                args.nobale = not args.nobale
                if args.nobale:
                    print ' - Time-based file baling disabled.'
                else:
                    print ' + Time-based file baling enabled.'

        ## end of interactive mode

    start = dt.now()
    total = len(flist)
    for num, fname in enumerate(flist):
        # XXX hack: pull message out of function in order to provide status
        #   update: e.g. [1/921]
        # possibly fix this cruft using logging
        __msg('\nStandardizing {n} ... [{x}/{of}]\n'.format(n=fname,
                                                            x=(num+1),
                                                            of=total))
        if args.nobale:
            _homogenize(fname, dest_path=args.out, baled=False)
        else:
            _homogenize(fname, dest_path=args.out)
    duration = dt.now() - start
    print ('\nStarted at %s \nFinished at %s (duration %s)' %
            (str(start)[:-7], str(dt.now())[:-7], str(duration)))



