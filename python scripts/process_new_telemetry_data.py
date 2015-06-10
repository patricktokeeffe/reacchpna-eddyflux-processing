# -*- coding: utf-8 -*-
"""
Processes most recent data downloaded from remote monitoring sites:

    1)  Check for new data in C:\Campbellsci\Loggernet

        If no data, send warning email else:

    #)  Combine new data on local hard drive with existing files hosted on
        data hard drive
    3)  Remove existing files from download location

Created on Mon Nov 04 17:13:20 2013

@author: pokeeffe
"""

from __future__ import print_function

import logging
import os
import os.path as osp

from glob import glob
from time import sleep
from sys import stdout, exit

from definitions.fileio import get_site_code
from definitions.paths import TELEMETRY_SRC, TELEMETRY, TELEMETRY_LOG
from standardize_toa5 import standardize_toa5
from version import version as __version__

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def process_new_telemetry_data():
    new_files = glob(osp.join(TELEMETRY_SRC, 'REMOTE_*.dat'))
    total = len(new_files)
    logger.info('Preparing to process new telemetry data in %s [%i files]' %
            (TELEMETRY_SRC, total))
    to_remove = []
    for fname in new_files:
        fsize = os.path.getsize(fname) / 1024
        logger.info('Processing %s (%iKB) ... ' % (fname, fsize))
        site = get_site_code(fname)
        dest = TELEMETRY % {'site' : site}
        try:
            standardize_toa5(fname, dest_path=dest, baled=False)
            to_remove.append(fname)
        except Exception as e:
            logger.error('Exception occurred processing %s - skipping (%s)' %
                         (osp.basename(fname), e))
    logger.debug('Preparing to delete source files')
    for fname in to_remove:
        logger.info('Deleting %s ... ' % fname)
        try:
            os.remove(fname)
        except OSError:
            logger.error('Exception occurred deleting %s - skipping' %
                         osp.basename(fname))


if __name__ == '__main__':
    console = logging.StreamHandler(stream=stdout)
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(console)

    try:
        logfile = logging.FileHandler(TELEMETRY_LOG)
        logfile.setLevel(logging.DEBUG)
        filefmt = logging.Formatter('%(asctime)s %(levelname)-8s  %(message)s')
        logfile.setFormatter(filefmt)
        logger.addHandler(logfile)
    except Exception as err:
        logger.error('Could not log to file (%s)' % TELEMETRY_LOG)
        # TODO add email notice? something to indicate the error noticably
        exit(1)

    logger.setLevel(logging.DEBUG)

    logger.info('Starting telemetry data processing script...')
    if not osp.isdir(TELEMETRY_SRC):
        logger.critical('Unable to locate source directory (%s) - aborting!' %
                        TELEMETRY_SRC)
        # TODO do something more useful than give up
        exit(1)
    if not osp.isdir(osp.splitdrive(TELEMETRY)[0]):
        logger.critical(('Unable to locate drive of target directory (%s) - '
                        'aborting!') % (TELEMETRY))
        # TODO do something more useful than give up
        exit(1)

    print('\n\tSource directory: %s' % TELEMETRY_SRC)
    print('\tDestination dir.: %s' % TELEMETRY)
    print('\n\t\tWARNING: Source *.dat files will be deleted!')
    print('\nPress <Ctrl>+C or <Alt>+F4 to cancel. Starting in ', end='')
    for n in range(31)[:1:-1]:
        print(n, end=' ')
        sleep(1)
    print()
    process_new_telemetry_data()
    logger.info('Done. Exiting in 10 seconds...\n')
    sleep(10)
