# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 18:09:21 2014

@author: pokeeffe
"""

from __future__ import print_function

import subprocess
import sys

from definitions.sites import site_list
from definitions.paths import RAW_ASCII, TELEMETRY


if __name__ == '__main__':
    print('==== Rebuild telemetry files :: REACCH Obj2 ====\n\n'
        'This program will reconstruct data files in the "telemetry data" '
        'folder of all monitoring sites. Any existing files will be '
        'overwritten and their data lost.\n\n'
        'In some instances, data gathered by telemetry is available when data '
        'collected from the field is not. DO NOT PROCEED if existing '
        'telemetry data has not been used to patch gaps in "standard format" '
        'data files.\n')

    print('Looking for raw TOA5 files in', RAW_ASCII)
    print('Writing rebuild files into', TELEMETRY)
    ans = raw_input('Would you like to rebuild into special directories? ')
    if ans and ans[0] in ['Y', 'y']:
        TELEMETRY = TELEMETRY + '_rebuild'
        print('  -> Rebuilding into', TELEMETRY)
    else:
        print('  -> No change.')

    raw_input('\nPress <enter> to begin or <ctrl>+C to abort.\n')

    for site in site_list:
        print('='*78)
        print('Rebuilding', site.name, '('+site.code+') \n')
        look_in = RAW_ASCII % {'site' : site.code}
        send_to = TELEMETRY % {'site' : site.code}

        call = ('python standardize_toa5.py --infilt *.dat --exfilt tsdata '
                'ts_data -d "%s" -o "%s" -v --nobale') % (look_in, send_to)
        subprocess.call(call, shell=True, stdout=sys.stdout)
        print()

