# Script for retrieving third-party documentation from the internet

from __future__ import print_function

import sys
import os
import os.path as osp

from urllib import URLopener
from webbrowser import open as openurl

def report_progress(a, b, c):
    # https://stackoverflow.com/a/2003565/2946116
    print("% 3.1f%% of %d bytes\r" % (min(100, float(a*b) / c*100), c), end='')

if __name__ == '__main__':
    os.chdir(osp.dirname(sys.argv[0]))
    print("Destination directory: " + os.getcwd())
    raw_input("Press <enter> to continue or Ctrl+C to exit")

    try:
        srcfile = sys.argv[1]
        f = open(srcfile, mode='r')
    except IndexError, IOError:
        print("Could not find accessible source file!")
        raw_input("Press <enter> to exit.")
        sys.exit(1)

    urlget = URLopener({})
    errors = []
    for line in f.readlines():
        try:
            url, fname = [s.strip() for s in line.split('    ')]
        except ValueError:
            print("Could not parse this input: " + line)
            continue
        if osp.isfile(fname):
            print('Skipping existing file %s' % fname)
        else:
            print('Downloading %s to %s' % (url, fname))
            try:
                urlget.retrieve(url, fname, report_progress)
            except IOError:
                print(' (!) Download failed, adding to plan B list')
                errors.append(url)

    if errors:
        print("\nAn error(s) was detected; would you like to retry using the "+
              "system browser?")
        raw_input("Press Ctrl+C to exit or <enter> to continue.")
        for url in errors:
            openurl(url)

    raw_input("Press <enter> to exit.")
