# -*- coding: utf-8 -*-
"""Split large ASCII files into smaller chunks

@author Patrick O'Keeffe <pokeeffe@wsu.edu>
"""

import os
import sys

from version import version as __version__

DEFAULT_HDR_LINES = 4
DEFAULT_MAX_LINES = 1000000

if __name__ == '_foo_':
    from argparse import ArgumentParser
    parser = ArgumentParser(description='spam and eggs.')
    parser.add_argument('source_file', help='file to split')

    parser.add_argument('-i',
                        '--interactive',
                        action='store_true',
                        help='start interactive session')
    parser.add_argument('-l',
                        '--hdr-lines',
                        type=int,
                        help='specify lines in header, default:4')
    parser.add_argument('-o',
                        '--out-lines',
                        type=int,
                        help='specify max lines per output file')
    parser.add_argument('-r',
                        '--rm-source',
                        action='store_true',
                        help='include flag to delete source files')

    args = parser.parse_args()


    _usage = """\
    Usage: %s [-i] [-h] [-o] file1 [file2 ... ]

    Write text file(s) as series of smaller files. Use -i for interactive mode.

    Supply a white-space separated list of files to process. Files will be read,
    then written as series of smaller files. Output files have the name of the
    original file plus an incremental suffix. The original file's header is
    repeated in each output file.

    Mandatory arguments to long options are mandatory for short options too.
      -i, --interactive     use interactive mode which confirms each step with
                            user and prompts for unknown values. Can be used
                            with other arguments
      -h, --hdr-lines       number of header lines, default: 4 (Campbell
                            Scientific TOA5 compatible)
      -o, --out-lines       number of lines per output file, default: 10^6
      -r, --rm-source       nonzero value results in source file being removed
                            (deleted) after being split up
    """ % sys.argv[0]


def _log(msg):
    sys.stdout.write(msg)


def split_toa5(source_file,
               max_lines=DEFAULT_MAX_LINES,
               hdr_lines=DEFAULT_HDR_LINES,
               delete_source=False):
    """Split text file up into several smaller files

    Reads specific number of rows from input file and writes them to output
    file. Number of rows written per output file controlled by ``max_lines``.

    Output file names are generated from input file name plus an underscore
    '_' and an incremented digit in two fields, starting from 1 (ie, '_01',
    '_02', ..., '_99', '_100').

    Header rows of the input file are preserved and repeated in each output
    file. Headers are assumed to start on line 0 always. The number of lines
    in the header is set through ``hdr_lines``.


    Parameters
    ----------
    source_file : file-like
        Any object which supports the readline method
    max_lines : int
        Number of non-header lines to output in each file (maximum; last
        file will typically have less) default: ``DEFAULT_MAX_LINES``
    hdr_lines : int
        Number of lines to use for header in output files. Use 0 to disable
        Header always starts on line 0. default: ``DEFAULT_HDR_LINES``
    delete_source: bool
        If True, attempts to delete source file. Default: False

    Returns
    -------
    list : of file names generated
    """
    tfile = open(source_file)
    basename, ext = os.path.splitext(tfile.name)
    partno = 1
    outname = '%s_%02d%s' % (basename, partno, ext)
    towrite = []
    results = []

    _log('Reading header... ')
    lineno = 0
    hdr = []
    for i in range(hdr_lines):
        hdr.append(tfile.readline())
        lineno += 1
    _log('Done.\n')

    _log('Reading data...\n')
    for line in tfile:
        lineno += 1
        towrite.append(line)
        if lineno % 160000 == 0:
            _log('.\n')
        if lineno % 2000 == 0:
            _log('.')
        if lineno > max_lines:
            _log('\nWriting chunk to file (%s)... ' % outname)
            outfile = open(outname, mode='w')
            outfile.writelines(hdr)
            outfile.writelines(towrite)
            outfile.close()
            _log('Done.\n')
            towrite = []
            lineno = 0
            partno += 1
            outname = '%s_%02d%s' % (basename, partno, ext)
            results.append(outname)
            #outname = ''.join([basename, '_PART', str(partno), ext])

    if len(towrite) > 0:
        _log('\nWriting chunk to file (%s)... ' % outname)
        outfile = open(outname, mode='w')
        outfile.writelines(hdr)
        outfile.writelines(towrite)
        outfile.close()
        _log('Done.\n')


    tfile.close()
    if delete_source:
        try:
            os.remove(source_file)
        except WindowsError as err:
            _log('Could not remove source file: %s' % err)
    _log('Finished splitting file (%s)\n' % tfile.name)
    return results


if __name__ == '__main__':
    print 'Hello, user. I can split large text files into smaller pieces. Just'
    print 'provide an absolute or relative path to a source file and I will'
    print 'create smaller files with the same name plus an incremental suffix.'
    print '\nYour current working directory is %s' % os.getcwd()

    if len(sys.argv) > 1:
        fname = sys.argv[1]
        print 'Using source file %s' % fname
    else:
        fname = raw_input('Name of source file?: ')

    print 'Default max lines per output file = %d' % DEFAULT_MAX_LINES
    while 1:
        ml = raw_input('Press <enter> to accept or provide new value: ').strip()
        if ml.isdigit() and int(ml) > 0:
            print 'now using %s' % ml
            maxlinesout = int(ml)
            break
        elif not ml:
            maxlinesout = DEFAULT_MAX_LINES
            break
        else:
            print '    >> Error: non-numeric value received.'

    raw_input('\nReady to begin. Press <enter> to continue...')
    if os.path.isfile(fname):
        split_toa5(fname, max_lines=maxlinesout)
    else:
        print 'Error: sorry but I cannot find the source file (%s)' % fname

