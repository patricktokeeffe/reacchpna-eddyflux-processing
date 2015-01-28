# -*- coding: utf-8 -*-
"""
Created on Fri Feb 07 09:29:44 2014

@author: pokeeffe
"""
from __future__ import print_function

import logging
import os
import os.path as osp
import sys
import subprocess

from datetime import datetime as dt

import pdfmerge

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email import Encoders

from definitions.sites import site_list
from definitions.paths import TELEMETRY_SRC


### Add your email here:
_TO = ['pokeeffe@wsu.edu']



os.environ['PATH'] = ';'.join(['C:\\Program Files\\Veusz',
                               'C:\\Program Files (x86)\\Veusz',
                               os.getenv('PATH')])
_VZ_CMD = r'veusz --export "%(out)s" "%(in)s"'

_TEMPLATE = 'telemetry_plot_template.vsz' # same directory as this script
_TO_PLOT = ['REMOTE_%s_stats5.dat', 'REMOTE_%s_diagnostics.dat']

_IMPORT = ("ImportFileCSV(u'%(file)s', dateformat=u'YYYY-MM-DD hh:mm:ss', "
           "headerignore=2, headermode='1st', linked=True, rowsignore=1)")


def email_telemetry_plots(logger):
    """Print plots for each site to PDF using latest telemetry data"""
    merged_pdf_name = 'telemetry_%s.pdf' % dt.now().date()

    sources = {}
    for site in site_list:
        sources[site.code] = [osp.join(TELEMETRY_SRC, f % site.code) for f in _TO_PLOT]

    output = {}
    header = []
    header_found = False
    page = 1
    with open(_TEMPLATE, 'r') as template:
        logger.info('Parsing telemetry plot template ... ')
        for line in template:
            if not header_found:
                # strip off lines up to first instance of Add('page') to
                # use as header for all temporary to-export files
                if line.startswith('ImportFileCSV'):
                    header.append('%s\n')
                elif not line.startswith('Add(\'page\','):
                    # prep all other lines for %-substitution but only catch
                    # when used as a percent sign:  '+2%')
                    header.append(line.replace("%'", "%%'"))
                else:
                    # catch first Add('page') as first output line
                    outlist = output.setdefault(page, [])
                    outlist.append(line)
                    header_found = True
            elif line.startswith('Add(\'page\','):
                page += 1
                outlist = output.setdefault(page, [])
                outlist.append(line)
            else:
                outlist.append(line.replace("%'", "%%'"))

    filesmade = []
    todelete = []
    logger.info('Generating Veusz export documents ... ')
    for site in site_list:
        for pgnum, content in output.items():
            tempname = 'template_%s_page%s.vsz' % (site.code, pgnum)
            import1 = _IMPORT % {'file' : sources[site.code][0]} #stats5
            import2 = _IMPORT % {'file' : sources[site.code][1]} #diagnostics
            with open(tempname, 'w') as f:
                f.write(''.join(header) % (import1, import2))
                f.write(''.join(content) % {'pagetitle' : site.code,
                                'today' : dt.now().strftime('%Y-%m-%d %X')})
            filesmade.append(tempname)
            todelete.append(tempname)

    tomerge = []
    for input in filesmade:
        output = osp.splitext(input)[0]+'.pdf'
        logger.info('Exporting to PDF: %s' % input)
        try:
            subprocess.call(_VZ_CMD % {'out' : output, 'in' : input})
            tomerge.append(output)
            todelete.append(output)
        except Exception:
            logger.error('Failed to export to PDF: %s' % input)

    logger.info('Merging exported PDF files ... ')
    pdfmerge.merge(tomerge, merged_pdf_name)

    from_ = 'reacch-telemetry@lar-d216-share.cee.wsu.edu'
    subject = 'REACCH telemetry plots [%s]' % dt.now().date()
    msgtext = """\
A summary of REACCH monitoring tower data collected over the past 24 hours is
attached for your enjoyment.

-----------------------------------------------------------------------------
This message was sent automatically from an unmonitored email address. To
unsubscribe, send a request to Patrick O'Keeffe <pokeeffe@wsu.edu>
"""
    attachment = MIMEBase('application', 'octet-stream')
    attachment.set_payload(open(merged_pdf_name, 'rb').read())
    Encoders.encode_base64(attachment)
    attachment.add_header('Content-Disposition',
                          'attachment; filename="%s"' % merged_pdf_name)
    msg = MIMEMultipart()
    msg['From'] = from_
    msg['To'] = ', '.join(_TO)
    msg['Subject'] = subject
    msg.attach(MIMEText(msgtext) )
    msg.attach(attachment)
    logger.info('Sending email summary to %s ' % ', '.join(_TO))
    server = smtplib.SMTP('localhost')
    server.sendmail(from_, _TO, msg.as_string())
    server.quit()
    todelete.append(merged_pdf_name)

    for name in todelete:
        try:
            os.remove(name)
            # FIXME known to fail to delete merged PDFs
            # this is caused by an issue deep within `pdfmerge`
            logger.info('Deleted %s' % name)
        except WindowsError as err:
            logger.warn('Unable to delete %s: %s' % (name, err))


if __name__ == '__main__':
    print('='*79)
    print('''\
| Regional Approaches to Climate Change 2011-2016                             |
| Laboratory for Atmospheric Research at Washington State University          |
|                                                                             |
''', end='')
    if not osp.isdir(TELEMETRY_SRC):
        print('''\
|   (!) ERROR  The telemetry data source folder could not be located:         |
|       {:<69s} |
|                                                                             |
'''.format(TELEMETRY_SRC), end='')
    print('''\
| This program NORMALLY RUNS AUTOMATICALLY each morning during the routine    |
| which pushes recently collected telemetry data to the LAR/REACCH share      |
| (ftp://lar-d216-share.cee.wsu.edu). It is not intended to be run manually   |
| and will generally not work except for on the LAR/REACCH share.             |
|                                                                             |
| This program generates plots of the most recent data collected by telemetry |
| from each monitoring tower site, exports them to a PDF file, and emails the |
| PDF to:                                                                     |
''', end='')
    for name in _TO:
        print('''\
|     {:<71s} |'''.format(name))
    print('='*79)

    ans = ''
    while not ans.strip().lower() == 'yes':
        ans = raw_input('Press <Ctrl>+C to exit or type "yes" to continue: ')

    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(logging.DEBUG)
    console.setFormatter(logging.Formatter('%(message)s'))
    logger = logging.getLogger(__name__)
    logger.addHandler(console)
    logger.setLevel(logging.DEBUG)

    email_telemetry_plots(logger)
