# -*- coding: utf-8 -*-
"""
Send email notice to alerts mailing list upon failed scheduled collection

@author: pokeeffe
"""
from __future__ import print_function

import sys

from datetime import datetime as dt

import smtplib
from email.MIMEText import MIMEText



def send_notice(site_code):
    """Print plots for each site to PDF using latest telemetry data"""

    to_ = 'lar-2011-reacch-tower-alerts@lists.wsu.edu'
    from_ = 'loggernet@lar-d216-share.cee.wsu.edu'
    subject = 'Failed collection notice (%s)' % dt.now().date()
    msgtext = """\
The primary retry intervals for %(site)s have been exhausted. This site is \
now on its secondary retry schedule.

"""

    msg = MIMEText(msgtext % {"site": site_code})
    msg['From'] = from_
    msg['To'] = to_
    msg['Subject'] = subject

    server = smtplib.SMTP('localhost')
    server.sendmail(from_, to_, msg.as_string())
    server.quit()


if __name__ == '__main__':
    sitecode = str(sys.argv[1]).upper() if len(sys.argv) > 1 else 'UNSPECIFIED'
    try:
        send_notice(sitecode)
    except:
        pass

