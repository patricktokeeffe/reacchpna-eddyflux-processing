# -*- coding: utf-8 -*-
"""Objects representing field sites

    Typically just import site_list to iterate over:

        from definitions.sites import site_list

@author: Patrick O'Keeffe <pokeeffe@wsu.edu>
"""


class cfnt():
    name = 'Cook Agronomy Farm no-till'
    code = 'CFNT'
    serial_num = '6034'

class lind():
    name = 'Lind Dryland Research Station'
    code = 'LIND'
    serial_num = '6035'

class cfct():
    name = 'Cook Agronomy Farm conventional till'
    code = 'CFCT'
    serial_num = '6503'

class mmtn():
    name = 'Moscow Mountain high rainfall'
    code = 'MMTN'
    serial_num = '6506'

class mslk():
    name = 'Moses Lake irrigated'
    code = 'MSLK'
    serial_num = '6505'
 
class wine():
    name = 'Vineyard Pilot Project'
    code = 'WINE'
    serial_num = '6504'


site_list = [cfnt, cfct, mmtn, lind, mslk, wine]


sn2code = dict([[site.serial_num, site.code] for site in site_list])


