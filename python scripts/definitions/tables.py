# -*- coding: utf-8 -*-
"""Data table structure definitions and utility functions

    Run module alone to conduct self-test routine.


@author: Patrick O'Keeffe <pokeeffe@wsu.edu>
"""

from copy import copy
from pandas.tseries.offsets import Day, MonthBegin


class ColumnNotFoundError(Exception): pass
"""Raised when look up of nonexistant column is attempted"""


def current_names(table, column):
    """Get current (table, column) names from historical aliases

    Provides the current table & column name for any given table & column
    name from REACCH Objective 2 EC tower data files. If column is no longer
    included, the tuple ``(None, None)`` is returned. See Details for an
    explanation of how it works.

    Parameters
    ----------
    table : str
        name of data table, as deployed, case-sensitive; can be found at the
        end of the first line in files with original headers
    column : str
        name of data file column, as deployed, case-sensitive;

    Returns
    -------
    Tuple of table name and column name according to present definitions or
    ``(None, None)`` if column is no longer present.

    Examples
    --------

    The sonic diagnostic flags are no longer totaled:

        >>> current_names('flux', 'sonic_cal_err_f_Tot')
        (None, None)

    Oldest data table columns are translated to current names:

        >>> current_names('flux', 'l')
        ('stats30', 'L')

    But if it's a current set of names, the input is returned:

        >>> current_names('stats30', 'L')
        ('stats30', 'L')

    Details
    -------
    Internally, ``current_names`` uses a dictionary whereby keys and values
    are tuples of the form (table name, column name). All current and prior
    table and column pairs are described a key. Table/column pairs which are
    defined in the current data schema have values of ``('','')``. Those
    columns which are dropped from the data schema have values of
    ``(None, None)``. When a table and/or column name is changed, the
    table/column key is retired by changing the corresponding tuple value(s)
    to the new name(s); unless the column has been dropped, a new dictionary
    entry must also be added with the new table/column name as the key and a
    tuple of empty strings as the value.

    By structuring the dictionary in this way, the actual look-up performed
    by ``current_names`` happens recursively:

        ('flux', 'WS_ms_WVc(1)')] => ('', '034b_ws') --->

        ('flux', '034b_ws')] => ('CFNT_stats30', 'Met1_wnd_spd') --->

        ('CFNT_stats30', 'Met1_wnd_wpd')] => ('stats30', '') --->

        ('stats30', 'Met1_wnd_spd')] => ('', '') # current tbl/col names

    This mimics how you might actually figure out what the current table and
    column name of a given column is. So long as the dictionary is correct,
    things are gravy, and despite being long, the dictionary is simple.
    Additionally, there are internal functions for checking the validity of
    the dictionary -- run (double-click) the source file to use them, they
    take too long to be suitable for running upon import.
    """
    try:
        tbl, col = col_alias[(table, column)]
    except KeyError:
        #print ('>>>> Unable to find historical names for column "%s" of table "%s"'
        #        % (column, table))
        raise ColumnNotFoundError
    if (tbl, col) == (None, None):
        return (None, None)
    elif (tbl, col) == ('', ''):
        return (table, column)
    elif tbl == '':
        return current_names(table, col)
    elif col == '':
        return current_names(tbl, column)
    else:
        return current_names(tbl, col)





table_definitions = {
    ### See docstring below definition ###
    'tsdata' : ['TIMESTAMP',
                'RECORD',
                'Ux',
                'Uy',
                'Uz',
                'Ts',
                'diag_sonic',
                'CO2',
                'H2O',
                'diag_irga',
                'amb_tmpr',
                'amb_press',
                'CO2_signal',
                'H2O_signal'],

    # definition for stats5 after end of initial dict def.
    'stats30' : ['TIMESTAMP',
                 'RECORD',
                 'L',
                 'u_star',
                 'tau',
                 'Fc_wpl',
                 'LE_wpl',
                 'Hc',
                 'Ts_Avg',
                 'Ts_Std',
                 'Tc_Avg',
                 'Uz_Std',
                 'wnd_spd',
                 'rslt_wnd_spd',
                 'rslt_wnd_dir',
                 'std_wnd_dir',
                 'sonic_uptime',
                 'CO2_ppm_Avg',
                 'CO2_mg_m3_Avg',
                 'CO2_mg_m3_Std',
                 'CO2_signal_Avg',
                 'H2O_g_kg_Avg',
                 'H2O_g_m3_Avg',
                 'H2O_g_m3_Std',
                 'H2O_signal_Avg',
                 'amb_tmpr_Avg',
                 'amb_press_Avg',
                 'irga_uptime',
                 'T_hmp_Avg',
                 'RH_hmp_Avg',
                 'e_hmp_Avg',
                 'e_sat_hmp_Avg',
                 'Rn_Avg',
                 'Rn_meas_Avg',
                 'PAR_totflx_Tot',
                 'PAR_flxdens_Avg',
                 'Met1_wnd_spd',
                 'Met1_rslt_wnd_spd',
                 'Met1_rslt_wnd_dir',
                 'Met1_std_wnd_dir',
                 'Rain_mm_Tot',
                 'soil_5TM_ID5_E_Avg',
                 'soil_5TM_ID5_T_Avg',
                 'soil_5TM_ID5_VWC_Avg',
                 'soil_5TM_ID6_E_Avg',
                 'soil_5TM_ID6_T_Avg',
                 'soil_5TM_ID6_VWC_Avg',
                 'soil_5TM_ID7_E_Avg',
                 'soil_5TM_ID7_T_Avg',
                 'soil_5TM_ID7_VWC_Avg',
                 'soil_5TM_ID8_E_Avg',
                 'soil_5TM_ID8_T_Avg',
                 'soil_5TM_ID8_VWC_Avg',
                 'soil_5TM_ID9_E_Avg',
                 'soil_5TM_ID9_T_Avg',
                 'soil_5TM_ID9_VWC_Avg',
                 'soil_hfp1_heat_flux_Avg',
                 'soil_hfp1_sensitivity',
                 'soil_hfp2_heat_flux_Avg',
                 'soil_hfp2_sensitivity',
                 'panel_tmpr_Avg',
                 'batt_volt_Avg',
                 ],

    'site_daily' : ['TIMESTAMP',
                    'RECORD',
                    'latitude_Med',
                    'longitude_Med',
                    'magnetic_variation_Med',
                    'nmbr_satellites_Avg',
                    'altitude_Med',
                    'altitude_Avg',
                    'gps_ready_Min',
                    'max_clock_change',
                    'nmbr_clock_change',
                    'batt_volt_Min',
                    'batt_volt_TMn',
                    'batt_volt_Max',
                    'batt_volt_TMx',
                    'T_hmp_Min',
                    'T_hmp_TMn',
                    'T_hmp_Max',
                    'T_hmp_TMx'],

    'diagnostics' : ['TIMESTAMP',
                     'RECORD',
                     'total_scans',
                     'scans_1hz',
                     'scans_5s',
                     'skipped_10hz_scans',
                     'skipped_1hz_scans',
                     'skipped_5s_scans',
                     'watchdog_errors'],

    # update homogenize_toa5 special case if modified
    'site_info' : ['TIMESTAMP',
                   'RECORD',
                   'UTC_offset',
                   'sonic_azimuth',
                   'NRLite2_sens',
                   'LI190SB_sens',
                   'HFP_1_sens',
                   'HFP_2_sens',
                   'CompileResults',
                   'CardStatus',
                   'RunSig',
                   'ProgSig',
                   'GitRepoTag',
                   'hfp_installed'],

    'extra_info' : ['TIMESTAMP',
                    'RECORD',
                    'Decagon_NDVI_installed',
                    'Decagon_PRI_installed',
                    'LGR_n2oco_installed',
                    'lgr_n2o_mult',
                    'lgr_n2o_offset',
                    'lgr_co_mult',
                    'lgr_co_offset',
                    'lgr_h2o_mult',
                    'lgr_h2o_offset',
                    'Picarro_co2ch4_installed',
                    'pic_co2_mult',
                    'pic_co2_offset',
                    'pic_ch4_mult',
                    'pic_ch4_offset',
                    'pic_h2o_mult',
                    'pic_h2o_offset'],

    'tsdata_extra' : ['TIMESTAMP',
                      'RECORD',
                      'lgr_n2o',
                      'lgr_co',
                      'lgr_h2o',
                      'pic_co2',
                      'pic_ch4',
                      'pic_h2o'],

    # definitions for stats5_ui below dictionary definition
    'stats30_ui' : ['TIMESTAMP',
                    'RECORD',
                    'dec_ndvi_up1_Avg',
                    'dec_ndvi_up2_Avg',
                    'dec_ndvi_dn1_Avg',
                    'dec_ndvi_dn2_Avg',
                    'dec_pri_up1_Avg',
                    'dec_pri_up2_Avg',
                    'dec_pri_dn1_Avg',
                    'dec_pri_dn2_Avg',
                    'tblcalls_Tot'],

    # definition for stats5_6rad below dict def.
    'stats30_6rad' : ['TIMESTAMP',
                      'RECORD',
                      'dec_6rad_uplook_Avg(1)',
                      'dec_6rad_uplook_Avg(2)',
                      'dec_6rad_uplook_Avg(3)',
                      'dec_6rad_uplook_Avg(4)',
                      'dec_6rad_uplook_Avg(5)',
                      'dec_6rad_uplook_Avg(6)',
                      'dec_6rad_dnlook_Avg(1)',
                      'dec_6rad_dnlook_Avg(2)',
                      'dec_6rad_dnlook_Avg(3)',
                      'dec_6rad_dnlook_Avg(4)',
                      'dec_6rad_dnlook_Avg(5)',
                      'dec_6rad_dnlook_Avg(6)',
                      'tblcalls_Tot']
}
"""This dict holds lists describing the order of columns because the alias
   look-up dict cannot hold that information in a straightforward way. """
table_definitions['stats5'] = copy(table_definitions['stats30'])
table_definitions['stats5_6rad'] = copy(table_definitions['stats30_6rad'])
table_definitions['stats5_ui'] = copy(table_definitions['stats30_ui'])


table_baleinfo = {
    ### See docstring below definition ###
    'tsdata' : ( [lambda x: x.year, lambda x: x.month, lambda x: x.day],
                 lambda x: x.index.normalize()[0],
                 Day(),
                 '100L' ),

    'stats30' : ( [lambda x: x.year, lambda x: x.month],
                  lambda x: MonthBegin().rollback(x.index.normalize()[0]),
                  MonthBegin(),
                  '30T' ),

    'stats5' : ( [lambda x: x.year, lambda x: x.month],
                 lambda x: MonthBegin().rollback(x.index.normalize()[0]),
                 MonthBegin(),
                 '5T' ),

    'site_daily' : (None, lambda x: x.index.normalize()[0], None, 'D'),

    'diagnostics' : (None, lambda x: x.index.normalize()[0], None, '5T'),

    'site_info' : (None, lambda x: x.index.normalize()[0], None, None),

    'extra_info' : (None, lambda x: x.index.normalize()[0], None, None),

    'tsdata_extra' : ( [lambda x: x.year, lambda x: x.month, lambda x: x.day],
                       lambda x: x.index.normalize()[0],
                       Day(),
                       '100L' ),

    'stats30_ui' : ( [lambda x: x.year, lambda x: x.month],
                     lambda x: MonthBegin().rollback(x.index.normalize()[0]),
                     MonthBegin(),
                     '30T' ),

    'stats5_ui' : ( [lambda x: x.year, lambda x: x.month],
                    lambda x: MonthBegin().rollback(x.index.normalize()[0]),
                    MonthBegin(),
                    '5T' ),

    'stats30_6rad' : ( [lambda x: x.year, lambda x: x.month],
                       lambda x: MonthBegin().rollback(x.index.normalize()[0]),
                       MonthBegin(),
                       '30T' ),

    'stats5_6rad' : ( [lambda x: x.year, lambda x: x.month],
                      lambda x: MonthBegin().rollback(x.index.normalize()[0]),
                      MonthBegin(),
                      '5T' ),
}
"""Dict holds info needed to 'time-bale' data files

Keys are string data table names. Values are 4-tuples containing:
    1) grpby   specify data grouping for export; by months, days or if not
               broken up by dates, None
    2) start   function to obtain first timestamp of 'proper' file; since
               data column may not start at beginning of period (think
               file starts @ 8am for daily, not midnight) calculate the
               first timestamp
    3) offset  pandas.tseries.offsets object corresponding to size of
               desired output file or None if not splitting by date
    4) freq    intended frequency of output (and input) files. required
               and is used to properly pad output files; if table is
               recorded to irregularly (eg site_info) then freq is None
               and files are not padded to a standard frequency
"""


col_alias = {
    ### See docstring below definition ###

    ########## Vineyard pilot study ########################################
    ('site_info', 'GitRepoTag') : ('', ''),

    ########## 20130507_XXXX - CURRENT #####################################
    ('stats30_ui', 'TIMESTAMP') : ('', ''),
    ('stats30_ui', 'RECORD') : ('', ''),
    ('stats30_ui', 'dec_ndvi_up1_Avg') : ('', ''),
    ('stats30_ui', 'dec_ndvi_up2_Avg') : ('', ''),
    ('stats30_ui', 'dec_ndvi_dn1_Avg') : ('', ''),
    ('stats30_ui', 'dec_ndvi_dn2_Avg') : ('', ''),
    ('stats30_ui', 'dec_pri_up1_Avg') : ('', ''),
    ('stats30_ui', 'dec_pri_up2_Avg') : ('', ''),
    ('stats30_ui', 'dec_pri_dn1_Avg') : ('', ''),
    ('stats30_ui', 'dec_pri_dn2_Avg') : ('', ''),
    ('stats30_ui', 'tblcalls_Tot') : ('', ''),

    ('stats5_ui', 'TIMESTAMP') : ('', ''),
    ('stats5_ui', 'RECORD') : ('', ''),
    ('stats5_ui', 'dec_ndvi_up1_Avg') : ('', ''),
    ('stats5_ui', 'dec_ndvi_up2_Avg') : ('', ''),
    ('stats5_ui', 'dec_ndvi_dn1_Avg') : ('', ''),
    ('stats5_ui', 'dec_ndvi_dn2_Avg') : ('', ''),
    ('stats5_ui', 'dec_pri_up1_Avg') : ('', ''),
    ('stats5_ui', 'dec_pri_up2_Avg') : ('', ''),
    ('stats5_ui', 'dec_pri_dn1_Avg') : ('', ''),
    ('stats5_ui', 'dec_pri_dn2_Avg') : ('', ''),
    ('stats5_ui', 'tblcalls_Tot') : ('', ''),

    ('diagnostics', 'TIMESTAMP') : ('', ''),
    ('diagnostics', 'RECORD') : ('', ''),
    ('diagnostics', 'total_scans') : ('', ''),
    ('diagnostics', 'scans_1hz') : ('', ''),
    ('diagnostics', 'scans_5s') : ('', ''),
    ('diagnostics', 'skipped_10hz_scans') : ('', ''),
    ('diagnostics', 'skipped_1hz_scans') : ('', ''),
    ('diagnostics', 'skipped_5s_scans') : ('', ''),
    ('diagnostics', 'watchdog_errors') : ('', ''),

    ('tsdata_extra', 'TIMESTAMP') : ('', ''),
    ('tsdata_extra', 'RECORD') : ('', ''),
    ('tsdata_extra', 'lgr_n2o') : ('', ''),
    ('tsdata_extra', 'lgr_co') : ('', ''),
    ('tsdata_extra', 'lgr_h2o') : ('', ''),
    ('tsdata_extra', 'pic_co2') : ('', ''),
    ('tsdata_extra', 'pic_ch4') : ('', ''),
    ('tsdata_extra', 'pic_h2o') : ('', ''),

    ('extra_info', 'TIMESTAMP') : ('', ''),
    ('extra_info', 'RECORD') : ('', ''),
    ('extra_info', 'Decagon_NDVI_installed') : ('', ''),
    ('extra_info', 'Decagon_PRI_installed') : ('', ''),
    ('extra_info', 'LGR_n2oco_installed') : ('', ''),
    ('extra_info', 'lgr_n2o_mult') : ('', ''),
    ('extra_info', 'lgr_n2o_offset') : ('', ''),
    ('extra_info', 'lgr_co_mult') : ('', ''),
    ('extra_info', 'lgr_co_offset') : ('', ''),
    ('extra_info', 'lgr_h2o_mult') : ('', ''),
    ('extra_info', 'lgr_h2o_offset') : ('', ''),
    ('extra_info', 'Picarro_co2ch4_installed') : ('', ''),
    ('extra_info', 'pic_co2_mult') : ('', ''),
    ('extra_info', 'pic_co2_offset') : ('', ''),
    ('extra_info', 'pic_ch4_mult') : ('', ''),
    ('extra_info', 'pic_ch4_offset') : ('', ''),
    ('extra_info', 'pic_h2o_mult') : ('', ''),
    ('extra_info', 'pic_h2o_offset') : ('', ''),
    ########################################################################


    ########## 20120810_LIND - CURRENT #####################################
    ('stats30_hfp', 'TIMESTAMP') : (None, None),
    ('stats30_hfp', 'RECORD') : (None, None),
    ('stats30_hfp', 'soil_hfp1_heat_flux_Avg') : ('stats30', ''),
    ('stats30_hfp', 'soil_hfp1_sensitivity') : ('stats30', ''),
    ('stats30_hfp', 'soil_hfp2_heat_flux_Avg') : ('stats30', ''),
    ('stats30_hfp', 'soil_hfp2_sensitivity') : ('stats30', ''),
    ('stats30_hfp', 'hfp1_samples_Tot') : (None, None), # 20120824_LIND
    ('stats30_hfp', 'hfp2_samples_Tot') : (None, None), # 20120824_LIND
    ('stats30_hfp', 'tblcalls_Tot') : (None, None),

    ('stats5_hfp', 'TIMESTAMP') : (None, None),
    ('stats5_hfp', 'RECORD') : (None, None),
    ('stats5_hfp', 'soil_hfp1_heat_flux_Avg') : ('stats5', ''),
    ('stats5_hfp', 'soil_hfp1_sensitivity') : ('stats5', ''),
    ('stats5_hfp', 'soil_hfp2_heat_flux_Avg') : ('stats5', ''),
    ('stats5_hfp', 'soil_hfp2_sensitivity') : ('stats5', ''),
    ('stats5_hfp', 'hfp1_samples_Tot') : (None, None), # 20120824_LIND
    ('stats5_hfp', 'hfp2_samples_Tot') : (None, None), # 20120824_LIND
    ('stats5_hfp', 'tblcalls_Tot') : (None, None),
    ########################################################################


    ########## 20120627_CFCT - CURRENT #####################################
    ('site_info', 'TIMESTAMP') : ('', ''),
    ('site_info', 'RECORD') : ('', ''),
    ('site_info', 'UTC_offset') : ('', ''),
    ('site_info', 'sonic_azimuth') : ('', ''),
    ('site_info', 'NRLite2_sens') : ('', ''),
    ('site_info', 'LI190SB_sens') : ('', ''),
    ('site_info', 'HFP_1_sens') : ('', ''),
    ('site_info', 'HFP_2_sens') : ('', ''),
    ('site_info', 'CompileResults') : ('', ''),
    ('site_info', 'CardStatus') : ('', ''),
    ('site_info', 'RunSig') : ('', ''),
    ('site_info', 'ProgSig') : ('', ''),
    ('site_info', 'SENSOR_DEC_5TM') : (None, None), # 20120810_LIND
    ('site_info', 'SENSOR_DEC_6RAD') : ('', 'Dec_6rad_installed'),
    # XXX should these *installed variables be retained somehow?
    ('site_info', 'Dec_6rad_installed') : (None, None),
    ('site_info', 'SENSOR_LGR_N2OCO') : ('', 'LGR_n2o_co_installed'),
    ('site_info', 'LGR_n2o_co_installed') : ('extra_info', 'LGR_n2oco_installed'),
    ('site_info', 'SENSOR_PIC_CO2CH4') : ('', 'Pic_co2_ch4_installed'),
    ('site_info', 'Pic_co2_ch4_installed') : ('extra_info', 'Picarro_co2ch4_installed'),
    ('site_info', 'SENSOR_HFP01SC') : ('', 'hfp_installed'),
    ('site_info', 'hfp_installed') : ('', ''),
    ########################################################################


    ########## 20120627_CFCT - CURRENT #####################################
    ########## 20120427_CFNT  #############################################
    ('stats30_6rad', 'TIMESTAMP') : ('', ''),
    ('stats30_6rad', 'RECORD') : ('', ''),
    ('stats30_6rad', 'dec_6rad_up_Avg(1)') : ('', 'dec_6rad_uplook_Avg(1)'),
    ('stats30_6rad', 'dec_6rad_up_Avg(2)') : ('', 'dec_6rad_uplook_Avg(2)'),
    ('stats30_6rad', 'dec_6rad_up_Avg(3)') : ('', 'dec_6rad_uplook_Avg(3)'),
    ('stats30_6rad', 'dec_6rad_up_Avg(4)') : ('', 'dec_6rad_uplook_Avg(4)'),
    ('stats30_6rad', 'dec_6rad_up_Avg(5)') : ('', 'dec_6rad_uplook_Avg(5)'),
    ('stats30_6rad', 'dec_6rad_up_Avg(6)') : ('', 'dec_6rad_uplook_Avg(6)'),
    ('stats30_6rad', 'dec_6rad_dn_Avg(1)') : ('', 'dec_6rad_dnlook_Avg(1)'),
    ('stats30_6rad', 'dec_6rad_dn_Avg(2)') : ('', 'dec_6rad_dnlook_Avg(2)'),
    ('stats30_6rad', 'dec_6rad_dn_Avg(3)') : ('', 'dec_6rad_dnlook_Avg(3)'),
    ('stats30_6rad', 'dec_6rad_dn_Avg(4)') : ('', 'dec_6rad_dnlook_Avg(4)'),
    ('stats30_6rad', 'dec_6rad_dn_Avg(5)') : ('', 'dec_6rad_dnlook_Avg(5)'),
    ('stats30_6rad', 'dec_6rad_dn_Avg(6)') : ('', 'dec_6rad_dnlook_Avg(6)'),
    ('stats30_6rad', 'dec_6rad_uplook_Avg(1)') : ('', ''), # 20120627 - CURRENT
    ('stats30_6rad', 'dec_6rad_uplook_Avg(2)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_uplook_Avg(3)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_uplook_Avg(4)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_uplook_Avg(5)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_uplook_Avg(6)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_dnlook_Avg(1)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_dnlook_Avg(2)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_dnlook_Avg(3)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_dnlook_Avg(4)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_dnlook_Avg(5)') : ('', ''),
    ('stats30_6rad', 'dec_6rad_dnlook_Avg(6)') : ('', ''),
    ('stats30_6rad', 'tblcalls_Tot') : ('', ''), # add 20120824_LIND

    ('stats5_6rad', 'TIMESTAMP') : ('', ''),
    ('stats5_6rad', 'RECORD') : ('', ''),
    ('stats5_6rad', 'dec_6rad_up_Avg(1)') : ('', 'dec_6rad_uplook_Avg(1)'),
    ('stats5_6rad', 'dec_6rad_up_Avg(2)') : ('', 'dec_6rad_uplook_Avg(2)'),
    ('stats5_6rad', 'dec_6rad_up_Avg(3)') : ('', 'dec_6rad_uplook_Avg(3)'),
    ('stats5_6rad', 'dec_6rad_up_Avg(4)') : ('', 'dec_6rad_uplook_Avg(4)'),
    ('stats5_6rad', 'dec_6rad_up_Avg(5)') : ('', 'dec_6rad_uplook_Avg(5)'),
    ('stats5_6rad', 'dec_6rad_up_Avg(6)') : ('', 'dec_6rad_uplook_Avg(6)'),
    ('stats5_6rad', 'dec_6rad_dn_Avg(1)') : ('', 'dec_6rad_dnlook_Avg(1)'),
    ('stats5_6rad', 'dec_6rad_dn_Avg(2)') : ('', 'dec_6rad_dnlook_Avg(2)'),
    ('stats5_6rad', 'dec_6rad_dn_Avg(3)') : ('', 'dec_6rad_dnlook_Avg(3)'),
    ('stats5_6rad', 'dec_6rad_dn_Avg(4)') : ('', 'dec_6rad_dnlook_Avg(4)'),
    ('stats5_6rad', 'dec_6rad_dn_Avg(5)') : ('', 'dec_6rad_dnlook_Avg(5)'),
    ('stats5_6rad', 'dec_6rad_dn_Avg(6)') : ('', 'dec_6rad_dnlook_Avg(6)'),
    ('stats5_6rad', 'dec_6rad_uplook_Avg(1)') : ('', ''), # 20120627 - CURRENT
    ('stats5_6rad', 'dec_6rad_uplook_Avg(2)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_uplook_Avg(3)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_uplook_Avg(4)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_uplook_Avg(5)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_uplook_Avg(6)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_dnlook_Avg(1)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_dnlook_Avg(2)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_dnlook_Avg(3)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_dnlook_Avg(4)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_dnlook_Avg(5)') : ('', ''),
    ('stats5_6rad', 'dec_6rad_dnlook_Avg(6)') : ('', ''),
    ('stats5_6rad', 'tblcalls_Tot') : ('', ''), # add 20120824_LIND
    #######################################################################


    ########## 20120309_LIND - CURRENT #####################################
    ('tsdata', 'TIMESTAMP') : ('', ''),
    ('tsdata', 'RECORD') : ('', ''),
    ('tsdata', 'Ux') : ('', ''),
    ('tsdata', 'Uy') : ('', ''),
    ('tsdata', 'Uz') : ('', ''),
    ('tsdata', 'Ts') : ('', ''),
    ('tsdata', 'diag_sonic') : ('', ''),
    ('tsdata', 'CO2') : ('', ''),
    ('tsdata', 'H2O') : ('', ''),
    ('tsdata', 'diag_irga') : ('', ''),
    ('tsdata', 'amb_tmpr') : ('', ''),
    ('tsdata', 'amb_press') : ('', ''),
    ('tsdata', 'CO2_signal') : ('', ''),
    ('tsdata', 'H2O_signal') : ('', ''),

    ('stats30', 'TIMESTAMP') : ('', ''),
    ('stats30', 'RECORD') : ('', ''),
    ('stats30', 'L') : ('', ''),
    ('stats30', 'u_star') : ('', ''),
    ('stats30', 'tau') : ('', ''),
    ('stats30', 'Fc_wpl') : ('', ''),
    ('stats30', 'LE_wpl') : ('', ''),
    ('stats30', 'Hc') : ('', ''),
    ('stats30', 'Ts_Avg') : ('', ''),
    ('stats30', 'Ts_Std') : ('', ''),
    ('stats30', 'Tc_Avg') : ('', ''),
    ('stats30', 'Uz_Std') : ('', ''),
    ('stats30', 'wnd_spd') : ('', ''),
    ('stats30', 'rslt_wnd_spd') : ('', ''),
    ('stats30', 'rslt_wnd_dir') : ('', ''),
    ('stats30', 'std_wnd_dir') : ('', ''),
    ('stats30', 'sonic_uptime') : ('', ''), # added 20120215_CFNT
    ('stats30', 'CO2_ppm_Avg_Avg') : ('', 'CO2_ppm_Avg'), # FIX see 20131018_XXXX
    ('stats30', 'CO2_ppm_Avg') : ('', ''),
    ('stats30', 'CO2_mg_m3_Avg') : ('', ''),
    ('stats30', 'CO2_mg_m3_Std') : ('', ''),
    ('stats30', 'CO2_signal_Avg') : ('', ''),
    ('stats30', 'H2O_g_kg_Avg') : ('', ''), # added 20120215_CFNT
    ('stats30', 'H2O_g_m3_Avg') : ('', ''),
    ('stats30', 'H2O_g_m3_Std') : ('', ''),
    ('stats30', 'H2O_signal_Avg') : ('', ''),
    ('stats30', 'amb_tmpr_Avg') : ('', ''), # added 20120215_CFNT
    ('stats30', 'amb_press_Avg') : ('', ''),
    ('stats30', 'irga_uptime') : ('', ''), # added 20120215_CFNT
    ('stats30', 'T_hmp_Avg') : ('', ''),
    ('stats30', 'RH_hmp_Avg') : ('', ''),
    ('stats30', 'e_hmp_Avg') : ('', ''), # added 20120215_CFNT
    ('stats30', 'e_sat_hmp_Avg') : ('', ''), # added 20120215_CFNT
    ('stats30', 'Rn_Avg') : ('', ''),
    ('stats30', 'Rn_meas_Avg') : ('', ''),
    ('stats30', 'PAR_totflx_Tot') : ('', ''),
    ('stats30', 'PAR_flxdens_Avg') : ('', ''),
    ('stats30', 'Met1_wnd_spd') : ('', ''),
    ('stats30', 'Met1_rslt_wnd_spd') : ('', ''),
    ('stats30', 'Met1_wnd_dir') : ('', 'Met1_rslt_wnd_dir'),#20120316_CFNT
    ('stats30', 'Met1_rslt_wnd_dir') : ('', ''),
    ('stats30', 'Met1_std_wnd_dir') : ('', ''),
    ('stats30', 'Rain_mm_Tot') : ('', ''),
    ('stats30', 'soil_5TM_ID5_epsilon_Avg') : ('', 'soil_5TM_ID5_E_Avg'),#20120824_LIND
    ('stats30', 'soil_5TM_ID5_E_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID5_T_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID5_VWC_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID6_epsilon_Avg') : ('', 'soil_5TM_ID6_E_Avg'),#20120824_LIND
    ('stats30', 'soil_5TM_ID6_E_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID6_T_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID6_VWC_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID7_epsilon_Avg') : ('', 'soil_5TM_ID7_E_Avg'),#20120824_LIND
    ('stats30', 'soil_5TM_ID7_E_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID7_T_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID7_VWC_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID8_epsilon_Avg') : ('', 'soil_5TM_ID8_E_Avg'),#20120824_LIND
    ('stats30', 'soil_5TM_ID8_E_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID8_T_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID8_VWC_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID9_epsilon_Avg') : ('', 'soil_5TM_ID9_E_Avg'),#20120824_LIND
    ('stats30', 'soil_5TM_ID9_E_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID9_T_Avg') : ('', ''),
    ('stats30', 'soil_5TM_ID9_VWC_Avg') : ('', ''),
    ('stats30', 'soil_hfp1_heat_flux_Avg') : ('', ''), # 20130507_XXXX
    ('stats30', 'soil_hfp1_sensitivity') : ('', ''), # 20130507_XXXX
    ('stats30', 'soil_hfp2_heat_flux_Avg') : ('', ''), # 20130507_XXXX
    ('stats30', 'soil_hfp2_sensitivity') : ('', ''), # 20130507_XXXX
    ('stats30', 'panel_tmpr_Avg') : ('', ''),
    ('stats30', 'batt_volt_Avg') : ('', ''),

    ('stats5', 'TIMESTAMP') : ('', ''),
    ('stats5', 'RECORD') : ('', ''),
    ('stats5', 'L') : ('', ''),
    ('stats5', 'u_star') : ('', ''),
    ('stats5', 'tau') : ('', ''),
    ('stats5', 'Fc_wpl') : ('', ''),
    ('stats5', 'LE_wpl') : ('', ''),
    ('stats5', 'Hc') : ('', ''),
    ('stats5', 'Ts_Avg') : ('', ''),
    ('stats5', 'Ts_Std') : ('', ''),
    ('stats5', 'Tc_Avg') : ('', ''),
    ('stats5', 'Uz_Std') : ('', ''),
    ('stats5', 'wnd_spd') : ('', ''),
    ('stats5', 'rslt_wnd_spd') : ('', ''),
    ('stats5', 'rslt_wnd_dir') : ('', ''),
    ('stats5', 'std_wnd_dir') : ('', ''),
    ('stats5', 'sonic_uptime') : ('', ''), # added 20120215_CFNT
    ('stats5', 'CO2_ppm_Avg_Avg') : ('', 'CO2_ppm_Avg'), # FIX see 20131018_XXXX
    ('stats5', 'CO2_ppm_Avg') : ('', ''),
    ('stats5', 'CO2_mg_m3_Avg') : ('', ''),
    ('stats5', 'CO2_mg_m3_Std') : ('', ''),
    ('stats5', 'CO2_signal_Avg') : ('', ''),
    ('stats5', 'H2O_g_kg_Avg') : ('', ''), # added 20120215_CFNT
    ('stats5', 'H2O_g_m3_Avg') : ('', ''),
    ('stats5', 'H2O_g_m3_Std') : ('', ''),
    ('stats5', 'H2O_signal_Avg') : ('', ''),
    ('stats5', 'amb_tmpr_Avg') : ('', ''), # added 20120215_CFNT
    ('stats5', 'amb_press_Avg') : ('', ''),
    ('stats5', 'irga_uptime') : ('', ''), # added 20120215_CFNT
    ('stats5', 'T_hmp_Avg') : ('', ''),
    ('stats5', 'RH_hmp_Avg') : ('', ''),
    ('stats5', 'e_hmp_Avg') : ('', ''), # added 20120215_CFNT
    ('stats5', 'e_sat_hmp_Avg') : ('', ''), # added 20120215_CFNT
    ('stats5', 'Rn_Avg') : ('', ''),
    ('stats5', 'Rn_meas_Avg') : ('', ''),
    ('stats5', 'PAR_totflx_Tot') : ('', ''),
    ('stats5', 'PAR_flxdens_Avg') : ('', ''),
    ('stats5', 'Met1_wnd_spd') : ('', ''),
    ('stats5', 'Met1_rslt_wnd_spd') : ('', ''),
    ('stats5', 'Met1_wnd_dir') : ('', 'Met1_rslt_wnd_dir'),#20120316_CFNT
    ('stats5', 'Met1_rslt_wnd_dir') : ('', ''),
    ('stats5', 'Met1_std_wnd_dir') : ('', ''),
    ('stats5', 'Rain_mm_Tot') : ('', ''),
    ('stats5', 'soil_5TM_ID5_epsilon_Avg') : ('', 'soil_5TM_ID5_E_Avg'),#20120824_LIND
    ('stats5', 'soil_5TM_ID5_E_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID5_T_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID5_VWC_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID6_epsilon_Avg') : ('', 'soil_5TM_ID6_E_Avg'),#20120824_LIND
    ('stats5', 'soil_5TM_ID6_E_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID6_T_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID6_VWC_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID7_epsilon_Avg') : ('', 'soil_5TM_ID7_E_Avg'),#20120824_LIND
    ('stats5', 'soil_5TM_ID7_E_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID7_T_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID7_VWC_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID8_epsilon_Avg') : ('', 'soil_5TM_ID8_E_Avg'),#20120824_LIND
    ('stats5', 'soil_5TM_ID8_E_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID8_T_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID8_VWC_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID9_epsilon_Avg') : ('', 'soil_5TM_ID9_E_Avg'),#20120824_LIND
    ('stats5', 'soil_5TM_ID9_E_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID9_T_Avg') : ('', ''),
    ('stats5', 'soil_5TM_ID9_VWC_Avg') : ('', ''),
    ('stats5', 'soil_hfp1_heat_flux_Avg') : ('', ''), # 20130507_XXXX
    ('stats5', 'soil_hfp1_sensitivity') : ('', ''), # 20130507_XXXX
    ('stats5', 'soil_hfp2_heat_flux_Avg') : ('', ''), # 20130507_XXXX
    ('stats5', 'soil_hfp2_sensitivity') : ('', ''), # 20130507_XXXX
    ('stats5', 'panel_tmpr_Avg') : ('', ''),
    ('stats5', 'batt_volt_Avg') : ('', ''),

    ('site_daily', 'TIMESTAMP') : ('', ''),
    ('site_daily', 'RECORD') : ('', ''),
    # TODO should some daily values be moved into to site_info table?
    ('site_daily', 'sonic_azimuth') : (None, None), # 20120627_CFCT
    ('site_daily', 'latitude') : ('', 'latitude_Med'), #20120323_LIND
    ('site_daily', 'latitude_Med') : ('', ''),
    ('site_daily', 'latitude_a_Avg') : (None, None), # 20120316_CFNT
    ('site_daily', 'latitude_b_Avg') : (None, None), # 20120316_CFNT
    ('site_daily', 'longitude') : ('', 'longitude_Med'), # 20120323_LIND
    ('site_daily', 'longitude_Med') : ('', ''),
    ('site_daily', 'longitude_a_Avg') : (None, None), # 20120316_CFNT
    ('site_daily', 'longitude_b_Avg') : (None, None), # 20120316_CFNT
    ('site_daily', 'magnetic_variation_Med') : ('', ''), # add 20120309_LIND
    ('site_daily', 'magnetic_variation_Avg') : (None, None), # 20120323_LIND
    ('site_daily', 'nmbr_satellites_Avg') : ('', ''),
    ('site_daily', 'altitude_Med') : ('', ''), # add 20120309_LIND
    ('site_daily', 'altitude_Avg') : ('', ''),
    ('site_daily', 'gps_ready_Min') : ('', ''),
    ('site_daily', 'max_clock_change') : ('', ''),
    ('site_daily', 'nmbr_clock_change') : ('', ''),
    # TODO figure out how to integrate disparate time-base variables like runsig
    ('site_daily', 'RunSig') : (None, None), # 20120627_CFCT
    ('site_daily', 'ProgSig') : (None, None), # 20120627_CFCT
    ('site_daily', 'batt_volt_Min') : ('', ''), # add 20130507_XXXX
    ('site_daily', 'batt_volt_TMn') : ('', ''), #
    ('site_daily', 'batt_volt_Max') : ('', ''), #
    ('site_daily', 'batt_volt_TMx') : ('', ''), #  ..
    ('site_daily', 'T_hmp_Min') : ('', ''), #
    ('site_daily', 'T_hmp_TMn') : ('', ''), #
    ('site_daily', 'T_hmp_Max') : ('', ''), #
    ('site_daily', 'T_hmp_TMx') : ('', ''), # add 20130507_XXXX
    ########################################################################


    ########## 20120330_CFNT - 20120727_LIND ###############################
    ('stats30_soil', 'TIMESTAMP') : (None, None), # merged into stats30 table
    ('stats30_soil', 'RECORD') : (None, None),
    ('stats30_soil', 'soil_5TM_ID5_epsilon_Avg') : ('stats30', ''), #20120810_LIND
    ('stats30_soil', 'soil_5TM_ID5_T_Avg') : ('stats30', ''), # all are affected
    ('stats30_soil', 'soil_5TM_ID5_VWC_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID6_epsilon_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID6_T_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID6_VWC_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID7_epsilon_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID7_T_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID7_VWC_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID8_epsilon_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID8_T_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID8_VWC_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID9_epsilon_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID9_T_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_5TM_ID9_VWC_Avg') : ('stats30', ''),
    ('stats30_soil', 'soil_hfp1_heat_flux_Avg') : ('stats30_hfp', ''),
    ('stats30_soil', 'soil_hfp1_sensitivity') : ('stats30_hfp', ''),
    ('stats30_soil', 'soil_hfp2_heat_flux_Avg') : ('stats30_hfp', ''),
    ('stats30_soil', 'soil_hfp2_sensitivity') : ('stats30_hfp', ''),

    ('stats5_soil', 'TIMESTAMP') : (None, None), # merged into stats5 table
    ('stats5_soil', 'RECORD') : (None, None),
    ('stats5_soil', 'soil_5TM_ID5_epsilon_Avg') : ('stats5', ''), #20120810_LIND
    ('stats5_soil', 'soil_5TM_ID5_T_Avg') : ('stats5', ''), # all are affected
    ('stats5_soil', 'soil_5TM_ID5_VWC_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID6_epsilon_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID6_T_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID6_VWC_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID7_epsilon_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID7_T_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID7_VWC_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID8_epsilon_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID8_T_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID8_VWC_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID9_epsilon_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID9_T_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_5TM_ID9_VWC_Avg') : ('stats5', ''),
    ('stats5_soil', 'soil_hfp1_heat_flux_Avg') : ('stats5_hfp', ''),
    ('stats5_soil', 'soil_hfp1_sensitivity') : ('stats5_hfp', ''),
    ('stats5_soil', 'soil_hfp2_heat_flux_Avg') : ('stats5_hfp', ''),
    ('stats5_soil', 'soil_hfp2_sensitivity') : ('stats5_hfp', ''),
    #########################################################################


    ###### 20120504_LIND, 20120628_CFNT and 20120720_CFNT #################
    # TODO what to do with LGR data?
    ('tsdata_n2o_co', 'TIMESTAMP') : (None, None),
    ('tsdata_n2o_co', 'RECORD') : (None, None),
    ('tsdata_n2o_co', 'lgr_n2o') : ('tsdata_extra', 'lgr_n2o'),
    ('tsdata_n2o_co', 'lgr_co') : ('tsdata_extra', 'lgr_co'),

    ('stats30_n2o_co', 'TIMESTAMP') : ('', ''),
    ('stats30_n2o_co', 'RECORD') : ('', ''),
    ('stats30_n2o_co', 'lgr_n2o_Avg') : ('', ''),
    ('stats30_n2o_co', 'lgr_n2o_Std') : ('', ''),
    ('stats30_n2o_co', 'lgr_co_Avg') : ('', ''),
    ('stats30_n2o_co', 'lgr_co_Std') : ('', ''),
    ('stats30_n2o_co', 'lgr_uptime') : ('', 'lgr_n2o_co_uptime'),
    ('stats30_n2o_co', 'cov_n2o_Uz') : (None, None), # cfnt update
    ('stats30_n2o_co', 'cov_co_Uz') : (None, None), # cfnt update
    ('stats30_n2o_co', 'lgr_n2o_co_uptime') : ('', ''), # cfnt update

    ('stats5_n2o_co', 'TIMESTAMP') : ('', ''),
    ('stats5_n2o_co', 'RECORD') : ('', ''),
    ('stats5_n2o_co', 'lgr_n2o_Avg') : ('', ''),
    ('stats5_n2o_co', 'lgr_n2o_Std') : ('', ''),
    ('stats5_n2o_co', 'lgr_co_Avg') : ('', ''),
    ('stats5_n2o_co', 'lgr_co_Std') : ('', ''),
    ('stats5_n2o_co', 'lgr_uptime') : ('', 'lgr_n2o_co_uptime'),
    ('stats5_n2o_co', 'cov_n2o_Uz') : (None, None), # cfnt update
    ('stats5_n2o_co', 'cov_co_Uz') : (None, None), # cfnt update
    ('stats5_n2o_co', 'lgr_n2o_co_uptime') : ('', ''), # cfnt update
    ########################################################################


    ########## 20120420_LIND only ##########################################
    ## re-adopted this format with 20130507_XXXX
    #('tsdata_extra', 'TIMESTAMP') : ('tsdata_n2o_co', ''),
    #('tsdata_extra', 'RECORD') : ('tsdata_n2o_co', ''),
    #('tsdata_extra', 'lgr_n2o') : ('tsdata_n2o_co', ''),
    #('tsdata_extra', 'lgr_co') : ('tsdata_n2o_co', ''),
    ##

    ('stats30_extra', 'TIMESTAMP') : ('stats30_n2o_co', ''),
    ('stats30_extra', 'RECORD') : ('stats30_n2o_co', ''),
    ('stats30_extra', 'lgr_n2o_Avg') : ('stats30_n2o_co', ''),
    ('stats30_extra', 'lgr_n2o_Std') : ('stats30_n2o_co', ''),
    ('stats30_extra', 'lgr_co_Avg') : ('stats30_n2o_co', ''),
    ('stats30_extra', 'lgr_co_Std') : ('stats30_n2o_co', ''),
    ('stats30_extra', 'lgr_uptime') : ('stats30_n2o_co', ''),
    ('stats30_extra', 'cov_n2o_Uz') : ('stats30_n2o_co', ''), # 20120504_LIND
    ('stats30_extra', 'cov_co_Uz') : ('stats30_n2o_co', ''), # 20120504_LIND

    ('stats5_extra', 'TIMESTAMP') : ('stats5_n2o_co', ''),
    ('stats5_extra', 'RECORD') : ('stats5_n2o_co', ''),
    ('stats5_extra', 'lgr_n2o_Avg') : ('stats5_n2o_co', ''),
    ('stats5_extra', 'lgr_n2o_Std') : ('stats5_n2o_co', ''),
    ('stats5_extra', 'lgr_co_Avg') : ('stats5_n2o_co', ''),
    ('stats5_extra', 'lgr_co_Std') : ('stats5_n2o_co', ''),
    ('stats5_extra', 'lgr_uptime') : ('stats5_n2o_co', ''),
    ('stats5_extra', 'cov_n2o_Uz') : ('stats5_n2o_co', ''), # 20120504_LIND
    ('stats5_extra', 'cov_co_Uz') : ('stats5_n2o_co', ''), # 20120504_LIND
    ########################################################################


    ########## 20120210_CFNT - 20120309_CFNT ###############################
    ('CFNT_tsdata', 'TIMESTAMP') : ('tsdata', ''), #20120309_LIND
    ('CFNT_tsdata', 'RECORD') : ('tsdata', ''), #20120309_LIND
    ('CFNT_tsdata', 'Ux') : ('tsdata', ''), #20120309_LIND
    ('CFNT_tsdata', 'Uy') : ('tsdata', ''), #20120309_LIND
    ('CFNT_tsdata', 'Uz') : ('tsdata', ''), #20120309_LIND
    ('CFNT_tsdata', 'Ts') : ('tsdata', ''), #20120309_LIND
    ('CFNT_tsdata', 'diag_sonic') : ('tsdata', ''), #20120309_LIND
    ('CFNT_tsdata', 'CO2') : ('tsdata', ''),  #20120309_LIND
    ('CFNT_tsdata', 'H2O') : ('tsdata', ''), #20120309_LIND
    ('CFNT_tsdata', 'diag_irga') : ('tsdata', ''),  #20120309_LIND
    ('CFNT_tsdata', 'amb_tmpr') : ('tsdata', ''),  #20120309_LIND
    ('CFNT_tsdata', 'amb_press') : ('tsdata', ''),  #20120309_LIND
    ('CFNT_tsdata', 'CO2_signal') : ('tsdata', ''),  #20120309_LIND
    ('CFNT_tsdata', 'H2O_signal') : ('tsdata', ''), #20120309_LIND

    ('CFNT_stats30', 'TIMESTAMP') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'RECORD') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'L') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'u_star') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'tau') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Fc_wpl') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'LE_wpl') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Hc') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Ts_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Ts_Std') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Tc_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Tc_Std') : (None, None), # 20120215_CFNT
    ('CFNT_stats30', 'Uz_Std') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'wnd_spd') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'rslt_wnd_spd') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'std_wnd_dir') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'wnd_dir_compass') : ('stats30', 'rslt_wnd_dir'),#20120309_LIND
    ('CFNT_stats30', 'sonic_samples_Tot') : (None, None), # 20120215_CFNT
    ('CFNT_stats30', 'sonic_uptime') : ('stats30', ''), # added 20120215_CFNT
    ('CFNT_stats30', 'CO2_ppm_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'CO2_ppm_Std') : (None, None), # only in 20120224_LIND
    ('CFNT_stats30', 'CO2_mg_m3_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'CO2_mg_m3_Std') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'CO2_signal_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'H2O_g_kg_Avg') : ('stats30', ''), # added 20120215_CFNT
    ('CFNT_stats30', 'H2O_g_kg_Std') : (None, None), # only in 20120224_LIND
    ('CFNT_stats30', 'H2O_g_m3_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'H2O_g_m3_Std') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'H2O_signal_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'amb_tmpr_Avg') : ('stats30', ''), # added 20120215_CFNT
    ('CFNT_stats30', 'amb_press_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'irga_samples_Tot') : (None, None), # only in 20120224_LIND
    ('CFNT_stats30', 'irga_uptime') : ('stats30', ''), # added 20120215_CFNT
    ('CFNT_stats30', 'T_hmp_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'RH_hmp_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'e_hmp_Avg') : ('stats30', ''), # added 20120215_CFNT
    ('CFNT_stats30', 'e_sat_hmp_Avg') : ('stats30', ''), # added 20120215_CFNT
    ('CFNT_stats30', 'hmp_uptime') : (None, None), # only in 20120224_LIND
    ('CFNT_stats30', 'Rn_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Rn_meas_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'PAR_totflx_Tot') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'PAR_flxdens_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Met1_wnd_spd') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Met1_rslt_wnd_spd') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Met1_wnd_dir') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Met1_std_wnd_dir') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'Met1_uptime') : (None, None), # only in 20120224_LIND
    ('CFNT_stats30', 'Rain_mm_Tot') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'panel_tmpr_Avg') : ('stats30', ''), #20120309_LIND
    ('CFNT_stats30', 'batt_volt_Avg') : ('stats30', ''), #20120309_LIND

    ('CFNT_stats5', 'TIMESTAMP') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'RECORD') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'L') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'u_star') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'tau') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Fc_wpl') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'LE_wpl') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Hc') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Ts_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Ts_Std') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Tc_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Tc_Std') : (None, None), # 20120215_CFNT
    ('CFNT_stats5', 'Uz_Std') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'wnd_spd') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'rslt_wnd_spd') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'std_wnd_dir') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'wnd_dir_compass') : ('stats5', 'rslt_wnd_spd'), #20120309_LIND
    ('CFNT_stats5', 'sonic_samples_Tot') : (None, None), # 20120215_CFNT
    ('CFNT_stats5', 'sonic_uptime') : ('stats5', ''), # added 20120215_CFNT
    ('CFNT_stats5', 'CO2_ppm_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'CO2_ppm_Std') : (None, None), # only in 20120224_LIND
    ('CFNT_stats5', 'CO2_mg_m3_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'CO2_mg_m3_Std') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'CO2_signal_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'H2O_g_kg_Avg') : ('stats5', ''), # added 20120215_CFNT
    ('CFNT_stats5', 'H2O_g_kg_Std') : (None, None), # only in 20120224_LIND
    ('CFNT_stats5', 'H2O_g_m3_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'H2O_g_m3_Std') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'H2O_signal_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'amb_tmpr_Avg') : ('stats5', ''), # added 20120215_CFNT
    ('CFNT_stats5', 'amb_press_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'irga_samples_Tot') : (None, None), # only in 20120224_LIND
    ('CFNT_stats5', 'irga_uptime') : ('stats5', ''), # added 20120215_CFNT
    ('CFNT_stats5', 'T_hmp_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'RH_hmp_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'e_hmp_Avg') : ('stats5', ''), # added 20120215_CFNT
    ('CFNT_stats5', 'e_sat_hmp_Avg') : ('stats5', ''), # added 20120215_CFNT
    ('CFNT_stats5', 'hmp_uptime') : (None, None), # only in 20120224_LIND
    ('CFNT_stats5', 'Rn_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Rn_meas_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'PAR_totflx_Tot') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'PAR_flxdens_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Met1_wnd_spd') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Met1_rslt_wnd_spd') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Met1_wnd_dir') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Met1_std_wnd_dir') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'Met1_uptime') : (None, None), # only in 20120224_LIND
    ('CFNT_stats5', 'Rain_mm_Tot') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'panel_tmpr_Avg') : ('stats5', ''), #20120309_LIND
    ('CFNT_stats5', 'batt_volt_Avg') : ('stats5', ''), #20120309_LIND

    ('CFNT_site_daily', 'TIMESTAMP') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'RECORD') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'sonic_azimuth') : ('site_daily', ''), #add 20120215_CFNT
    ('CFNT_site_daily', 'latitude_a_Avg') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'latitude_b_Avg') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'longitude_a_Avg') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'longitude_b_Avg') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'latitude_a_Std') : (None, None), # 20120215_CFNT
    ('CFNT_site_daily', 'latitude_b_Std') : (None, None), # 20120215_CFNT
    ('CFNT_site_daily', 'longitude_a_Std') : (None, None), # 20120215_CFNT
    ('CFNT_site_daily', 'longitude_b_Std') : (None, None), # 20120215_CFNT
    ('CFNT_site_daily', 'magnetic_variation_Avg') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'magnetic_variation_Std') : (None, None), # 20120215_CFNT
    ('CFNT_site_daily', 'nmbr_satellites_Avg') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'altitude_Avg') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'altitude_Std') : (None, None), # 20120215_CFNT
    ('CFNT_site_daily', 'gps_ready_Min') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'max_clock_change') : ('site_daily', ''), #20120309_LIND
    ('CFNT_site_daily', 'nmbr_clock_change') : ('site_daily', ''), #20120309_LIND
    ########################################################################


    ########## 20120204_CFNT only ###########################################
    ('CFNT_met_5min', 'TIMESTAMP') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'RECORD') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'L') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'Hs') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'u_star') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'Ts_stdev') : ('CFNT_stats5', 'Ts_Std'), # 20120210_CFNT
    ('CFNT_met_5min', 'Uz_stdev') : ('CFNT_stats5', 'Uz_Std'), # 20120210_CFNT
    ('CFNT_met_5min', 'wnd_spd') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'rslt_wnd_spd') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'std_wnd_dir') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'wnd_dir_compass') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'Ts_Avg') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'sonic_samples_Tot') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'Fc_wpl') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'LE_wpl') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'Hc') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'CO2_stdev') : ('CFNT_stats5', 'CO2_mg_m3_Std'), # 20120210_CFNT
    ('CFNT_met_5min', 'H2O_stdev') : ('CFNT_stats5', 'H2O_g_m3_Std'), # 20120210_CFNT
    ('CFNT_met_5min', 'Tc_stdev') : ('CFNT_stats5', 'Tc_Std'), # 20120210_CFNT
    ('CFNT_met_5min', 'CO2_mean') : ('CFNT_stats5', 'CO2_mg_m3_Avg'), # 20120210_CFNT
    ('CFNT_met_5min', 'H2O_mean') : ('CFNT_stats5', 'H2O_g_m3_Avg'), # 20120210_CFNT
    ('CFNT_met_5min', 'amb_press_mean') : ('CFNT_stats5', 'amb_press_Avg'), # 20120210_CFNT
    ('CFNT_met_5min', 'Tc_mean') : ('CFNT_stats5', 'Tc_Avg'), # 20120210_CFNT
    ('CFNT_met_5min', 'CO2_sig_strgth_mean') : ('CFNT_stats5', 'CO2_signal_Avg'), # 20120210_CFNT
    ('CFNT_met_5min', 'H2O_sig_strgth_mean') : ('CFNT_stats5', 'H2O_signal_Avg'), # 20120210_CFNT
    ('CFNT_met_5min', 'T_hmp_mean') : ('CFNT_stats5', 'T_hmp_Avg'), # 20120210_CFNT
    ('CFNT_met_5min', 'RH_hmp_mean') : ('CFNT_stats5', 'RH_hmp_Avg'), # 20120210_CFNT
    ('CFNT_met_5min', 'Rn_Avg') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'Rn_meas_Avg') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'PAR_totflx_Tot') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'PAR_flxdens_Avg') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', '034b_ws2') : ('CFNT_stats5', 'Met1_wnd_spd'), # 20120210_CFNT
    ('CFNT_met_5min', '034b_wd2') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', '034b_stdwd2') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'Rain_mm_Tot') : ('CFNT_stats5', ''), # 20120210_CFNT
    # TODO should this 5min GPS data be merged into site daily files?
    ('CFNT_met_5min', 'latitude_a') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'latitude_b') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'longitude_a') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'longitude_b') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'magnetic_variation') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'altitude') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'max_clock_change') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'nmbr_clock_change') : (None, None), # 20120210_CFNT
    ('CFNT_met_5min', 'panel_tmpr_Avg') : ('CFNT_stats5', ''), # 20120210_CFNT
    ('CFNT_met_5min', 'batt_volt_Avg') : ('CFNT_stats5', ''), # 20120210_CFNT
    #########################################################################


    ########## 20111024_CFNT to 20111027_CFNT ###############################
    ('extra_gas', 'TIMESTAMP') : (None, None),
    ('extra_gas', 'RECORD') : (None, None),
    ('extra_gas', 'n2o_Avg') : (None, None),
    ('extra_gas', 'c_monoxide_Avg') : (None, None),
    ('extra_gas', 'ch4_Avg') : (None, None),
    ('extra_gas', 'co2_picarro_Avg') : (None, None),
    ('extra_gas', 'co2_mix_ratio_Avg') : (None, None),
    ('extra_gas', 'h2o_mix_ratio_Avg') : (None, None),
    ('extra_gas', '034b_ws') : (None, None), # add 20111027
    ('extra_gas', '034b_wd') : (None, None), # add 20111027
    ('extra_gas', '034b_stdwd') : (None, None), # add 20111027
    #########################################################################


    ########## 20110819_CFNT to 20120204_CFNT ###############################
    ('ts_data', 'TIMESTAMP') : ('CFNT_tsdata', ''), # 20120210_CFNT
    ('ts_data', 'RECORD') : ('CFNT_tsdata', ''), # 20120210_CFNT
    ('ts_data', 'Ux') : ('CFNT_tsdata', ''), # 20120210_CFNT
    ('ts_data', 'Uy') : ('CFNT_tsdata', ''), # 20120210_CFNT
    ('ts_data', 'Uz') : ('CFNT_tsdata', ''), # 20120210_CFNT
    ('ts_data', 'Ts') : ('CFNT_tsdata', ''), # 20120210_CFNT
    ('ts_data', 'diag_sonic') : ('CFNT_tsdata', ''), # 20120210_CFNT
    ('ts_data', 'atiUx') : (None, None), # 20111009_CFNT
    ('ts_data', 'atiUy') : (None, None), # 20111009_CFNT
    ('ts_data', 'atiUz') : (None, None), # 20111009_CFNT
    ('ts_data', 'atiTs') : (None, None), # 20111009_CFNT
    ('ts_data', 'CO2_molar') : ('', 'co2_molar_density'), # 20111009_CFNT
    ('ts_data', 'H2O_molar') : ('', 'h2o_molar_density'), # 20111009_CFNT
    ('ts_data', 'co2_molar_density') : (None, None), # 20120210_CFNT
    ('ts_data', 'h2o_molar_density') : (None, None), # 20120210_CFNT
    ('ts_data', 'co2_mix_ratio') : ('', 'co2_molar_density'), # 20111024_CFNT
    ('ts_data', 'h2o_mix_ratio') : ('', 'h2o_molar_density'), # 20111024_CFNT
    ('ts_data', 'n2o') : ('tsdata_extra', 'lgr_n2o'), # 20111024_CFNT
    ('ts_data', 'c_monoxide') : ('tsdata_extra', 'lgr_co'), # 20111024_CFNT
    ('ts_data', 'ch4') : ('tsdata_extra', 'pic_ch4'), # 20111024_CFNT
    ('ts_data', 'co2_picarro') : ('tsdata_extra', 'pic_co2'), # 20111024_CFNT
    ('ts_data', 'CO2') : ('CFNT_tsdata', ''),  # 20120210_CFNT
    ('ts_data', 'H2O') : ('CFNT_tsdata', ''), # 20120210_CFNT
    ('ts_data', 'diag_irga') : ('CFNT_tsdata', ''),  # 20120210_CFNT
    ('ts_data', 'amb_tmpr') : ('CFNT_tsdata', ''),  # 20120210_CFNT
    ('ts_data', 'amb_press') : ('CFNT_tsdata', ''),  # 20120210_CFNT
    ('ts_data', 'CO2_sig_strgth') : ('CFNT_tsdata', 'CO2_signal'), # 20120210_CFNT
    ('ts_data', 'H2O_sig_strgth') : ('CFNT_tsdata', 'H2O_signal'), # 20120210_CFNT

    ('flux', 'TIMESTAMP') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'RECORD') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'l') : ('', 'L'), # 20120204_CFNT
    ('flux', 'L') : ('CFNT_stats30', ''), # 20120204_CFNT
    ('flux', 'Hs') : (None, None), # 20120210_CFNT
    ('flux', 'tau') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'u_star') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'Ts_stdev') : ('CFNT_stats30', 'Ts_Std'), # 20120210_CFNT
    ('flux', 'Ts_Ux_cov') : (None, None), # 20120210_CFNT
    ('flux', 'Ts_Uy_cov') : (None, None), # 20120210_CFNT
    ('flux', 'Ts_Uz_cov') : (None, None), # 20120210_CFNT
    ('flux', 'Ux_stdev') : (None, None), # 20120210_CFNT
    ('flux', 'Ux_Uy_cov') : (None, None), # 20120210_CFNT
    ('flux', 'Ux_Uz_cov') : (None, None), # 20120210_CFNT
    ('flux', 'Uy_stdev') : (None, None), # 20120210_CFNT
    ('flux', 'Uy_Uz_cov') : (None, None), # 20120210_CFNT
    ('flux', 'Uz_stdev') : ('CFNT_stats30', 'Uz_Std'), # 20120210_CFNT
    ('flux', 'wnd_spd') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'rslt_wnd_spd') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'wnd_dir_sonic') : (None, None), # 20120210_CFNT
    ('flux', 'std_wnd_dir') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'wnd_dir_compass') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'Ux_Avg') : (None, None), # 20120210_CFNT
    ('flux', 'Uy_Avg') : (None, None), # 20120210_CFNT
    ('flux', 'Uz_Avg') : (None, None), # 20120210_CFNT
    ('flux', 'Ts_Avg') : ('CFNT_stats30', ''),  # 20120210_CFNT
    ('flux', 'sonic_azimuth') : (None, None), # 20120210_CFNT
    ('flux', 'sonic_samples_Tot') : (None, None), # 20120215_CFNT
    ('flux', 'no_sonic_head_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'no_new_sonic_data_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'amp_l_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'amp_h_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'sig_lck_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'del_T_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'aq_sig_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'sonic_cal_err_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'Fc_wpl') : ('CFNT_stats30', ''),  # 20120210_CFNT
    ('flux', 'LE_wpl') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'Hc') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'CO2_stdev') : ('CFNT_stats30', 'CO2_mg_m3_Std'), # 20120210_CFNT
    ('flux', 'CO2_Ux_cov') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_Uy_cov') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_Uz_cov') : (None, None), # 20120210_CFNT
    ('flux', 'H2O_stdev') : ('CFNT_stats30', 'H2O_g_m3_Std'), # 20120210_CFNT
    ('flux', 'H2O_Ux_cov') : (None, None), # 20120210_CFNT
    ('flux', 'H2O_Uy_cov') : (None, None), # 20120210_CFNT
    ('flux', 'H2O_Uz_cov') : (None, None), # 20120210_CFNT
    ('flux', 'Tc_stdev') : ('CFNT_stats30', 'Tc_Std'), # 20120210_CFNT
    ('flux', 'Tc_Ux_cov') : (None, None), # 20120210_CFNT
    ('flux', 'Tc_Uy_cov') : (None, None), # 20120210_CFNT
    ('flux', 'Tc_Uz_cov') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_mean') : ('CFNT_stats30', 'CO2_mg_m3_Avg'), # 20120210_CFNT
    ('flux', 'H2O_mean') : ('CFNT_stats30', 'H2O_g_m3_Avg'), # 20120210_CFNT
    ('flux', 'amb_press_mean') : ('CFNT_stats30', 'amb_press_Avg'), # 20120210_CFNT
    ('flux', 'Tc_mean') : ('CFNT_stats30', 'Tc_Avg'), # 20120210_CFNT
    ('flux', 'rho_a_mean') : (None, None), # 20120210_CFNT
    ('flux', 'Fc_irga') : (None, None), # 20120210_CFNT
    ('flux', 'LE_irga') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_wpl_LE') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_wpl_H') : (None, None), # 20120210_CFNT
    ('flux', 'H2O_wpl_LE') : (None, None), # 20120210_CFNT
    ('flux', 'H2O_wpl_H') : (None, None), # 20120210_CFNT
    ('flux', 'irga_samples_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'no_irga_head_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'no_new_irga_data_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'irga_bad_data_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'gen_sys_fault_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'sys_startup_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'motor_spd_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'tec_tmpr_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'src_pwr_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'src_tmpr_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'src_curr_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'irga_off_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'irga_sync_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'amb_tmpr_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'amb_press_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_I_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_Io_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'H2O_I_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'H2O_Io_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_Io_var_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'H2O_Io_var_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_sig_strgth_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'H2O_sig_strgth_f_Tot') : (None, None), # 20120210_CFNT
    ('flux', 'CO2_sig_strgth_mean') : ('CFNT_stats30', 'CO2_signal_Avg'), # 20120210_CFNT
    ('flux', 'H2O_sig_strgth_mean') : ('CFNT_stats30', 'H2O_signal_Avg'), # 20120210_CFNT
    ('flux', 'T_hmp_mean') : ('CFNT_stats30', 'T_hmp_Avg'), # 20120210_CFNT
    #####('flux', 'e_hmp_mean') : (None, None), # 20120210_CFNT #####
    ('flux', 'e_hmp_mean') : ('CFNT_stats30', 'e_hmp_Avg'), # 20120215_CFNT
    #####('flux', 'e_sat_hmp_mean') : (None, None), # 20120210_CFNT #####
    ('flux', 'e_sat_hmp_mean') : ('CFNT_stats30', 'e_sat_hmp_Avg'), # 2012015_CFNT
    ('flux', 'H2O_hmp_mean') : (None, None), # 20120210_CFNT
    ('flux', 'RH_hmp_mean') : ('CFNT_stats30', 'RH_hmp_Avg'), # 20120210_CFNT
    ('flux', 'rho_a_mean_hmp') : (None, None), # 20120210_CFNT
    ('flux', 'Rn_Avg') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'Rn_meas_Avg') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'par_totflx_Tot') : ('', 'PAR_totflx_Tot'), # 20120204_CFNT
    ('flux', 'PAR_totflx_Tot') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'par_flxdens_Avg') : ('', 'PAR_flxdens_Avg'), # 20120204_CFNT
    ('flux', 'PAR_flxdens_Avg') : ('CFNT_stats30', ''), # 20120210_CFNT
    ('flux', 'WS_ms_Avg') : (None, None), # 20111011_CFNT
    ('flux', 'WS_ms_WVc(1)') : ('', '034b_ws'), # 20111011_CFNT
    ('flux', '034b_ws') : ('CFNT_stats30', 'Met1_wnd_spd'), # 20120210_CFNT
    ('flux', 'WS_ms_WVc(2)') : ('', '034b_wd'), # 20111011_CFNT
    ('flux', '034b_wd') : (None, None), # 20120210_CFNT
    ('flux', 'WS_ms_WVc(3)') : ('', '034b_stdwd'), # 20111011_CFNT
    ('flux', '034b_stdwd') : (None, None), # 20120210_CFNT
    ('flux', 'ati_azimuth') : (None, None), # found in CFNT_20110923.1655_flux.dat
    ('flux', 'ati_ws') : (None, None), # found in CFNT_20110923.1655_flux.dat
    ('flux', 'ati_wd') : (None, None), # found in CFNT_20110923.1655_flux.dat
    ('flux', 'ati_stdwd') : (None, None), # found in CFNT_20110923.1655_flux.dat
    ('flux', 'atiUavg') : (None, None), # found in CFNT_20110923.1655_flux.dat
    ('flux', 'atiVavg') : (None, None), # found in CFNT_20110923.1655_flux.dat)
    ('flux', 'atiWavg') : (None, None), # found in CFNT_20110923.1655_flux.dat
    ('flux', 'atitmpavg') : (None, None), # found in CFNT_20110923.1655_flux.dat
    ('flux', 'Rain_mm_Tot') : ('CFNT_stats30', ''),  # 20120210_CFNT
    # TODO should this 30min GPS data be merged into daily files?
    ('flux', 'latitude_a') : (None, None), # 20120210_CFNT
    ('flux', 'latitude_b') : (None, None), # 20120210_CFNT
    ('flux', 'longitude_a') : (None, None), # 20120210_CFNT
    ('flux', 'longitude_b') : (None, None), # 20120210_CFNT
    ('flux', 'speed') : (None, None), # 20120210_CFNT
    ('flux', 'course') : (None, None), # 20120210_CFNT
    ('flux', 'magnetic_variation') : (None, None), # 20120210_CFNT
    ('flux', 'fix_quality') : (None, None), # 20120210_CFNT
    ('flux', 'nmbr_satellites') : (None, None), # 20120210_CFNT
    ('flux', 'altitude') : (None, None), # 20120210_CFNT
    ('flux', 'pps') : (None, None), # 20120210_CFNT
    ('flux', 'dt_since_gprmc') : (None, None), # 20120210_CFNT
    ('flux', 'gps_ready') : (None, None), # 20120210_CFNT
    ('flux', 'max_clock_change') : (None, None), # 20120210_CFNT
    ('flux', 'nmbr_clock_change') : (None, None), # 20120210_CFNT
    ('flux', 'panel_tmpr_Avg') : ('CFNT_stats30', ''),  # 20120210_CFNT
    ('flux', 'batt_volt_Avg') : ('CFNT_stats30', ''),  # 20120210_CFNT
    ('flux', 'slowsequence_Tot') : (None, None), # 20120210_CFNT
    #########################################################################
}
"""Column name aliases

Dictionary keys & values are tuples of the form (table name, column name).
All current/prior table and column pairs are described by keys; superceding
pairs are described by the values. A value tuple of (None, None) indicates
the column is no longer recorded in data tables. A blank string in the value
tuple means that item in the tuple is the same as the corresponding item in
the key (ie remains unchanged).

For example:

    col_alias[('flux', 'gps_ready')] => (None, None) # no longer recorded

    col_alias[('flux', 'WS_ms_WVc(1)')] => ('', '034b_ws') --->
    col_alias[('flux', '034b_ws')] => ('CFNT_stats30', 'Met1_wnd_spd') --->
    col_alias[('CFNT_stats30', 'Met1_wnd_wpd')] => ('stats30', '') --->
    col_alias[('stats30', 'Met1_wnd_spd')] => ('', '') # current tbl/col names
"""

# make list of all unique table names present in col_alias dict
historical_table_names = set([ k[0] for (k,v) in col_alias.iteritems()])




def _verify_col_alias():
    """Follow all past column names to current name to verify lookup table

    Return truth of whether column lookup table is free of missing data
    """
    # TODO make output shown only for errors
    errs = ''
    print ('Verifying column alias dictionary...\n')
    for (st, sc) in sorted(col_alias):
        try:
            dt, dc = current_names(st, sc)
        except KeyError:
            errs = errs + ('- unable to complete lookup for table:column '
                '"%s:%s"\n' % (st, sc))
            continue
        #print (('%s:%s' % (st, sc)).ljust(39) + ('%s:%s\n' % (dt,dc)))
    if errs:
        print ('\nWARNINGS:\n'+errs+'\n')
        return False
    else:
        print ('No warnings.\n')
        return True


def _verify_table_definitions():
    """Attempt to look up each column in header definition

    Return truth of whether all defined headers are current based on col_alias
    """
    # TODO make output shown only for errors
    errmsg = ''
    print ('Verifying column order defintions...\n')
    for tbl in table_definitions:
        for col in table_definitions[tbl]:
            try:
                t, c = current_names(tbl, col)
            except KeyError:
                errmsg = errmsg + ('- header definition "%s:%s" not found in '
                    'lookup table\n' % (tbl, col))
                continue
            #print (('%s:%s' % (tbl, col)).ljust(39) + ('%s:%s\n' % (t,c)))
            if (t != tbl) or (c != col):
                errmsg = errmsg + ('- header definition "%s:%s" does not '
                    'match lookup table "%s:%s"\n' % (tbl, col, t, c))
    if errmsg:
        print ('WARNINGS:\n'+errmsg+'\n')
        return False
    else:
        print ('No warnings.\n')
        return True


if __name__ == '__main__':
    ans = raw_input('This module is not designed for standalone operation. '
                    'Proceed with self-test routine? [Y]es else no:')
    if ans and ans.strip() in ['y', 'Y']:
        try:
            _verify_col_alias()
        except Exception as ex:
            print ('/*- A problem occurred while verifying the historical column '
                    'alias dictionary (``col_alias``): \n{err}'.format(err=ex))
        try:
            _verify_table_definitions()
        except Exception as ex:
            print ('/*- A problem occurred while verifying the table column '
                    'definitions (``table_definitions``): \n{err})'.format(err=ex))




