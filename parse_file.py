#!/usr/bin/env python3

import os
import re
import datetime
import h5py
import numpy as np


# from viirs_h5_db.h5_setting import InfoH5Setting


class InfoFile:

    def __init__(self):

        # self.h5_setting = InfoH5Setting()
        self.infile = None
        self.filename = None
        self.ftype = None
        self.space_craft = None
        self.dt_start = None
        self.dt_end = None
        self.dt_create = None
        self.orbit = None
        self.source = None
        self.state = None
        self.content_list = []
        self.gname = None
        self.link = None

        # store data in dictionary as path:data paris
        self.raster = {}
        # self.raster_coeff = {}
        self.qf3_scan_rdr = {}
        self.midtime = {}
        self.radiance_factor = {}
        self.latitude = {}
        self.longitude = {}
        # self.attribute = {}
        self.gring = {}  # only if is a geolocation file
        self.nscan = None

        self.is_geo = False
        self.space = None
        self.content = None  # content type for raster input
        self.reproj = False  # is the raster reprojected?
        self.is_h5 = None
        self.ngranule = None
        self.solz = None
        self.desc = {}

    def parse_h5(self, infile):

        self.parse_file_name(infile)
        self.parse_h5_content(infile)

        # is geolocation file?
        # self.is_geo = self.ask_is_geo()

        # what is the space (M, D, I)?
        self.space = self.ask_space()

    def parse_raster(self, infile):

        self.parse_file_name(infile)
        # parse content to hex wkb is handled in import_to_db.py
        # self.parse_raster_content(infile)

    def parse_file(self, infile):

        if infile.endswith('.h5'):
            self.parse_h5(infile)
            self.is_h5 = True
        elif infile.endswith('.tif'):
            self.parse_raster(infile)
            self.is_h5 = False
        else:
            print('Unsupported filetype.')

    def parse_file_name(self, infile):
        # read file name and collect following information
        # (1) Data type  (SVDNB, SVM10)
        # (2) Space craft (npp, j01)
        # (3) Start / end / creation datetime
        # (4) source (noaa, noac, nobc)
        # (5) Status (ops, dev)
        self.infile = infile

        # get absolute path
        self.link = os.path.abspath(infile)

        base_name, ext_name = os.path.splitext(os.path.basename(infile))
        self.filename = ''.join([base_name, ext_name])
        print('Processing file: %s' % self.filename)

        #################
        # general parsing

        # get granule name
        m = re.match('.*(..._d[0-9]{8}_t[0-9]{7}_e[0-9]{7}_b[0-9]{5}).*', self.filename)
        if m is not None:
            self.gname = m.groups()[0]

        # get file type from full file name if available
        full = re.match('(.....)_..._d[0-9]{8}_t[0-9]{7}_e[0-9]{7}_b[0-9]{5}_' +
                        'c([0-9]{8})([0-9]{6})([0-9]{6})_(....)_(...).*', self.filename)
        if full is not None:
            self.ftype, cdate, ctime, ctimef, self.source, self.state = full.groups()
            self.dt_create = (datetime.datetime.strptime('_'.join([cdate, ctime]), '%Y%m%d_%H%M%S') +
                              datetime.timedelta(microseconds=float(ctimef)))

        # get granule info from file name if available
        parts = re.match('.*(...)_d([0-9]{8})_t([0-9]{6})([0-9])_e([0-9]{6})([0-9])_b([0-9]{5}).*', self.filename)
        if parts is not None:
            self.space_craft, date, ttime, ttimef, etime, etimef, orbit = parts.groups()

            self.dt_start = (datetime.datetime.strptime('_'.join([date, ttime]), '%Y%m%d_%H%M%S') +
                             datetime.timedelta(milliseconds=float(ttimef)*100))
            self.dt_end = (datetime.datetime.strptime('_'.join([date, etime]), '%Y%m%d_%H%M%S') +
                           datetime.timedelta(milliseconds=float(etimef)*100))
            if self.dt_end < self.dt_start:
                self.dt_end += datetime.timedelta(days=1)
            self.orbit = int(orbit)

        #########################
        # raster specific parsing

        if '.lines.' in self.filename:
            self.content = 'lines'
            self.is_geo = True
            self.reproj = True
        elif '.samples' in self.filename:
            self.content = 'samples'
            self.is_geo = True
            self.reproj = True
        elif '.rade9.' in self.filename:
            self.content = 'rade9'
            self.reproj = True
        elif '.srade9.' in self.filename:
            self.content = 'srade9'
            self.reproj = False
        elif '.dspace_rad.' in self.filename:
            self.content = 'dspace_rad'
            self.reproj = False
            self.space = 'D'
        elif '.rad.' in self.filename:
            self.content = 'rad'
            self.reproj = True
        elif '.vflag' in self.filename:
            self.content = 'vflag'
            self.reproj = True
        elif '.dflag.' in self.filename:
            self.content = 'dflag'
            self.space = 'd'
        elif '.dflagr.' in self.filename:
            self.content = 'dflag'
            self.space = 'd'
            self.reproj = True
        elif '.mflag.' in self.filename:
            self.content = 'mflag'
            self.space = 'm'
        elif '.mflagr.' in self.filename:
            self.content = 'mflag'
            self.space = 'm'
            self.reproj = True
        elif '.blur.' in self.filename:
            self.content = 'blur'
        elif '.lon.' in self.filename:
            self.content = 'longitude'
            self.reproj = False
        elif '.lat.' in self.filename:
            self.content = 'latitude'
            self.reporj = False

    def parse_h5_content(self, infile):
        # get target contents in the h5 file and store them in self var

        print('Opening h5 file')
        h5f = h5py.File(infile,'r')

        # retrieve content list
        h5f.visit(self.content_list.append)

        # print(list(h5f.attrs.keys()))
        if 'N_GEO_Ref' not in list(h5f.attrs.keys()):
            self.is_geo = True
        else:
            if h5f.filename.startswith('G'):
                self.is_geo = True
            else:
                self.is_geo = False

        for radiance_key in [i for i in self.content_list if i.endswith('/Radiance')]:
            self.raster[radiance_key] = np.array(h5f[radiance_key])

        for factor_key in [i for i in self.content_list if i.endswith('/RadianceFactors')]:
            self.radiance_factor[factor_key] = np.array(h5f[factor_key])

        for qf3_key in [i for i in self.content_list if i.endswith('/QF3_SCAN_RDR')]:
            self.qf3_scan_rdr[qf3_key] = np.array(h5f[qf3_key])

        granule_group = [i for i in self.content_list if '_Gran_' in i]
        self.ngranule = len(granule_group)  # count granule attribute groups

        # granule specific attribute
        # gring is available in all files
        for granule in granule_group:
            for gring_key in [i for i in list(h5f[granule].attrs.keys()) if
                              (i.endswith('G-Ring_Latitude') or i.endswith('G-Ring_Longitude'))]:
                self.gring[os.path.join(granule, gring_key)] = np.array(h5f[granule].attrs[gring_key])
            for desc_key in [i for i in list(h5f[granule].attrs.keys()) if
                             i.endswith('Ascending/Descending_Indicator')]:
                # print(granule, desc_key)
                self.desc[os.path.join(granule, desc_key)] = h5f[granule].attrs[desc_key]

        # geolocation file only extraction
        if self.is_geo:
            for midtime_key in [i for i in self.content_list if i.endswith('/MidTime')]:
                self.midtime[midtime_key] = np.array(h5f[midtime_key])
            for lat_key in [i for i in self.content_list if i.endswith('/Latitude')]:
                self.latitude[lat_key] = np.array(h5f[lat_key])
            for lon_key in [i for i in self.content_list if i.endswith('/Longitude')]:
                self.longitude[lon_key] = np.array(h5f[lon_key])
            for solz_key in [i for i in self.content_list if i.endswith('/SolarZenithAngle')]:
                sol_grid = np.array(h5f[solz_key])
                # avoid select -999.3 as the min
                self.solz = np.array([sol_grid[np.where(sol_grid > -999.3)].min().tolist(),
                                      sol_grid.max().tolist()])

        for nscan_key in [i for i in self.content_list if i.endswith('/NumberOfScans')]:
            self.nscan = int(np.array(h5f[nscan_key])[0])

        h5f.close()

    def ask_space(self):
        dspace = ['GDNBO', 'SVDNB']
        mspace = ['GMTCO', 'SVM07', 'SVM08', 'SVM09', 'SVM10', 'SVM11', 'SVM12', 'SVM13', 'SVM14',
                  'SVM15', 'SVM16']
        ispace = ['GITCO', 'SVI04', 'SVI05']
        if self.ftype in dspace:
            return 'D'
        elif self.ftype in mspace:
            return 'M'
        elif self.ftype in ispace:
            return 'I'
        else:
            return None
