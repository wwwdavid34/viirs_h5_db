#!/usr/bin/env python3

import datetime
import os
import sys
import subprocess
import re
import psycopg2
from psycopg2 import sql
from tools import randomword
from shapely import wkt, wkb
from parse_file import InfoFile


class DBInfo:

    def __init__(self):

        self.db_info = {
            'local': {
                'database': 'viirs_h5',
                'host': 'localhost',
                'port': '5432',
                'user': 'postgres',
                'pass': '2ijillgl'
            },
            'sharkube': {
                'database': 'viirs_h5',
                'host': 'sharkube.com',
                'port': '5432',
                'user': 'postgres',
                'pass': '2ijillgl'
            },
            'boat': {
                'database': 'eog_raster',
                'host': 'boat.colorado.edu',
                'port': '5432',
                'user': 'rasteradmin',
                'pass': 'viirs2018'
            },
            'eogdev': {
                'database': 'eog_raster',
                'host': 'eogdev.mines.edu',
                'port': '5432',
                'user': 'fengchihsu',
                'pass': '2ijillgl'
            }

        }

        self.server = None

    def get(self, server='eogdev'):

        if self.server is not None:
            return self.server

        if server == 'local':
            self.server = self.db_info['local']
            return self.db_info['local']
        if server == 'sharkube':
            self.server = self.db_info['sharkube']
            return self.db_info['sharkube']
        if server == 'boat':
            self.server = self.db_info['boat']
            #print(self.db_info['boat'])
            return self.db_info['boat']
        if server == 'eogdev':
            self.server = self.db_info['eogdev']
            #print(self.db_info['eogdev'])
            return self.db_info['eogdev']
        # default to local boat db
        #self.server = self.db_info['eogdev']
        #print(self.db_info['eogdev'])
       # return self.db_info['eogdev']

    def make_con(self, server='boat'):
        server_info = self.get(server)
        conn = psycopg2.connect(database=server_info['database'],
                                host=server_info['host'],
                                port=server_info['port'],
                                user=server_info['user'],
                                password=server_info['pass'])
        return conn


class ImportToDB(object):

    def __init__(self, file_path, update=False, server='boat'):

        # parse h5 information
        self.finfo = InfoFile()
        self.finfo.parse_file(file_path)
        self.gid = None
        self.h5id = None
        self.rastid = None

        # prepare database connection
        db_info = DBInfo()
        self.server = db_info.get(server)

        # start importing h5 info into database
        self.import_to_db(update)
        
    def make_conn(self):

        return psycopg2.connect(database=self.server['database'],
                                host=self.server['host'],
                                port=self.server['port'],
                                user=self.server['user'],
                                password=self.server['pass']
                                )

    def import_to_db(self, update):

        # do leap second check first
        self._update_leap_second()

        if self.finfo.is_h5:

            filename = self.finfo.filename
            h5id = self._ask_h5id(filename)

            if h5id is None:

                # granule table needs to be updated first no matter what
                self._insert_granule_info()
                self._insert_file_hdf5_info()
                if self.finfo.midtime != {}:
                    self._insert_midtime()
                if self.finfo.qf3_scan_rdr != {}:
                    self._insert_qf3_scan_rdr()
                if self.finfo.radiance_factor != {}:
                    self._insert_radiance_factor()
                if self.finfo.is_geo:
                    self._insert_gring_ncei()
                    self._insert_solz()

            else:
                if update:
                    self._update_file_info()
                else:
                    print('This HDF5 file is already in the database: %s' % filename)
                    sys.exit(0)

        else:

            filename = self.finfo.filename
            rastid = self._ask_rastid(filename)

            if rastid is None:

                self._insert_granule_info()
                self._insert_file_raster_info()
                # self._import_raster_od()

            else:
                if update:
                    self._update_file_info()
                    # self._update_raster_od()
                else:
                    print('This raster file is already in the database: %s' % filename)
                    sys.exit(0)

    def _ask_h5id(self, filename):

        if self.h5id is not None:
            return self.h5id

        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL("SELECT h5id FROM {} WHERE fname=%s")
                        .format(sql.Identifier('info_file_hdf5')),
                        (filename,)
                        )
            result = cur.fetchall()
            cur.close()
            if len(result) == 0:
                print('h5id not found for file: %s' % filename)
                return None
            else:
                h5id = result[0][0]
                print('Found h5id %s' % h5id)
                self.h5id = h5id
                return h5id
        except psycopg2.Error:
            print('h5id retrieve error.')
            sys.exit(1)
        finally:
            conn.close()

    def _ask_rastid(self, filename):

        if self.rastid is not None:
            return self.rastid

        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL("SELECT rastid FROM {} WHERE fname=%s")
                        .format(sql.Identifier('info_file_raster')),
                        (filename,)
                        )
            result = cur.fetchall()
            cur.close()
            if len(result) == 0:
                print('rastid not found for file: %s' % filename)
                return None
            else:
                rastid = result[0][0]
                print('Found rastid %s' % rastid)
                self.rastid = rastid
                return rastid
        except psycopg2.Error:
            print('rastid retrieve error.')
            sys.exit(1)
        finally:
            conn.close()

    def _ask_gid(self, gname):

        if self.gid is not None:
            return self.gid

        conn = self.make_conn()
        cur = conn.cursor()

        try:
            cur.execute(sql.SQL("SELECT gid FROM {} WHERE gname=%s")
                        .format(sql.Identifier('info_granule')),
                        (gname,)
                        )
            result = cur.fetchall()
            cur.close()
            if len(result) == 0:
                print('gid not found for gname: %s' % gname)
                return None
            else:
                gid = result[0][0]
                print('Found gid: %s' % gid)
                self.gid = gid
                return gid
        except psycopg2.Error:
            print('gid retrieve error.')
            sys.exit(1)
        finally:
            conn.close()

    def _make_temp_dir(self):
        # create temporary dir for CSV to be copied into db

        randomstr = randomword(5)
        # randomstr = '_'
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        temp_dir = os.path.join('/tmp', '_'.join(['h5pg', timestamp, randomstr]))
        os.makedirs(temp_dir)

        return temp_dir

    def _download_leap_second_file(self, dest_dir):

        import ftplib
        url = 'ftp://ftp.nist.gov/pub/time/leap-seconds.list'
        # url = self.leap_second_file
        filename = os.path.basename(url)
        local_filename = os.path.join(dest_dir, filename)
        file = open(local_filename, 'wb')

        server, cwd = re.match('ftp://(.*?)/(.*)/', url).groups()
        ftp = ftplib.FTP(server)
        ftp.login('anonymous')
        ftp.cwd(cwd)

        try:
            ftp.retrbinary("RETR " + filename, file.write)
        except:
            print("Error")

        return local_filename

    def _need_to_update_leap_second(self):

        conn = self.make_conn()
        cur = conn.cursor()
        a = datetime.datetime(1980, 1, 1)  # default a old day
        try:
            cur.execute(sql.SQL('SELECT modified FROM {} ORDER BY modified DESC LIMIT 1')
                        .format(sql.Identifier('leap_seconds')))
            a = cur.fetchall()[0][0]
        except:
            print('Check leap second status failed, will proceed to update.')
            return True
        finally:
            conn.close()
        if datetime.datetime.now() - a > datetime.timedelta(days=170):
            return True
        else:
            return False

    def _update_leap_second(self):

        # check if the leap second table is up to date
        ## select the modified date from the row with latest implement date leap_second table
        ## if the date is within 1 month then do nothing
        ## otherwise...
        if not self._need_to_update_leap_second():
            print('Leap second table is up to date.')
            return
        print('Leap second table needs to be updated.')
        # download the latest leap second file
        temp_dir = self._make_temp_dir()
        leap_second_file = self._download_leap_second_file(temp_dir)
        # parse the file
        cdate = datetime.datetime.now()
        epoch = datetime.datetime(1900, 1, 1)
        f = open(leap_second_file, 'r')
        for i in f:
            if not i.startswith('#'):
                ds, lp, ex = [j for j in i.split('\t') if j is not '']
                dt = epoch + datetime.timedelta(seconds=int(ds))
                try:
                    conn = self.make_conn()
                    cur = conn.cursor()
                    cur.execute(sql.SQL('INSERT INTO {}(epoch_dt, epoch, leap_seconds, modified) VALUES ( %s, %s, %s, %s)')
                                .format(sql.Identifier('leap_seconds')),
                                [
                                    dt.strftime('%Y-%m-%d'),
                                    int(ds),
                                    int(lp),
                                    cdate.strftime('%Y-%m-%d')
                                ])
                    conn.commit()
                    conn.close()
                except psycopg2.IntegrityError:
                    print('This leap second record already exists: %s %s' % (ds, lp))
                except psycopg2.Error:
                    print('Warning: Leap second record update failed.')

    def _update_file_info(self):

        if self.finfo.is_h5:
            self._update_link_hdf5()
        else:
            self._update_link_raster()

    def _insert_file_raster_info(self):

        # get gid
        self.gid = self._ask_gid(self.finfo.gname)
        if self.gid is None:
            sys.exit(1)

        val = self._get_raster_od_hex()

        # import file info
        col_name_list = [
            'fname',
            'ftype',
            'space_craft',
            'dt_start',
            'dt_end',
            'dt_create',
            'orbit',
            'source',
            'state',
            'space',
            'geolocation',
            'gid',
            'link',
            'content',
            'rast'
        ]

        sql_var = '(' + ','.join(['%s'] * len(col_name_list)) + ')'

        col_names = ','.join(col_name_list)

        conn = self.make_conn()
        cur = conn.cursor()

        try:
            cur.execute(sql.SQL('INSERT INTO {} (' + col_names + ') VALUES ' + sql_var)
                        .format(sql.Identifier('info_file_raster')),
                        [
                            self.finfo.filename,
                            self.finfo.ftype,
                            self.finfo.space_craft,
                            self.finfo.dt_start,
                            self.finfo.dt_end,
                            self.finfo.dt_create,
                            self.finfo.orbit,
                            self.finfo.source,
                            self.finfo.state,
                            self.finfo.space,
                            self.finfo.is_geo,
                            self.gid,
                            self.finfo.link,
                            self.finfo.content,
                            val
                        ])
            conn.commit()
        except psycopg2.IntegrityError:
            print('This file is already in the table: %s' % self.finfo.filename)
        finally:
            conn.close()

    def _insert_file_hdf5_info(self):

        # get gid
        self.gid = self._ask_gid(self.finfo.gname)
        if self.gid is None:
            sys.exit(1)

        if self.finfo.ngranule == 1:
            geo_obj = self._make_gring_info_single()
        else:
            geo_obj = self._make_gring_info_multi()
        wkbhex = wkb.dumps(geo_obj, srid=4326, hex=True)

        # import file info
        col_name_list = [
            'fname',
            'ftype',
            'space_craft',
            'dt_start',
            'dt_end',
            'dt_create',
            'orbit',
            'source',
            'state',
            'space',
            'nscan',
            'ngranule',
            'geolocation',
            'desc_indicator',
            'gid',
            'gring',
            'link'
        ]

        sql_var = '('+','.join(['%s']*len(col_name_list))+')'

        # put desc indicator boolean from dict into list
        desc_val = []
        for key in self.finfo.desc.keys():
            if self.finfo.desc[key][0][0] == 0:
                desc_val.append(False)
            else:
                desc_val.append(True)
        # print(desc_val)
        col_names = ','.join(col_name_list)

        conn = self.make_conn()
        cur = conn.cursor()

        try:
            cur.execute(sql.SQL('INSERT INTO {} (' + col_names + ') VALUES ' + sql_var)
                        .format(sql.Identifier('info_file_hdf5')),
                        [
                            self.finfo.filename,
                            self.finfo.ftype,
                            self.finfo.space_craft,
                            self.finfo.dt_start,
                            self.finfo.dt_end,
                            self.finfo.dt_create,
                            self.finfo.orbit,
                            self.finfo.source,
                            self.finfo.state,
                            self.finfo.space,
                            self.finfo.nscan,
                            self.finfo.ngranule,
                            self.finfo.is_geo,
                            desc_val,
                            self.gid,
                            wkbhex,
                            self.finfo.link
                        ])
            conn.commit()
            print('%s ingested.' % self.finfo.filename)
        except psycopg2.IntegrityError:
            print('This file is already in the table: %s' % self.finfo.filename)
        finally:
            conn.close()

    def _insert_granule_info(self):

        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL("INSERT INTO {}(gname) VALUES (%s)")
                        .format(sql.Identifier('info_granule')),
                        [self.finfo.gname]
                        )
            conn.commit()
        except psycopg2.IntegrityError:
            print('This gname is already in the table: %s' % self.finfo.gname)
        finally:
            cur.close()
            conn.close()

    def _make_gring_info_single(self):

        keys = list(self.finfo.gring.keys())
        lat_gr_key = [i for i in keys if i.endswith('/G-Ring_Latitude')][0]
        lon_gr_key = [i for i in keys if i.endswith('/G-Ring_Longitude')][0]
        lat_gr = self.finfo.gring[lat_gr_key]
        lon_gr = self.finfo.gring[lon_gr_key]

        zipper = [' '.join([str(lon_gr[i][0]), str(lat_gr[i][0])]) for i in range(0, len(lat_gr))]
        zipper.append(zipper[0])

        wkt_text = 'POLYGON ((' + ','.join(zipper) + '))'
        geo_obj = wkt.loads(wkt_text)

        return geo_obj

    def _make_gring_info_multi(self):

        ngra = self.finfo.ngranule
        keys = list(self.finfo.gring.keys())
        wkt_gr = []
        for n in range(0, ngra):
            lat_gr_key = [i for i in keys if i.endswith('_Gran_'+str(n)+'/G-Ring_Latitude')][0]
            lon_gr_key = [i for i in keys if i.endswith('_Gran_'+str(n)+'/G-Ring_Longitude')][0]
            lat_gr = self.finfo.gring[lat_gr_key]
            lon_gr = self.finfo.gring[lon_gr_key]
            zipper = [' '.join([str(lon_gr[i][0]), str(lat_gr[i][0])]) for i in range(0, len(lat_gr))]
            zipper.append(zipper[0])
            wkt_gr.append('(('+','.join(zipper)+'))')
        wkt_text = 'MULTIPOLYGON ('+','.join(wkt_gr)+')'
        geo_obj = wkt.loads(wkt_text)

        return geo_obj

    def _insert_solz(self):

        h5id = self._ask_h5id(self.finfo.filename)
        val = self.finfo.solz

        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL('INSERT INTO {}(h5id, val) VALUES (' + ','.join(['%s'] * 2) + ')')
                        .format(sql.Identifier('solar_zenith')),
                        [
                            h5id,
                            val.tolist()
                        ])

            conn.commit()
        except psycopg2.IntegrityError:
            print('This solar zenith min/max insert already exists?')
        # except psycopg2.Error:
        #     print('solar zenith min/max insert failed.')
        #     sys.exit(1)
        finally:
            conn.close()

    def _insert_midtime(self):

        h5id = self._ask_h5id(self.finfo.filename)
        key = list(self.finfo.midtime.keys())[0]
        val = self.finfo.midtime[key]

        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL('INSERT INTO {}(h5id, val) VALUES (' + ','.join(['%s'] * 2) + ')')
                        .format(sql.Identifier('midtime')),
                        [
                            h5id,
                            val.tolist()
                        ])

            conn.commit()
        except psycopg2.IntegrityError:
            print('This midtime insert already exists?')
        except psycopg2.Error:
            print('midtime insert failed.')
            sys.exit(1)
        finally:
            conn.close()

    def _insert_qf3_scan_rdr(self):

        h5id = self._ask_h5id(self.finfo.filename)
        key = list(self.finfo.qf3_scan_rdr.keys())[0]
        val = self.finfo.qf3_scan_rdr[key]

        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL('INSERT INTO {}(h5id, val) VALUES (' + ','.join(['%s'] * 2) + ')')
                        .format(sql.Identifier('qf3_scan_rdr')),
                        [
                            h5id,
                            val.tolist()
                        ])
            conn.commit()
        except psycopg2.IntegrityError:
            print('This qf3_scan_rdr insert already exists?')
        except psycopg2.Error:
            print('qf3_scan_rdr insert failed.')
            sys.exit(1)
        finally:
            conn.close()

    def _insert_radiance_factor(self):

        h5id = self._ask_h5id(self.finfo.filename)
        key = list(self.finfo.radiance_factor.keys())[0]
        val = self.finfo.radiance_factor[key]

        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL('INSERT INTO {}(h5id, val) VALUES (' + ','.join(['%s'] * 2) + ')')
                        .format(sql.Identifier('radiance_factor')),
                        [
                            h5id,
                            val.tolist()
                        ])
            conn.commit()
        except psycopg2.IntegrityError:
            print('This radiance_factor insert already exists?')
        except psycopg2.Error:
            print('radiance_factor insert failed.')
            sys.exit(1)
        finally:
            conn.close()

    def _get_raster_od_hex(self):

        # path = os.path.join(self.finfo.link, self.finfo.filename)
        ret = subprocess.Popen(['raster2pgsql', '-R', self.finfo.link], stdout=subprocess.PIPE)
        out = None
        #print(list(ret.stdout))
        for line in iter(ret.stdout):
            if 'INSERT' in str(line):
                out = str(line)

        rem = re.match(".*'([0-9A-Z]{20,}).*", str(out))
        val = rem.group(1)

        return val

    def _update_link_hdf5(self):

        h5id = self._ask_h5id(self.finfo.filename)

        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL('UPDATE {} SET link = %s WHERE h5id = %s')
                        .format(sql.Identifier('info_file_hdf5')),
                        [
                            self.finfo.link,
                            h5id
                        ])
            conn.commit()
            print('Update success.')
        except psycopg2.Error:
            print('Update error for h5id=%s: %s' % (h5id, self.finfo.link))
            sys.exit(1)
        finally:
            conn.close()

    def _update_link_raster(self):

        rastid = self._ask_rastid(self.finfo.filename)
        val = self._get_raster_od_hex()

        # print('Update raster')
        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL('UPDATE {} SET rast = %s, link = %s WHERE rastid = %s')
                        .format(sql.Identifier('info_file_raster')),
                        [
                            val,
                            self.finfo.link,
                            rastid
                        ])
            conn.commit()
            print('Update success.')
        except psycopg2.Error:
            print('Update error for rastid=%s: %s' % (rastid, self.finfo.link))
            sys.exit(1)
        finally:
            conn.close()

    def _insert_link_dnb_loc(self):
        # insert records with link to gdnbo lat/lon grid

        h5id = self._ask_h5id(self.finfo.filename)
        lat_grid = self.finfo.latitude
        lon_grid = self.finfo.longitude

        # save lat/lon grid to local files


    def _insert_gring_ncei(self):

        h5id = self._ask_h5id(self.finfo.filename)
        geo_obj = self._make_gring_ncei()
        wkbhex = wkb.dumps(geo_obj, srid=4326, hex=True)

        conn = self.make_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql.SQL('INSERT INTO {} (h5id, space, gring) VALUES ( %s, %s, %s )')
                        .format(sql.Identifier('gring_ncei')),
                        [
                            h5id,
                            self.finfo.space,
                            wkbhex
                        ])

            conn.commit()
            print('Insert gring_ncei success.')
        # except psycopg2.Error:
        #     print('Insert gring_ncei failed for gid=%s: %s' %(gid, self.finfo.gname))
        #     sys.exit(1)
        finally:
            conn.close()

    def _make_gring_ncei(self):

        import numpy as np

        # this module makes ncei house baked gring from lat/lon grids provided in geolocation files
        klats = list(self.finfo.latitude.keys())[0]
        mlats = self.finfo.latitude[klats]
        klons = list(self.finfo.longitude.keys())[0]
        mlons = self.finfo.longitude[klons]
        mlines, mcols = np.shape(np.array(mlats))

        # dls = find(malts(:,1) > -999)
        dls = np.where(mlats[:, 0] > -999)[0]
        ndls = len(dls)

        granule_mode = True
        if self.finfo.ngranule > 1:
            granule_mode = False

        # cross180 = False
        if granule_mode:
            print('Granule Mode.')
            bounds = [[0] * 2 for _ in range(6)]

            # col based
            bounds[0][1] = mlats[dls[0]][0]
            bounds[0][0] = mlons[dls[0]][0]
            bounds[1][1] = mlats[dls[0]][round(mcols/2) - 1]
            bounds[1][0] = mlons[dls[0]][round(mcols/2) - 1]
            bounds[2][1] = mlats[dls[0]][mcols - 1]
            bounds[2][0] = mlons[dls[0]][mcols - 1]
            bounds[3][1] = mlats[dls[-1]][mcols - 1]
            bounds[3][0] = mlons[dls[-1]][mcols - 1]
            bounds[4][1] = mlats[dls[-1]][round(mcols/2) - 1]
            bounds[4][0] = mlons[dls[-1]][round(mcols/2) - 1]
            bounds[5][1] = mlats[dls[-1]][0]
            bounds[5][0] = mlons[dls[-1]][0]
            # print([
            #     [dls[0],0],
            #     [dls[0],round(mcols/2)-1],
            #     [dls[0],mcols-1],
            #     [dls[-1],mcols-1],
            #     [dls[-1],round(mcols/2)-1],
            #     [dls[-1],0]
            # ])
            # print(bounds)
            # input()

            zipper = [
                ' '.join([str(k) for k in bounds[i]]) for i in range(0, len(bounds))
            ]
            zipper.append(zipper[0])

        else:

            ngrans = self.finfo.ngranule
            print('Aggregate mode with %s granules.' % ngrans)
            bounds = [[0] * 2 for _ in range((6 + (ngrans - 1) * 2))]

            # col based
            bounds[0][1] = mlats[dls[0]][0]
            bounds[0][0] = mlons[dls[0]][0]
            bounds[1][1] = mlats[dls[0]][round(mcols / 2) - 1]
            bounds[1][0] = mlons[dls[0]][round(mcols / 2) - 1]
            bounds[2][1] = mlats[dls[0]][mcols - 1]
            bounds[2][0] = mlons[dls[0]][mcols - 1]

            for ngr in range(1, ngrans):
                bounds[2 + ngr][1] = mlats[dls[round(ngr * ndls / ngrans) - 1]][mcols - 1]
                bounds[2 + ngr][0] = mlons[dls[round(ngr * ndls / ngrans) - 1]][mcols - 1]

            bounds[2 + ngrans][1] = mlats[dls[-1]][mcols - 1]
            bounds[2 + ngrans][0] = mlons[dls[-1]][mcols - 1]
            bounds[3 + ngrans][1] = mlats[dls[-1]][round(mcols / 2) - 1]
            bounds[3 + ngrans][0] = mlons[dls[-1]][round(mcols / 2) - 1]
            bounds[4 + ngrans][1] = mlats[dls[-1]][0]
            bounds[4 + ngrans][0] = mlons[dls[-1]][0]

            for ngr in range(1, ngrans):
                bounds[4 + ngrans + ngr][1] = mlats[dls[round((ngrans - ngr) * ndls / ngrans) - 1]][0]
                bounds[4 + ngrans + ngr][0] = mlons[dls[round((ngrans - ngr) * ndls / ngrans) - 1]][0]

        # over pole test is skipped in this python port

            zipper = [
                ' '.join([str(k) for k in bounds[i]]) for i in range(0, len(bounds))
            ]
            zipper.append(zipper[0])

        wkt_text = 'POLYGON ((' + ','.join(zipper) + '))'
        # print(wkt_text)
        geo_obj = wkt.loads(wkt_text)
        # print(geo_obj)

        return geo_obj







