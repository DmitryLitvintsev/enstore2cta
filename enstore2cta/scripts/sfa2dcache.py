#!/bin/env python

"""
This script does Enstore SFA -> dCache SFA

"""

from __future__ import print_function
import argparse
import errno
import multiprocessing
import os
import re
import socket
import subprocess
import sys
import time
import yaml

import psycopg2
import psycopg2.extras

try:
    import urlparse
except ModuleNotFoundError:
    import urllib.parse as urlparse


CONFIG_FILE = os.getenv("MIGRATION_CONFIG")
if not CONFIG_FILE:
    CONFIG_FILE = "sfa2dcache.yaml"

HOSTNAME = socket.getfqdn()

PNFS_HOME = "/pnfs/fs/usr"

printLock = multiprocessing.Lock()

CRC_SWITCH = '2019-08-21 09:54:26'


def get_switch_epoch():
    """
    Timestamp when the change from 0 to 1 based adler checksum happened
    """
    time_format = '%Y-%m-%d %H:%M:%S'
    os.environ['TZ'] = 'America/Chicago'
    epoch = int(time.mktime(time.strptime(CRC_SWITCH, time_format)))
    return epoch


def convert_0_adler32_to_1_adler32(crc, filesize):
    BASE = 65521
    size = filesize % BASE
    s1 = (crc & 0xffff)
    s2 = ((crc >> 16) & 0xffff)
    s1 = (s1 + 1) % BASE
    s2 = (size + s2) % BASE
    new_adler = (s2 << 16) + s1
    return new_adler


def execute_command(cmd):
    """
    Executes shell command

    :param cmd: command string
    :type cmd: str
    :return: shell command return code
    :rtype: int
    """
    p = subprocess.Popen(cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    output, errors = p.communicate()
    rc = p.returncode
    return rc


def print_error(text):
    """
    Print text string to stderr prefixed with timestamp
    and ERROR keyword

    :param text: text to be printed
    :type text: str
    :return: no value
    :rtype: none
    """
    with printLock:
        sys.stderr.write(time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(time.time()))+" ERROR : " + text + "\n")
        sys.stderr.flush()


def print_message(text):
    """
    Print text string to stdout prefixed with timestamp
    and INFO keyword

    :param text: text to be printed
    :type text: str
    :return: no value
    :rtype: none
    """
    with printLock:
        sys.stdout.write(time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.localtime(time.time()))+" INFO : " + text + "\n")
        sys.stdout.flush()


def get_path(pnfsid):
    """
    Get path for pnfsid
    Uses mounts pnfs to do that
    """
    path = None
    with open("/pnfs/fnal.gov/usr/.(pathof)(%s)" % (pnfsid, ), "r") as fh:
        path = fh.readlines()[0].strip()
    return path

def get_pnfsid(path):
    """
    Get pnfsis for path
    """
    pnfsid = None
    dn = os.path.dirname(path)
    fn = os.path.basename(path)
    with open("%s/.(id)(%s)" % (dn, fn,), "r") as fh:
        pnfsid = fh.readlines()[0].strip()
    return pnfsid


# create DB connection from URI
def create_connection(uri):
    result = urlparse.urlparse(uri)
    connection = psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port)
    return connection

INSERT_LOCATION = """
insert into t_locationinfo
(inumber , itype, ipriority, ictime, iatime, istate, ilocation)
values ((select inumber from t_inodes where ipnfsid = %s), %s, %s, now(), now(), %s, %s)
"""

DELETE_LOCATION = """
delete from t_locationinfo
where inumber = (select inumber from t_inodes where ipnfsid = %s) and
ilocation like 'dcache%%'
"""


INSERT_CHECKSUM = """
insert into t_inodes_checksum
(inumber, itype, isum)
values (%s, 1, %s)
"""

class SfaWorker(multiprocessing.Process):
    """
    This class is responsible for setting AL/RP = NEARLINE/CUSTODIAL
    setting files precious on location.
    """
    def __init__(self, queue, configuration):
        super().__init__()
        self.queue = queue
        self.configuration = configuration

    def run(self):
        # db connection pool to enstore db
        enstore_db = create_connection(self.configuration.get("enstore_db"))
        # chimera_db
        chimera_db = create_connection(self.configuration.get("chimera_db"))

        Q = "select f.*, v.storage_group from file f inner join volume v on v.id = f.volume  where bfid = %s"

        for data in iter(self.queue.get, None):
            inumber = data.get("ino")
            file_name = data.get("path")
            pnfsid = data.get("pnfsid")
            chimera_file_size = int(data.get("fsize"))
            chimera_file_family = data.get("file_family")
            bfid = data.get("bfid")

            if not bfid:
                print_error(f"file {pnfsid}, {file_name} has no bfid")
                continue

            csum_info = select(chimera_db,
                               "select isum from t_inodes_checksum where inumber = %s and itype = 1",
                               (inumber,))

            chimera_file_csum = None
            if csum_info:
                chimera_file_csum = csum_info[0].get("isum")

            file_infos = select(enstore_db,
                                Q,
                                (bfid, ))
            if not file_infos:
                print_error(f"file {pnfsid}, {file_name} {bfid} not found in enstore db")
                continue

            file_info = file_infos[0]

            enstore_bfid = file_info["bfid"]
            enstore_file_size = int(file_info["size"])
            enstore_file_csum = int(file_info["crc"])
            enstore_file_create_time = int(enstore_bfid[4:14])
            enstore_storage_group = file_info["storage_group"]
            deleted = file_info["deleted"]

            #print_message(f"{pnfsid} {file_name} storage_group =  {enstore_storage_group} file_family = {chimera_file_family}")

            if deleted != "n":
                print_error(f"file {pnfsid}, {file_name} BFID marked deleted {bfid}")
                continue

            if bfid != enstore_bfid:
                print_error(f"file {pnfsid}, {file_name} BFID mismatch {bfid} != {enstore_bfid}")
                continue

            if enstore_file_create_time < get_switch_epoch() and HOSTNAME.endswith(".fnal.gov"):
                enstore_file_csum =  convert_0_adler32_to_1_adler32(enstore_file_csum,
                                                                     enstore_file_size)

            if chimera_file_size != enstore_file_size:
                print_error(f"file {pnfsid}, {file_name} {bfid} size does not match {chimera_file_size} != {enstore_file_size}")
                continue

            enstore_file_csum = hex(enstore_file_csum).lstrip("0x").zfill(8)

            if not chimera_file_csum:
                print_error(f"file {pnfsid}, {bfid} no chimera checksum, inserting")
                insert(chimera_db,
                       INSERT_CHECKSUM,
                       (inumber,
                        enstore_file_csum))
            elif chimera_file_csum != enstore_file_csum:
                print_error(f"file {pnfsid}, {file_name} {bfid} checksum does not match {chimera_file_csum} != {enstore_file_csum}")
                continue

            #
            # get list of children
            #

            package_files_count = file_info["package_files_count"]
            enstore_children = select(enstore_db,
                                      "select pnfs_id from file where bfid != package_id and package_id = %s and deleted = 'n'",
                                      (bfid,))

            pnfsids = [x["pnfs_id"] for x in enstore_children]

            for child_pnfsid in pnfsids:
                insert(chimera_db,
                       DELETE_LOCATION,
                       (child_pnfsid,))

                try:
                    insert(chimera_db,
                           INSERT_LOCATION,
                           (child_pnfsid,
                            0,
                            10,
                            1,
                            f"dcache://dcache/?store={enstore_storage_group}&group={chimera_file_family}&bfid={child_pnfsid}:{pnfsid}"
                           ))
                except psycopg2.errors.NotNullViolation:
                    #print_error(f"file {pnfsid} child {child_pnfsid} does not exist")
                    pass

        enstore_db.close()
        chimera_db.close()

def select(con, sql, pars=None):
    """
    Select  database records

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    cursor = None
    try:
        cursor = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if pars:
            cursor.execute(sql, pars)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def update(con, sql, pars):
    """
    Update database record

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    return insert(con, sql, pars)


def insert(con, sql, pars):
    """
    Insert database record

    :param con: database connection
    :type con: Connection

    :param sql: SQL statement
    :type sql: str

    :param pars: query parameters
    :type pars: tuple

    :return: result
    :rtype: object
    """
    cursor = None
    try:
        cursor = con.cursor()
        cursor.execute(sql, pars)
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass



def main():
    """
    main function
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dir",
        help="top directory name")

    parser.add_argument(
        "--cpu_count",
        action  = "store",
        type = int,
        default =  multiprocessing.cpu_count(),
        help="override cpu count - number of simultaneously processed labels")



    args = parser.parse_args()



    configuration = None
    try:
        mode = os.stat(CONFIG_FILE).st_mode
        if mode != 33152:
            print_error("Access to config file file %s is too permissive, do chmod 0600" %
                        (CONFIG_FILE,))
            sys.exit(1)
        with open(CONFIG_FILE, "r") as f:
            configuration = yaml.safe_load(f)
    except (OSError, IOError) as e:
        if e.errno == errno.ENOENT:
            print_error("Config file %s does not exist" % (CONFIG_FILE,))
        sys.exit(1)

    if not configuration:
        print_error("Failed to load configuration %s" % (CONFIG_FILE,))
        sys.exit(1)

    print (configuration)


#    if not args.dir:
#        parser.print_help(sys.stderr)
#        sys.exit(1)

    if not os.path.exists(PNFS_HOME):
        print_error("PNFS is not mounted. Quitting.")
        sys.exit(1)

#    if not os.path.exists(args.dir):
#        print_error("Directry %s does not exist. Quitting.")
#        sys.exit(1)

#    pnfsid = get_pnfsid(args.dir)
    pnfsid = '00008ACFB7F5ADB641ED8596422F49DA5D50'

    print_message("**** Start processing ***")

    queue = multiprocessing.Queue(20000)
    workers = []

    cpu_count = args.cpu_count

    for i in range(cpu_count):
        worker = SfaWorker(queue, configuration)
        workers.append(worker)
        worker.start()

    QUERY = """
    WITH RECURSIVE paths(ino, path, pnfsid, fsize, ftype) AS (VALUES
    (pnfsid2inumber(%s),'','', 0::BIGINT, 16384)
    UNION SELECT i.inumber,
		 path||'/'||d.iname,
		 i.ipnfsid,
		 i.isize,
		 i.itype
    FROM
        t_dirs d, t_inodes i, paths p
    WHERE p.ftype=16384 AND
          d.iparent=p.ino AND
	  d.iname != '.' AND
          d.iname != '..' AND
	  i.inumber=d.ichild)
    SELECT p.ino,
           p.path,
    	   p.pnfsid,
	   p.fsize,
	   encode(l1.ifiledata,'escape') as bfid,
           ts.istoragesubgroup as file_family
    FROM paths p
    LEFT OUTER JOIN t_level_1 l1 ON (p.ino = l1.inumber)
    LEFT OUTER JOIN t_storageinfo ts ON (p.ino = ts.inumber)
    WHERE p.ftype = 32768
    """

    chimera_db = cursor = None
    try:
        chimera_db =  create_connection(configuration.get("chimera_db"))
        cursor = chimera_db.cursor("cursor_sfa",
                                   cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(QUERY, (pnfsid, ))

        total = 0
        t0 = time.time()

        while True:
            res = cursor.fetchmany(10000)
            if not res:
                break
            total += len(res)
            for r in res:
                queue.put(r)
            print_message("Processing %d,  queue size %d "%(total, queue.qsize()))
    finally:
        for i in (cursor, chimera_db):
            try:
                if i:
                    i.close()
            except:
                pass

    for i in range(cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()

    print_message("**** FINISH ****")
    print_message("Took %d seconds" % (int(time.time()-t0+0.5),))


if __name__ == "__main__":
    main()
