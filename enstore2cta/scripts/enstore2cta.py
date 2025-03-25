#!/bin/env python
from __future__ import print_function
import argparse
import errno
import multiprocessing
import os
import re
import socket
import stat
import subprocess
import sys
import time
import uuid
import traceback
import psycopg2
import psycopg2.extras
import datetime
import getpass
import yaml

try:
    import urlparse
except ModuleNotFoundError:
    import urllib.parse as urlparse


CONFIG_FILE = os.getenv("MIGRATION_CONFIG")
if not CONFIG_FILE:
    CONFIG_FILE = "enstore2cta.yaml"

HOSTNAME = socket.getfqdn()

CTA_MEDIA_TYPES = { "LTO8" : { "media_type_name" : "LTO8",
                               "cartridge" : "LTO-8",
                               "capacity_in_bytes" : 12000000000000,
                               "primary_density_code" : 94,
                               "secondary_density_code" : None,
                               "nb_wraps" : None,
                               "min_lpos" : None,
                               "max_lpos" : None,
                               "user_comment" : "LTO-8 cartridge formated at 12 TB",
                               "creation_log_user_name" : getpass.getuser(),
                               "creation_log_host_name" : HOSTNAME,
                               "creation_log_time" : int(time.time()),
                               "last_update_user_name" : getpass.getuser(),
                               "last_update_host_name" : HOSTNAME,
                               "last_update_time" : int(time.time()) },
                    "LTO7M": { "media_type_name" : "LTO7M",
                               "cartridge" : "LTO-7",
                               "capacity_in_bytes" : 9000000000000,
                               "primary_density_code" : 93,
                               "secondary_density_code" : None,
                               "nb_wraps" : None,
                               "min_lpos" : None,
                               "max_lpos" : None,
                               "user_comment" : "LTO-7 M8 cartridge formated at 9 TB",
                               "creation_log_user_name" : getpass.getuser(),
                               "creation_log_host_name": HOSTNAME,
                               "creation_log_time" : int(time.time()),
                               "last_update_user_name" : getpass.getuser(),
                               "last_update_host_name" : HOSTNAME,
                               "last_update_time" : int(time.time()) },
                    "LTO9" : { "media_type_name" : "LTO9",
                               "cartridge" : "LTO-9",
                               "capacity_in_bytes" : 18000000000000,
                               "primary_density_code" : 96,
                               "secondary_density_code" : None,
                               "nb_wraps" : None,
                               "min_lpos" : None,
                               "max_lpos" : None,
                               "user_comment" : "LTO-9 cartridge formatted at 18TB",
                               "creation_log_user_name" : getpass.getuser(),
                               "creation_log_host_name": HOSTNAME,
                               "creation_log_time" : int(time.time()),
                               "last_update_user_name" : getpass.getuser(),
                               "last_update_host_name" : HOSTNAME,
                               "last_update_time" : int(time.time()) },
}

INSERT_MEDIA_TYPES = """
insert into media_type (
  media_type_id,
  media_type_name,
  cartridge,
  capacity_in_bytes,
  primary_density_code,
  secondary_density_code,
  nb_wraps,
  min_lpos,
  max_lpos,
  user_comment,
  creation_log_user_name,
  creation_log_host_name,
  creation_log_time,
  last_update_user_name,
  last_update_host_name,
  last_update_time
) values (
  (select nextval('media_type_id_seq')),
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s
)
"""

def insert_cta_media_types(cta_db):
    for key, value in CTA_MEDIA_TYPES.items():
        res = insert(cta_db,
                     INSERT_MEDIA_TYPES,
                     (value["media_type_name"],
                      value["cartridge"],
                      value["capacity_in_bytes"],
                      value["primary_density_code"],
                      value["secondary_density_code"],
                      value["nb_wraps"],
                      value["min_lpos"],
                      value["max_lpos"],
                      value["user_comment"],
                      value["creation_log_user_name"],
                      value["creation_log_host_name"],
                      value["creation_log_time"],
                      value["last_update_user_name"],
                      value["last_update_host_name"],
                      value["last_update_time"]))


SELECT_LIBRARIES = """
select distinct library as library
from volume
  where active_files>0
        and system_inhibit_0 = 'none'
        and library not like 'shelf%'
        and media_type in ('LTO8', 'M8', 'LTO9')
--        and storage_group != 'cms'
"""

SELECT_LIBRARIES_FOR_VO = """
select distinct storage_group, library, file_family
from volume
  where active_files>0
        and system_inhibit_0 = 'none'
        and library not like 'shelf%%'
        and media_type in ('LTO8', 'M8', 'LTO9')
--        and file_family not like '%%-MIGRATION%%'
        and storage_group = %s
"""

SELECT_LIBRARIES_FOR_ALL_VOS = """
select distinct storage_group, library, file_family
from volume
  where active_files>0
        and system_inhibit_0 = 'none'
        and library not like 'shelf%%'
        and media_type in ('LTO8', 'M8', 'LTO9')
--        and file_family not like '%%-MIGRATION%%'
"""


SELECT_STORAGE_CLASSES = """
select distinct storage_group||'.'||file_family as storage_class
from volume
  where active_files>0
        and media_type in ('LTO8', 'M8', 'LTO9')
        and system_inhibit_0 = 'none'
        and library not like 'shelf%'
        and file_family not like '%_copy_1'
--        and storage_group != 'cms'
"""

SELECT_MULTIPLE_COPY_STORAGE_CLASSES = """
select distinct storage_group||'.'||file_family as storage_class
from volume
  where active_files>0
        and media_type in ('LTO8', 'M8', 'LTO9')
        and system_inhibit_0 = 'none'
        and library not like 'shelf%'
        and file_family like '%_copy_1'
--        and storage_group != 'cms'
"""


SELECT_VOS = """
select distinct storage_group from volume
  where active_files>0
        and system_inhibit_0 = 'none'
        and library not like 'shelf%'
        and file_family not like '%_copy_1'
--        and storage_group != 'cms'
"""

#
# pick up only "primary" volmes that do nopt have
# '_copy_1' suffix in file_family name
#

SELECT_ALL_ENSTORE_VOLUMES = """
select label from volume
  where media_type in ('LTO8', 'M8', 'LTO9')
        and system_inhibit_0 = 'none'
        and library not like 'shelf%'
        and file_family not like '%_copy_1'
        and active_files > 0
--        and storage_group != 'cms'
        order by label asc
"""


SELECT_ENSTORE_FILES_FOR_VOLUME = """
select f.*,
       v.storage_group||'.'||v.file_family||'@cta' as storage_class,
       v.wrapper
from file f inner join volume v
  on v.id = f.volume
  where
        v.media_type in ('LTO8', 'M8', 'LTO9')
        and v.system_inhibit_0 = 'none'
        and v.label = %s
        and v.active_files > 0
        and f.deleted = 'n'
        order by f.location_cookie
"""

SELECT_ENSTORE_FILES_FOR_VOLUME_WITH_COPY = """
select f.*,
       v.storage_group||'.'||v.file_family||'@cta' as storage_class,
       v.wrapper as original_wrapper,
       f1.bfid as copy_bfid,
       f1.location_cookie as copy_location_cookie,
       f1.deleted as copy_deleted,
       v1.*
from file f
inner join volume v on v.id = f.volume
left outer join file_copies_map fcm on fcm.bfid = f.bfid
left outer join file f1 on f1.bfid = fcm.alt_bfid
left outer join volume v1 on v1.id = f1.volume
  where
        v.media_type in ('LTO8', 'M8', 'LTO9')
        and v.system_inhibit_0 = 'none'
        and v.label = %s
        and v.active_files > 0
        and (f1.deleted is null or f1.deleted = 'n')
        and f.deleted = 'n'
        order by f.pnfs_id
"""

# Enstore to CTA media_type map.
# Entries in CTA are expected to exist.
media_type_map = {
    "LTO8" : "LTO8",
    "M8" : "LTO7M",
    "LTO9" : "LTO9"
}

printLock = multiprocessing.Lock()


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


def file_location_to_sequence(location):
    return ( location - 2 ) / 3 + 1


def extract_file_number_old(enstore_file):
    """
    enstore_file is a dictionary
    expected to have location_cookie and wrapper fields
    """
    fseq = int(enstore_file["location_cookie"].split("_")[2])
    if enstore_file["wrapper"] == "cern":
        #
        # when CERN wrapper is used the records look like
        # HFT ("HeaderFileTrailer")
        # Enstore stores actual location of the file as location_cookie
        # CTA stores so called sequence number, which is
        # the triplet number on a tape:
        #
        # Enstore location cookie:  2  5  8
        #                          HFTHFTHFT
        # CTA sequence number:      1  2  3
        #
        fseq = file_location_to_sequence(fseq)
    return fseq


def extract_file_number(location_cookie, wrapper):
    fseq = int(location_cookie.split("_")[2])
    if wrapper == "cern":
        #
        # when CERN wrapper is used the records look like
        # HFT ("HeaderFileTrailer")
        # Enstore stores actual location of the file as location_cookie
        # CTA stores so called sequence number, which is
        # the triplet number on a tape:
        #
        # Enstore location cookie:  2  5  8
        #                          HFTHFTHFT
        # CTA sequence number:      1  2  3
        #
        fseq = file_location_to_sequence(fseq)
    return fseq


def extract_eod(enstore_volume):
    """
    enstore_volume is a dictionary
    expected to have wrapper end eod_cookie fields
    """
    eod = int(enstore_volume["eod_cookie"].split("_")[2]) - 1
    if enstore_volume["wrapper"] == "cern":
        #
        # when CERN wrapper is used the records look like
        # HFT ("HeaderFileTrailer")
        # Enstore stores actual location of the file as location_cookie
        # CTA stores so called sequence number, which is
        # the triplet number on a tape:
        #
        # Enstore location cookie:  2  5  8
        #                          HFTHFTHFT
        # CTA sequence number:      1  2  3
        #
        eod = file_location_to_sequence(eod)
    return eod


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


INSERT_DISK_INSTANCE = """
insert into disk_instance (
  disk_instance_name,
  user_comment,
  creation_log_user_name,
  creation_log_host_name,
  creation_log_time,
  last_update_user_name,
  last_update_host_name,
  last_update_time
) values (
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s
)
"""

def insert_disk_instance(cta_db, disk_instance_name):
    res = insert(cta_db,
                 INSERT_DISK_INSTANCE,
                 (disk_instance_name,
                  disk_instance_name,
                  getpass.getuser(),
                  HOSTNAME,
                  int(time.time()),
                  getpass.getuser(),
                  HOSTNAME,
                  int(time.time())))



INSERT_LOGICAL_LIBRARY = """
insert into logical_library (
  logical_library_id,
  logical_library_name,
  is_disabled,
  disabled_reason,
  user_comment,
  creation_log_user_name,
  creation_log_host_name,
  creation_log_time,
  last_update_user_name,
  last_update_host_name,
  last_update_time
  ) values (
  (select nextval('logical_library_id_seq')),
  %s,
  '0',
  null,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s
  )
"""


def insert_logical_libraries_old(enstore_db, cta_db):
    #
    # create logical libraries in CTA mamed the same Enstore libraries
    #
    logical_libraries = enstore_libraries = get_enstore_libraries(enstore_db)
    for library in enstore_libraries:
        try:
            res = insert(cta_db,
                         INSERT_LOGICAL_LIBRARY,
                         (library,
                          "Imported from Enstore %s" % (library, ),
                          getpass.getuser(),
                          HOSTNAME,
                          int(time.time()),
                          getpass.getuser(),
                          HOSTNAME,
                          int(time.time())))
        except psycopg2.IntegrityError:
            print_message(f"Logical library {library} already exists")
            pass
    return enstore_libraries


def insert_logical_libraries(cta_db, config):
    logical_libraries = set(config.get("library_map").values())
    for library in logical_libraries:
        try:
            res = insert(cta_db,
                         INSERT_LOGICAL_LIBRARY,
                         (library,
                          "Imported from Enstore %s" % (library, ),
                          getpass.getuser(),
                          HOSTNAME,
                          int(time.time()),
                          getpass.getuser(),
                          HOSTNAME,
                          int(time.time())))
        except psycopg2.IntegrityError:
            print_message(f"Logical library {library} already exists")
            pass
    return logical_libraries


INSERT_VO = """
insert into virtual_organization (
  virtual_organization_id,
  virtual_organization_name,
  read_max_drives,
  write_max_drives,
  max_file_size,
  user_comment,
  creation_log_user_name,
  creation_log_host_name,
  creation_log_time,
  last_update_user_name,
  last_update_host_name,
  last_update_time,
  disk_instance_name
) values (
  (select nextval('virtual_organization_id_seq')),
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s)
"""

def insert_vos(enstore_db, cta_db, disk_instance_name):
    vos = select(enstore_db,
                 SELECT_VOS)
    for row in vos:
        vo = row["storage_group"]
        try:
            res = insert(cta_db,
                         INSERT_VO,
                         (vo,
                          2, #FIXME read_max_drives
                          2, #FIXME write_max_drives
                          10*(1<<40), # 10 TB
                          "Imported from Enstore",
                          getpass.getuser(),
                          HOSTNAME,
                          int(time.time()),
                          getpass.getuser(),
                          HOSTNAME,
                          int(time.time()),
                          disk_instance_name))
        except psycopg2.IntegrityError:
            print_message(f"VO {vo} already exists")
            pass
    return [row["storage_group"] for row in vos]


INSERT_STORAGE_CLASS = """
insert into storage_class (
  storage_class_id,
  storage_class_name,
  nb_copies,
  virtual_organization_id,
  user_comment,
  creation_log_user_name,
  creation_log_host_name,
  creation_log_time,
  last_update_user_name,
  last_update_host_name,
  last_update_time
) values (
  (select nextval('storage_class_id_seq')),
  %s,
  %s,
  (select virtual_organization_id from virtual_organization where virtual_organization_name = %s),
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s
)
"""

def insert_storage_class(cta_db, storage_class, vo, number_of_copies=1):
    try:
        res = insert(cta_db,
                     INSERT_STORAGE_CLASS,
                     (storage_class+"@cta",
                      number_of_copies,
                      vo,
                      "Imported from Enstore",
                      getpass.getuser(),
                      HOSTNAME,
                      int(time.time()),
                      getpass.getuser(),
                      HOSTNAME,
                      int(time.time())))
    except psycopg2.IntegrityError:
        print_message(f"Storage class {storage_class} already exists")
        pass


def insert_storage_classes(enstore_db, cta_db):
    multiple_copy_storge_classes = select(enstore_db,
                                          SELECT_MULTIPLE_COPY_STORAGE_CLASSES)

    added_classes = {}
    number_of_copies = 2
    for row in multiple_copy_storge_classes:
        storage_class = row["storage_class"]
        storage_class = storage_class.rstrip("_copy_1")
        vo = storage_class.split(".")[0]
        insert_storage_class(cta_db, storage_class, vo, number_of_copies)
        added_classes[storage_class] = number_of_copies

    storage_classes = select(enstore_db,
                            SELECT_STORAGE_CLASSES)
    number_of_copies = 1

    for row in storage_classes:
        storage_class = row["storage_class"]
        if storage_class not in added_classes:
            vo = storage_class.split(".")[0]
            insert_storage_class(cta_db, storage_class, vo, number_of_copies)
            added_classes[storage_class] = number_of_copies
    return added_classes


INSERT_TAPE_POOL = """
insert into tape_pool (
  tape_pool_id,
  tape_pool_name,
  virtual_organization_id,
  nb_partial_tapes,
  is_encrypted,
  supply,
  user_comment,
  creation_log_user_name,
  creation_log_host_name,
  creation_log_time,
  last_update_user_name,
  last_update_host_name,
  last_update_time,
  encryption_key_name
  ) values (
  (select nextval('tape_pool_id_seq')),
  %s,
  (select virtual_organization_id from virtual_organization where virtual_organization_name = %s),
  %s,
  '0',
  null,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  null)
"""

def insert_tape_pools(cta_db, storage_classes):
    for storage_class, number_of_copies in storage_classes.items():
        vo, file_family = storage_class.split(".")
        user_comment = "Pool for %s" % (storage_class,)
        tape_pool_name = storage_class
        for i in range(number_of_copies):
            try:
                res = insert_returning(cta_db,
                                       INSERT_TAPE_POOL,
                                       (tape_pool_name,
                                        "%s" % (vo,),
                                        0,
                                        user_comment,
                                        getpass.getuser(),
                                        HOSTNAME,
                                        int(time.time()),
                                        getpass.getuser(),
                                        HOSTNAME,
                                        int(time.time())))
            except psycopg2.IntegrityError:
                pass
            finally:
                user_comment = "Pool for %s copy %s" % (storage_class, str(i+1),)
                tape_pool_name = "%s_copy_%s" % (storage_class, str(i+1),)



INSERT_ARCHIVE_ROUTE = """
insert into archive_route (
  storage_class_id,
  copy_nb,
  tape_pool_id,
  user_comment,
  creation_log_user_name,
  creation_log_host_name,
  creation_log_time,
  last_update_user_name,
  last_update_host_name,
  last_update_time
) values (
  (select storage_class_id from storage_class where storage_class_name = %s),
  %s,
  (select tape_pool_id from tape_pool where tape_pool_name = %s),
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s
)
"""

def insert_archive_routes(cta_db,
                          storage_classes):

    for storage_class, number_of_copies in storage_classes.items():
        vo, file_family = storage_class.split(".")
        tape_pool_name = storage_class
        for i in range(number_of_copies):
            try:
                res = insert(cta_db,
                             INSERT_ARCHIVE_ROUTE,
                             (storage_class + "@cta",
                              i+1,
                              tape_pool_name,
                              "Archive route for tape pool %s, %s" % (storage_class, str(i+1),),
                              getpass.getuser(),
                              HOSTNAME,
                              int(time.time()),
                              getpass.getuser(),
                              HOSTNAME,
                              int(time.time())))
            except Exception as e:
                print_message("Failed to insert archive_route for %s %s" %
                              (storage_class, str(e)))
                pass
            finally:
                tape_pool_name = "%s_copy_%s" % (storage_class, str(i+1),)



INSERT_ARCHIVE_FILE = """
insert into archive_file (
  archive_file_id,
  disk_instance_name,
  disk_file_id,
  disk_file_uid,
  disk_file_gid,
  size_in_bytes,
  checksum_blob,
  checksum_adler32,
  storage_class_id,
  creation_time,
  reconciliation_time,
  is_deleted,
  collocation_hint
) values (
  (select nextval ('archive_file_id_seq')),
  (select disk_instance_name from disk_instance where disk_instance_name = %s),
  %s,
  %s,
  %s,
  %s,
  null,
  %s,
  (select storage_class_id from storage_class where storage_class_name = %s),
  %s,
  %s,
  %s,
  null
)
"""

INSERT_TAPE_FILE = """
insert into tape_file (
  vid,
  fseq,
  block_id,
  logical_size_in_bytes,
  copy_nb,
  creation_time,
  archive_file_id
) values (
  %s,
  %s,
  %s,
  %s,
  %s,
  %s,
  %s
)
"""

def insert_cta_file(connection, enstore_file, cta_label, config):
    file_create_time = int(enstore_file["bfid"][4:14])
    file_size = enstore_file["size"]
    file_crc = enstore_file["crc"]
    #
    # take care of "adler32 seeed 0" nonsense
    #
    if file_create_time < get_switch_epoch() and HOSTNAME.endswith(".fnal.gov"):
        file_crc =  convert_0_adler32_to_1_adler32(file_crc, file_size)

    cta_file = insert_returning(connection,
                                INSERT_ARCHIVE_FILE,(
                                    config.get("disk_instance_name"),
                                    enstore_file["pnfs_id"],
                                    enstore_file["uid"],
                                    enstore_file["gid"],
                                    file_size,
                                    file_crc,
                                    enstore_file["storage_class"],
                                    file_create_time,
                                    int(time.time()),
                                    '0'
                                ))
    archive_file_id = int(cta_file["archive_file_id"])
    location_cookie = enstore_file["location_cookie"]
    wrapper = enstore_file["original_wrapper"]
    fseq = extract_file_number(location_cookie, wrapper)
    res = insert(connection,
                 INSERT_TAPE_FILE, (
                     cta_label,
                     fseq,
                     fseq,
                     file_size,
                     1,
                     file_create_time,
                     archive_file_id))
    return archive_file_id

def insert_cta_tape_file_copy(connection,
                              archive_file_id,
                              enstore_file,
                              config):
    file_create_time = int(enstore_file["copy_bfid"][4:14])
    location_cookie = enstore_file["copy_location_cookie"]
    wrapper = enstore_file["wrapper"]
    fseq = extract_file_number(location_cookie, wrapper)

    res = insert(connection,
                 INSERT_TAPE_FILE, (
                     enstore_file["label"][:6],
                     fseq,
                     fseq,
                     enstore_file["size"],
                     2, # copy number
                     file_create_time,
                     archive_file_id))

INSERT_CTA_TAPE = """
insert into tape (
   vid,  media_type_id, vendor, logical_library_id, tape_pool_id,
   encryption_key_name, data_in_bytes, last_fseq, nb_master_files,
   master_data_in_bytes, is_full, is_from_castor, dirty,
   nb_copy_nb_1, copy_nb_1_in_bytes,  nb_copy_nb_gt_1,
   copy_nb_gt_1_in_bytes, label_format, label_drive, label_time,
   last_read_drive, last_read_time, last_write_drive, last_write_time,
   read_mount_count, write_mount_count, user_comment,
   tape_state, state_reason, state_update_time, state_modified_by,
   creation_log_user_name, creation_log_host_name, creation_log_time,
   last_update_user_name, last_update_host_name, last_update_time,
   verification_status)
   values (%s,
           (select media_type_id from media_type where media_type_name = %s),
           'Unknown',
           (select logical_library_id from logical_library where logical_library_name = %s),
           (select tape_pool_id from tape_pool where tape_pool_name = %s),
           '',
           %s,
           %s,
           %s,
           %s,
           '1',
           '0',
           '0',
           %s,
           %s,
           0,
           0,
           %s,
           'Enstore',
           %s,
           '',
           %s,
           'Enstore',
           %s,
           %s,
           %s,
           %s,
           'ACTIVE',
           'Migrated from Enstore',
           %s,
           %s,
           %s,
           %s,
           %s,
           %s,
           %s,
           %s,
           ''
   )
"""

# label_format is just before 'Enstore' above

def insert_cta_tape(connection, enstore_volume, config):
    vo = enstore_volume["storage_group"]
    logical_library_name = enstore_volume["library"]
    file_family =  enstore_volume["file_family"]
    tape_pool_name = "%s.%s" % (vo, file_family,)

    if config.get("library_map"):
        try:
            logical_library_name = config.get("library_map")[logical_library_name]
        except KeyError:
            raise

    label_format = "2"
    if enstore_volume["wrapper"] == "cern":
        label_format = "3"

    res = insert(connection,
                 INSERT_CTA_TAPE,(
                     enstore_volume["label"][:6],
                     config.get("media_type_map")[enstore_volume["media_type"]],
                     logical_library_name,
                     config.get("tape_pool_name", tape_pool_name),
                     enstore_volume["active_bytes"],
                     #extract_file_number(enstore_volume["eod_cookie"]) - 1,
                     extract_eod(enstore_volume),
                     enstore_volume["active_files"],
                     enstore_volume["active_bytes"],
                     enstore_volume["active_files"],
                     enstore_volume["active_bytes"],
                     label_format,
                     int(time.mktime(enstore_volume["declared"].timetuple())),
                     int(time.mktime(enstore_volume["last_access"].timetuple())),
                     int(time.mktime(enstore_volume["last_access"].timetuple())),
                     min(enstore_volume["sum_rd_access"], enstore_volume["sum_mounts"]),
                     min(enstore_volume["sum_wr_access"], enstore_volume["sum_mounts"]),
                     ("Migrated from Enstore: %s" % (enstore_volume["comment"],))[:1000],
                     int(time.time()),
                     getpass.getuser(),
                     getpass.getuser(),
                     HOSTNAME,
                     int(time.time()),
                     getpass.getuser(),
                     HOSTNAME,
                     int(time.time())
                     ))
    return res


SELECT_CTA_LOCATION = """
select 'cta://cta/'||af.disk_file_id||'?archiveid='||af.archive_file_id as
location from archive_file af
        INNER JOIN tape_file tf on tf.archive_file_id = af.archive_file_id
  WHERE
      af.disk_file_id = %s
"""


def get_cta_location(connection, enstore_file):
    location  = select(connection,
                       SELECT_CTA_LOCATION,
                       (enstore_file["pnfs_id"],))
    if location:
        return location[0]["location"]
    else:
        return None


INSERT_CHIMERA_LOCATION = """
insert into t_locationinfo (inumber, itype, ipriority, ictime, iatime, istate, ilocation)
   values (
   (select inumber from t_inodes where ipnfsid = %s),
   0,
   10,
   now(),
   now(),
   1,
   %s)
"""

def insert_chimera_location(connection, enstore_file, location):
    res = insert(connection,
                 INSERT_CHIMERA_LOCATION,
                 (enstore_file["pnfs_id"],
                  location,))
    return res


UPDATE_COPY_COUNTS = """
update tape
   set nb_copy_nb_1 = t.nb_copy_nb_1,
       copy_nb_1_in_bytes = t.copy_nb_1_in_bytes,
       nb_copy_nb_gt_1 = t.nb_copy_nb_gt_1,
       copy_nb_gt_1_in_bytes = t.copy_nb_gt_1_in_bytes
from
   (select tf.vid as vid,
      sum(case when tf.copy_nb > 1 then af.size_in_bytes else 0 end) as copy_nb_gt_1_in_bytes,
      sum(case when tf.copy_nb = 1 then af.size_in_bytes else 0 end) as copy_nb_1_in_bytes,
      sum(case when tf.copy_nb > 1 then 1 else 0 end) as nb_copy_nb_gt_1,
      sum(case when tf.copy_nb = 1 then 1 else 0 end) as nb_copy_nb_1
    from archive_file af
       inner join tape_file tf on tf.archive_file_id = af.archive_file_id
    group by tf.vid) as t
    where t.vid = tape.vid
"""

def update_cta_copy_counts(cta_db):
    res = update(cta_db, UPDATE_COPY_COUNTS)
    return res


def get_library_map(enstore_db):
    res = select(enstore_db,
                 SELECT_LIBRARIES_FOR_ALL_VOS)
    vos = {}
    for row in res:
        vo = row["storage_group"]
        if vo not in vos:
            vos[vo] = []
        vos[vo].append({
            "library" : row["library"],
            "file_family"  : row["file_family"]
        })
    return vos


class Worker(multiprocessing.Process):
    """
    Class that processed individual enstore volume
    """
    def __init__(self, queue, config):
        super(Worker, self).__init__()
        self.queue = queue
        self.config = config

    def run(self):
        enstore_db, cta_db, chimera_db = None, None, None
        try:
            # enstore db
            enstore_db = create_connection(self.config.get("enstore_db"))
            # cta db
            cta_db = create_connection(self.config.get("cta_db"))
            # chimera_db
            chimera_db = create_connection(self.config.get("chimera_db"))

            added_copy_volumes = set()
            for label in iter(self.queue.get, None):
                cta_label = label[:6]
                print_message("Doing label %s" % (label, ))
                enstore_volumes = select(enstore_db,
                                         "select * from volume where label=%s",
                                         (label,))
                if not enstore_volumes:
                    print_error("No such volume %s" % (label, ))
                    continue
                enstore_volume = enstore_volumes[0]
                try:
                    res = insert_cta_tape(cta_db, enstore_volume, self.config)
                except KeyError:
                    print_error("Failed to insert tape label %s because mapping for libary %s does not exist" % (enstore_volume["label"], enstore_volume["library"],))
                    continue
                except psycopg2.IntegrityError:
                    # except psycopg2.IntegrityError as e:
                    # print_error("%s already exist, skipping, %s " %
                    #             (enstore_volume["label"], str(e)))
                    print_error(f"{label} Done, aleady exists, skipping")
                    continue
                files = select(enstore_db,
                               SELECT_ENSTORE_FILES_FOR_VOLUME_WITH_COPY,
                               (label, ))
                for f in files:
                    try:
                        archive_file_id = insert_cta_file(cta_db,
                                                          f,
                                                          cta_label,
                                                          self.config)
                        #
                        # do we have a copy
                        #
                        copy_label = f.get("label")
                        if copy_label:
                            if copy_label not in added_copy_volumes:
                                added_copy_volumes.add(copy_label)
                                try:
                                    res = insert_cta_tape(cta_db,
                                                          f,
                                                          self.config)
                                    print_message("%s added label containing "
                                                  "copies  %s" % (label,
                                                                  copy_label,))
                                except psycopg2.IntegrityError:
                                    pass
                            try:
                                if f["copy_deleted"] == "n":
                                    insert_cta_tape_file_copy(cta_db,
                                                              archive_file_id,
                                                              f,
                                                              self.config)
                            except Exception as e:
                                print_error("%s Failed to insert tape_file, %s"
                                            " %s %s %s, skipping %s" %
                                            (label,
                                             f["label"],
                                             f["pnfs_id"],
                                             f["bfid"],
                                             f["copy_bfid"],
                                             str(e)))
                                pass

                        if not self.config["skip_locations"]:

                            location = "cta://cta/%s?archiveid=%d" % (f["pnfs_id"],
                                                                      archive_file_id,)
                            try:
                                res = insert_chimera_location(chimera_db, f, location)
                            except Exception as e:
                                print_error("%s %s failed to insert location into chimera DB %s, %s" %
                                            (label, f["pnfs_id"], location, str(e),))
                                pass

                    except psycopg2.IntegrityError:
                    #except Exception as e:
                        print_error("%s, failed to insert archive_file, multiple pnfsid, skipping %s" %
                                    (enstore_volume["label"], f["pnfs_id"], ))
                        continue
                print_message("%s Done, %d files" %(label, len(files),))
        except Exception as e:
            print_message("Exception %s" % (str(e)))
        finally:
            for i in (enstore_db, cta_db, chimera_db):
                if i:
                    try:
                        i.close()
                    except:
                        pass



def update(con, sql, pars=None):
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


def insert(con, sql, pars=None):
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
        if pars:
            res = cursor.execute(sql, pars)
        else:
            res = cursor.execute(sql)
        con.commit()
        return res
    except Exception:
        con.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass

def insert_returning(con, sql, pars=None):
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
        sql +=  "returning *"
        cursor = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if pars:
            cursor.execute(sql, pars)
        else:
            cursor.execute(sql)
        res = cursor.fetchone()
        con.commit()
        return res
    except Exception:
        con.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


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


def parse_enstore_config(file_name):
    #
    # Parse enstore config
    #
    configdict = {}
    with open(file_name, "r") as f:
        lines = "".join(f.readlines())
        exec(lines)
    return configdict


def get_enstore_libraries(enstore_db):
    rows = select(enstore_db,
                  SELECT_LIBRARIES)
    libraries = [row["library"] for row in rows]
    return libraries


#    library_keys = [i for i in enstore_config.keys() if i.endswith(".library_manager")]
#    movers_keys = [i for i in enstore_config.keys() if i.endswith(".mover")]
#
#    libraries = {}
#    for library in library_keys:
#        for mover in movers_keys:
#            if enstore_config[mover].get("library") == library:
#                short_name = library.rstrip(".library_manager")
#                libraries[short_name] = libraries.get(short_name, 0) + 1
#    return libraries


def main():

    """
    main function
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="This script converts Enstore metadata to CTA metadata. "
        "It looks for YAML configuration file pointed to by MIGRATION_CONFIG "
        "environment variable or, if it is not defined, it looks for file enstore2cta.yaml "
        "in current directory. Script will quit if configuration YAML is not found. "
        )

    parser.add_argument(
        "--label",
        help="comma separated list of labels")

    parser.add_argument(
        "--all",
        help="do all labels",
        action="store_true")

    parser.add_argument(
        "--skip_locations",
        help="skip filling chimera locations (good for testing)",
        action="store_true")

    parser.add_argument(
        "--add",
        help="add volume(s) to existing system, do not create vos, pools, archive_routes etc. These need to pre-exist in CTA db",
        action="store_true")

    parser.add_argument(
        "--storage_class",
        help="Add storage class corresponding to volume. Needed when adding single volume to existing system using --add option")

    parser.add_argument(
        "--vo",
        help="vo corresponding to storage_class. Needed when adding single volume to existing system using --add option")

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

    configuration["skip_locations"] = args.skip_locations
    print (configuration)

    if args.label and args.all:
        parser.print_help(sys.stderr)
        sys.exit(1)

    if not args.label and not args.all:
        parser.print_help(sys.stderr)
        sys.exit(1)

    cta_db, enstore_db, chimera_db = None, None, None

    try:
        cta_db = create_connection(configuration.get("cta_db"))
    except:
        print_error("Failed to initialize connection to cta_db, quitting")
        sys.exit(1)

    try:
        enstore_db = create_connection(configuration.get("enstore_db"))
    except:
        print_error("Failed to initialize connection to enstore_db, quitting")
        sys.exit(1)

    try:
        chimera_db = create_connection(configuration.get("chimera_db"))
        chimera_db.close()
    except:
        print_error("Failed to initialize connection to chimera_db, quitting")
        sys.exit(1)

    if args.add:
        if args.storage_class and args.vo:
            cta_db = create_connection(configuration.get("cta_db"))
            res = insert_storage_class(cta_db,
                                       args.storage_class,
                                       args.vo,
                                       1)

    labels = None
    if args.label:
        labels = [i.upper() for i in args.label.strip().split(",")]

    if args.all:
        enstore_db = create_connection(configuration.get("enstore_db"))
        cursor = enstore_db.cursor()
        cursor.execute(SELECT_ALL_ENSTORE_VOLUMES)
        labels = [i[0] for i in cursor.fetchall()]
        if cursor:
            cursor.close()

    if not labels:
         print_error("**** No labels found, quitting ***")
         sys.exit(1)

    if not args.add:
        try:
            insert_cta_media_types(cta_db)
        except:
            pass

        try:
            insert_disk_instance(cta_db,
                                 disk_instance_name=configuration.get("disk_instance_name"))
        except psycopg2.IntegrityError as e:
            print_message("Disk instrance {} aleady exists, not an error".
                          format(configuration.get("disk_instance_name")))
            pass

        vos = insert_vos(enstore_db,
                         cta_db,
                         disk_instance_name=configuration.get("disk_instance_name"))
        #libraries = insert_logical_libraries_old(enstore_db, cta_db)
        libraries = insert_logical_libraries(cta_db, configuration)
        storage_classes = insert_storage_classes(enstore_db, cta_db)
        insert_tape_pools(cta_db, storage_classes)
        insert_archive_routes(cta_db,
                              storage_classes)
        enstore_db.close()
        cta_db.close()

    print_message("**** Start processing %d  labels ****" % (len(labels), ))
    t0 = time.time()

    queue = multiprocessing.Queue(10000)
    workers = []
    #cpu_count = multiprocessing.cpu_count()
    cpu_count = args.cpu_count

    for i in range(cpu_count):
        worker = Worker(queue, configuration)
        workers.append(worker)
        worker.start()

    for label in labels:
        queue.put(label)

    for i in range(cpu_count):
        queue.put(None)

    for worker in workers:
        worker.join()

    print_message("Finished file migration, bootstrapping tapes copies counts")

    try:
        cta_db = create_connection(configuration.get("cta_db"))
        res = update_cta_copy_counts(cta_db)
    except:
        print_error("Failed to connect to cta_db, quitting")
        sys.exit(1)
    finally:
        if cta_db:
            cta_db.close()

    print_message("**** FINISH ****")
    print_message("Took %d seconds" % (int(time.time()-t0+0.5),))


if __name__ == "__main__":
    main()
