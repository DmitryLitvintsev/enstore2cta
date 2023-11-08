# enstore2cta

enstore2cta - Enstore to CTA migration script
=============================================

Documentation
-------------

https://dmitrylitvintsev.github.io/enstore2cta/


Requirements
------------

Script works both with python2 and python3 and requires `psycopg2` module be installed (using `pip` or `yum install python-psycopg2`).


Invocation
----------
To run the script a config file `enstore2cta.yaml` *must* exist in
current directory or be pointed at by MIGRATION_CONFIG environment variable.
Look for example in enstore2cta/etc. It must have "0600" permission


```

$ python enstore2cta.py
usage: enstore2cta.py [-h] [--label LABEL] [--all] [--skip_locations] [--add]
                      [--storage_class STORAGE_CLASS] [--vo VO]
                      [--cpu_count CPU_COUNT]

This script converts Enstore metadata to CTA metadata. It looks for YAML
configuration file pointed to by MIGRATION_CONFIG environment variable or, if
it is not defined, it looks for file enstore2cta.yaml in current directory.
Script will quit if configuration YAML is not found.

optional arguments:
  -h, --help            show this help message and exit
  --label LABEL         comma separated list of labels (default: None)
  --all                 do all labels (default: False)
  --skip_locations      skip filling chimera locations (good for testing)
                        (default: False)
  --add                 add volume(s) to existing system, do not create vos,
                        pools, archive_routes etc. These need to pre-exist in
                        CTA db (default: False)
  --storage_class STORAGE_CLASS
                        Add storage class corresponding to volume. Needed when
                        adding single volume to existing system using --add
                        option (default: None)
  --vo VO               vo corresponding to storage_class. Needed when adding
                        single volume to existing system using --add option
                        (default: None)
  --cpu_count CPU_COUNT
                        override cpu count - number of simulateously processed
                        labels (default: 8)
                        single volume to existing system using --add option
```

(default cpu_count is equal to `multiprocessing.cpu_count()`)

The script can work with individual label(s) passed as comma separated values to `--label` option. Or it can be invoked with `--all` switch to migrate all labels. The migration is done by label.

Additionally, on an existing CTA systenm one can use
`--add` option to add a volume also specifying its `--storage_class` (e.g. "cms.foo") and `--vo` (e.g. "cms").

Configuration
--------------

Script expects configuration file `enstore2cta.yaml` in current directory or pointed to by environment variable `MIGRATION_CONFIG`. The yaml file has to have "0600" permission bits and has to have the following parameters defned:

```
# name of the disk instance
# will be created if no "--add" option is used
disk_instance_name: dCache

# ucomment and modify when adding single volume
# to existing system usinf "--add" option
# tape_pool_name must exist in CTA
#tape_pool_name: ctasystest

# DB connection parameters,
# cta_db and chimera_db require r/w access
cta_db: postgresql://user:password@host:port/db_name
enstore_db: postgresql://user:password@host:port/db_name
chimera_db: postgresql://user:password@host:port/db_name

# Enstore to CTA media_type map.
media_type_map:
  LTO8: LTO8
  M8: LTO7M
  LTO9: LTO9

# map from Enstore LMs to CTA logical library name(s)
# this map is used if there is desire to map existing
# Enstore LMs to pre-created CTA logical libraries
# If this map is comemnted out Enstore LMs will be re-created
# as CTA logical libraries
# ucomment and modify  when adding single volume to existing system
# destination logical_library_name must exist in CTA
#
#library_map:
#  CD-LTO8F1: TS4500G1
#  CD-LTO8F1T: TS4500G1
#  CD-LTO8G1: TS4500G1
#  CD-LTO8G1T: TS4500G1
#  CD-LTO8G2: TS4500G1
#  CD-LTO8G2T: TS4500G1
#  CTA-TESTING: TS4500G1
#  TFF1-LTO9: TS4500G1
#  TFF1-LTO9T: TS4500G1
#  TFF2-LTO9: TS4500G1
#  TFF2-LTO9M: TS4500G1
#  TFF2-LTO9T: TS4500G1
#  TFF1-LTO8: TS4500G1

```
