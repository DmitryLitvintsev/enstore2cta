enstore2cta - Enstore to CTA migration script
=============================================

The script ``enstore2cta.py``, located in ``enstore2cta/scripts``, implements
database migration from Enstore DB to CTA DB. Both databases must be
`PostgreSQL` databases. The script has various steering options (see below).
It spawns multiple processes, each process processing a unique Enstore volume.


Requirements
------------


The script works both with python2 and python3 and requires ``psycopg2`` module be installed (using ``pip`` or ``yum install python-psycopg2``).


Invocation
----------
To run the script a config file ``enstore2cta.yaml`` *must* exist in
the current directory or be pointed at by ``MIGRATION_CONFIG`` environment variable.
Look for example in ``enstore2cta/etc``. It must have "0600" permission (to protect database passwords if any).

::

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
                         override cpu count - number of simultaneously processed
                         labels (default: 8)
                         single volume to existing system using --add option


(default cpu_count is equal to ``multiprocessing.cpu_count()``)

The script can work with individual label(s) passed as comma separated values to ``--label`` option. Or it can be invoked with ``--all`` switch to migrate all labels. The migration is done by label.

Additionally, on an existing CTA system one can use
``--add`` option to add a volume also specifying its ``--storage_class`` (e.g. "cms.foo") and ``--vo`` (e.g. "cms").
