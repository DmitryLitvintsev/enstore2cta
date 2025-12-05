SFA files
=========

One of the issues that has been identified - CTA does not have
functionality corresponding to Enstore SFA (Small File Aggregation).
In the nutshell the SFA system is as extension of Enstore system that
manages  intermediate disk storage on the side (intermediate between
dCache and Enstore). Depending on policies based on ``file_family``,
``storage_group``, ``library`` and file size Enstore directs files
to the intermediate storage for subsequent periodic packaging - tarring
the small files into large package files that then are written to
tape more efficiently.

The child/parent relation is captured in the same ``file`` table by
setting child's ``file.package_id`` to be equal to BFID of the package file.

To read SFA files in dCache/CTA setup this relation has to translate in
chimera.

There is a solution for it, used by similar to SFA, SAPPHIRE system by dCache.
We need to translate::

 child_pnfsid, package_pnfsid ->
    -> dcache://dcache/?store=<vo>&group=<file_family>&bfid=<child_pnfsid>:<package_pnfsid>

I.e. the child/package relation exists as location in ``t_locationinfo`` Chimera
table. As long as these locations exist dCache can read these files from CTA using an hsm script. T.e. SAPPHIRE system is not need for reading of SFA files.

This can be populated out of band.

After some iterations, the final SFA location is expressed as::

 sfa://sfa/<child_pnfsid>?packageid=<parent_pnfsid>

The script sfa2dcache.py, located in enstore2cta/scripts,
implements SFA metadata migration from Enstore DB to dCache DB.

Invocation
----------

To run the scripqt a config file ``enstore2cta.yaml`` *must* exist in
the current directory or be pointed at by ``MIGRATION_CONFIG`` environment variable.
Look for example in ``enstore2cta/etc``. It must have "0600" permission (to protect database passwords if any).

The configuration yaml must have connection parameters to enstoredb and chimeradb
defined with the latter being write (insert, update) enabled.

::

 #  python3 sfa2dcache.py  --help
 usage: sfa2dcache.py [-h] [--dir DIR] [--cpu_count CPU_COUNT]

 optional arguments:
   -h, --help            show this help message and exit
   --dir DIR             top directory name
   --cpu_count CPU_COUNT
                         override cpu count - number of simultaneously processed labels

Where top directry name is the name of directory where Enstore stores package
files (typically `/pnfs/fs/usr/file_aggregation/`)
