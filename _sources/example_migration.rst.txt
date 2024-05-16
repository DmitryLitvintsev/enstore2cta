Example Migration
=================

Validation
----------

On storagedev201, run the following::

 python3 enstore2cta.py --label VR1871M8 --add > try.log 2>&1

Result::

 # tail try.log
 2023-11-06 12:41:01 INFO : **** Start processing 1  labels ****
 2023-11-06 12:41:01 INFO : Finished file migration, bootstrapping tapes copies counts
 2023-11-06 12:41:02 INFO : Doing label VR1871M8
 2023-11-06 12:41:02 INFO : **** FINISH ****
 2023-11-06 12:41:02 INFO : Took 0 seconds
 2023-11-06 12:56:24 INFO : VR1871M8 Done, 2338 files

"Took 0 seconds" because python3 does ``map(lambda x: x.join(), processes)``
not the way I expected (fixed later). Took about 15 minutes!!!. Identified several issues - storagdev201 was loaded, DB ifdb07 performing poorly, destination chimera db running on ITB was very slow. Conclusion - we could not draw any performance numbers from this setup. But the script has been proven to work.

Check that the file from that tape can be read by dCache::

 [fndcaitb3] (rw-stkendca28a-1@rw-stkendca28a-1Domain) enstore >  rh restore 000003042F3D23AF47BFB08A496A256A861C
 Fetch request queued.
 [fndcaitb3] (rw-stkendca28a-1@rw-stkendca28a-1Domain) enstore > rep ls  000003042F3D23AF47BFB08A496A256A861C
 000003042F3D23AF47BFB08A496A256A861C <---S-------L(0)[0]> 0 si={ssa_test.diskSF1T_in_LTO8G1T}

Observed CTA reading volume ``VR1871``, and after a while::

 [fndcaitb3] (rw-stkendca28a-1@rw-stkendca28a-1Domain) enstore > rep ls  000003042F3D23AF47BFB08A496A256A861C
 000003042F3D23AF47BFB08A496A256A861C <C-------X--L(0)[0]> 2097152000 si={ssa_test.diskSF1T_in_LTO8G1T}

 [fndcaitb3] (rw-stkendca28a-1@rw-stkendca28a-1Domain) enstore > pf 000003042F3D23AF47BFB08A496A256A861C
 /pnfs/fs/usr/ssa_test/CTA/large/7/dc0796f8-3b21-400b-8d9f-c066a80aca5c.data
 [fndcaitb3] (rw-stkendca28a-1@rw-stkendca28a-1Domain) enstore >

File was staged and marked cached on pool.


Full migration
--------------

Used fdm1903, 8 TB NVME drive. 24 cores.

1. use `pg_basebackup` to bring over chimera db from production
2. created CTA database from schema DDL
3. used production enstoredb as a source


CMS
---

::

 nohup  python enstore2cta_cms.py --all --skip_locations > cms.log 2>&1&

The ``--skip_locations`` was used because I did not have CMS chimera carried over.

Result: ::

 # tail cms.log
 2023-11-07 23:09:36 INFO : VRA931M8 Done, 3115 files
 2023-11-07 23:09:36 INFO : VRA930M8 Done, 3159 files
 2023-11-07 23:09:37 INFO : VRA932M8 Done, 3145 files
 2023-11-07 23:09:37 INFO : VRA934M8 Done, 3338 files
 2023-11-07 23:09:37 INFO : VRA936M8 Done, 3253 files
 2023-11-07 23:09:37 INFO : VRA937M8 Done, 3185 files
 2023-11-07 23:09:37 INFO : VRA938M8 Done, 3195 files
 2023-11-07 23:09:37 INFO : Finished file migration, bootstrapping tapes copies counts
 2023-11-07 23:10:44 INFO : **** FINISH ****
 2023-11-07 23:10:44 INFO : Took 2210 seconds

On db end: ::

 cta_cms=# select pg_size_pretty(pg_database_size('cta_cms'));
 pg_size_pretty
 ----------------
 16 GB
 (1 row)


 cta_cms=# select count(*) from archive_file;
   count
 ----------
  28193693
 (1 row)



Public
------

::

 nohup  python enstore2cta.py --all  > public.log 2>&1&

Above command does "everything" including inserting locations to chimera db.
Results: ::

 # tail public.log
 2023-11-07 21:04:48 INFO : VRA788M8 Done, 14591 files
 2023-11-07 21:04:49 INFO : VRA771M8 Done, 20641 files
 2023-11-07 21:04:53 INFO : VRA768M8 Done, 24322 files
 2023-11-07 21:04:53 INFO : VRA784M8 Done, 21471 files
 2023-11-07 21:05:01 INFO : VRA734M8 Done, 44020 files
 2023-11-07 21:05:56 INFO : VRA752M8 Done, 68899 files
 2023-11-07 21:06:03 INFO : VRA785M8 Done, 66068 files
 2023-11-07 21:06:03 INFO : Finished file migration, bootstrapping tapes copies counts
 2023-11-07 21:11:14 INFO : **** FINISH ****
 2023-11-07 21:11:14 INFO : Took 17813 seconds

On db end::

 cta_dev=# select count(*) from archive_file;
    count
 -----------
  151757273
 (1 row)

 cta_dev=# select pg_size_pretty(pg_database_size('cta_dev'));
  pg_size_pretty
 ----------------
  91 GB
 (1 row)


Minor limitation
----------------

During migration the value of comment in ``tape.user_comment`` is assigned the value ``"Migrated from Enstore: "+volume.comment``. The width of ``tape.user_comment`` is 1000 characters. Some of the comments on Enstore volumes exceed
``1000 - len("Migrated from Enstore: ")``::

 enstoredb=# select count(*), storage_group from volume
             where character_length(comment) > 1000-23
             group by storage_group order by count(*) desc;
  count | storage_group
 -------+---------------
     50 | nova
      1 | cms
 (2 rows)


This is solved by simply truncating comment string to 1000 before inserting.
