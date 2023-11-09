Enstore to CTA mapping
======================

.. list-table:: Enstore to CTA mapping
   :header-rows: 1

   * - Enstore
     - CTA
     - Comment
   * - ``volume.label``
     - ``vid = volume.label[:6]``
     -
   * - ``volume.storage_group``
     - ``virtual_organization_name``
     -
   * - | ``volume.storage_group``
       | ``volume.file_family``
     - ``storage_class.storage_class_name=volume.storage_group+"."+volume.file_family+"@cta"``
     - | This is needed so that dCache can
       | communicate to CTA and still use ``storage_class``
       | for data steering within dCache.
   * - ``volume.library``
     - ``logical_library_name``
     -
   * - ``file.bfid``
     - ``archive_file.archive_file_id``
     - Sequence in CTA
   * - ``file.pnfs_id``
     - ``archive_file.disk_file_id``
     -
   * - | if bfid has entry in
       | ``file_copies_map``
     - | ``storage_class.nb_copies``
       | ``archive_route.copy_nb``
       | And extra entries in ``tape_file``
     - | The ``storage_class.nb_copies`` is set to 2
       | if ``volume.file_family ~ '.*_copy_1'``
       | and or each enry in ``file_copies_map``
       | an extra entry is made in ``file_copies_map``
       | corresponding to file copy

The script ``enstore2cta.py`` running with ``--all`` options performs the following steps:
1. creates ``disk_instance`` with name corresponding to ``"disk_instance_name"``  key in configuration
   file ``enstore2cta.yaml``;
2. selects distinct names of ``volume.storage_group`` -> creates entries in ``virtual_organization``;
3. selects distinct names of ``volume.library`` -> creates CTA ``logical_library`` entries
   with the same names;
4. selects distinct ``volume.storage_group||'.'||volume.file_family||'@cta'`` -> creates corresponding
   entries in ``storage_class`` table. If ``volume.file_family ~ '.*_copy_1`` the ``nb_copies`` is set to 2;
5. for each vo creates ``tape_pool`` entry;
6. for each storage class and corresponding tape_pool (by vo) creates ``archive_route`` entry;
7. selects all Enstore volumes, that do not have ``"_copy_1"`` suffix and puts them on the Queue;
8. spwans number of processes (default - number of cores) and feeds volume to them one at a time via Queue;
9. each process:
  1. inserts volume into ``tape`` table;
  2. selects all active direct files, together with all their copies (if there are copies)
    from the ``file``, ``volume``, ``file_copies_map`` join
    and loops over them inserting entries into  ``archive_file`` and ``tape_file``, for each
    copy, it also makes an entry into ``tape`` for copy volume (does it only once for each
    new copy volume)  and ``tape_file`` for file copies;
  3. calculates CTA file location and inserts in into Chimera ``t_locationinfo`` table;
10. when Queue drops to 0, the processes shutdown and a single bootstrap query is run to
    updtate copy counts on all entries in ``tape`` table.
