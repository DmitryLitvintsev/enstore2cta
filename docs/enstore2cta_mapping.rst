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
       |  ``volume.file_family``
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
     -
