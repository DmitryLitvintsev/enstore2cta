SFA files
=========

One of the issues that has been identified - CTA does not have
functinality corresponding to Enstore SFA (Small File Aggregation).
In the nutsheed SFA system is and extension of Enstore system that
managed intermediate disk storage in the side (intermediate between
dCache and Enstore). Depending on policies based on ``file_family``,
``storage_group``, ``library`` and file sizd Enstore directs files
to the intermediate storage for subsequent perioding packaging - tarring
the small files into large package files that then are written to
tape more efficiently.

The child/parent relation is captured in the same ``file`` table by
setting child's ``file.package_id`` to be equal of BFID of the package file.

To read SFA files in dCache/CTA setup this relation has to translate in
chimera.

There is a solution for it, used by similat to SFA, SAPPHIRE system by dCache.
We need to translate::

 child_pnfsid, package_pnfsid ->
    -> dcache://dcache/?store=vo&group=file_family&bfid={child_pnfsid}:package_pnfsid

I.e. the child/package relation exists as location in ``t_locationinfo`` Chimera
table.

This can be populated out of band.
