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
setting child's ``file.package_id`` to be equal of BFID of the package file.

To read SFA files in dCache/CTA setup this relation has to translate in
chimera.

There is a solution for it, used by similar to SFA, SAPPHIRE system by dCache.
We need to translate::

 child_pnfsid, package_pnfsid ->
    -> dcache://dcache/?store=vo&group=file_family&bfid=child_pnfsid:package_pnfsid

I.e. the child/package relation exists as location in ``t_locationinfo`` Chimera
table. As long as these locations exist dCache can read these files from CTA using an hsm script. T.e. SAPPHIRE system is not need for reading of SFA files.

This can be populated out of band.
