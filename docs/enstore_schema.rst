Enstore Schema
==============

Here is Enstore DB schema that does not contain "unattached"
tables like ``media_capacity``:

.. image:: images/enstoredb.relationships.real.compact.png

There are two main tables - ``file`` and ``volume`` and a bunch of ancillary
tables of which only ``file_copies_map`` is important for Enstore -> CTA transition. This table maps `primary` file copy and secondary file copies. In reality
Enstore uses maximum one extra copy of a file. Not all files have extra copies.

File table
----------

Each file copy in Enstore is uniquely identified by a BFID  -  bit file id,
which is a string obtained by adding a three letter `brand` (which is the same for all files in a given Enstore instance), the Unix epoch, multiplied by 100000 and a counter which is reserved to resolve collisions. BFID is generated in the code base and is iserted into ``file`` table where it has unique contraint. If insert fails, the counter is incremented and the record insertion is tried again. And so on until it succeeds.

.. code-block:: python

    bfid = "CDMS" + str(time.time()*100000)

Each file record contains PNFSID (dCache inode identifier) that ties it back to
the front end storage system; adler32 checksum; a reference to the file package for small files in SFA (Small File Aggregation) equal BFID of the package or ``null`` for `direct` files; file size; original file name;  UID/GID of user who creted the file; tape location and a ``deleted`` flag that indicates whether or not the file
has been removed from namespace.

Volume table
------------

Every tape in Enstore is stored in the ``volume`` table.
The many to one ``file`` to ``volume`` relation is done on integer ``volume.id`` primary key via ``file.volume`` foreign key.

Each volume record tracks how many active/deleted/total files and bytes exist
on the volume (via DB trigger on insert/update/delete). It has a volume label; total/remaining bytes; number of mounts; number of read and write accesses; severla status fields that allow to classify tapes (e.g. ``full``, ``NOACCESS``, ``NOTALLOWED``, ``migrated``, ``migrating``). The values of status fields are arbitrary strings.

The Enstore system  has a concept of virtual library, so called libary manager (LM). The LM
manages a set of movers that have SCSI tape drives attached. LM (and movers) are Enstore servers and are configured based in Enstore instance configuration and are not captured in database schema. Each LM has a unique name and draws specific tapes allocated for it. This relation is captured in ``volume.library`` field.

Since Enstroe LMs map  to actual  physical tape libraries, the volumes have to be pre-allocated to specific LMs.

Accounting and data steering aspects of Enstore operations use ``volume.storage_group`` field (usually corresponding to a VO name); ``volume.file_family`` a string field that tells enstore to use the same set of tapes to write data having this attribute. ``volume.file_family_width`` an integer taht specified how many tape deives can be used simultaneously to write data with speciric ``file_family``.

Enstore does not have pre-defined ``library``, ``storage_group`` and ``file_family`` concepts. When files are written to  Enstore it receives the instruction fo what (``library``, ``file_family``, ``file_family_width``) to use from Enstore command line client ``encp``. When invoked, the ``encp`` client takes these parameters from directory tags of the destination directory or they can be passed as options to encp. File family value can be completelty arbitrary and user defined. Specifying random ``library`` string results in failure to write if Enstore does not actually have a running LM with matching name.

File copies
-----------

If `encp` is passed comma separated list of libraries via directory tag or command line option Enstore will make as many copies of the file on volumes belonging to these libraries. In practice Fermilab Enstore system uses maximum 2 file copies for a subset of data. The relation between `primary` and `secondary` is captured in ``file_copies_map`` table having ``bfid`` and ``alt_bfid`` to express the relation.
