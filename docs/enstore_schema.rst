Enstore Schema
==============

Here is Enstore DB schema that does not contain "unattached"
tables like ``media_capacity``:

.. image:: images/enstoredb.relationships.real.compact.png

There are two main tables - ``file`` and ``volume`` and a bunch of ancillary
tables of which only ``file_copies_map`` is important for Enstore -> CTA transition. This table maps `primary` file copy and secondary file copies. In reality
Enstore uses maximum one extra copy of a file. Not all files have extra copies.

Each file copy in Enstore is unuqiely identified by a BFID  -  bit file id,
which is string obtained by adding a three letter `brand` (which is the same for all files in a given Enstore instance), the Unix epoch, multiplied by 100000 and a counter which is reserved to resolve collisions. BFID is generated in the code base and is iserted into ``file`` table where it has unique contraint. If insert file, the counter is incremented and the record insertion is tried again. And so on until it succeeds.

.. code-block:: python

    bfid = "CDMS" + str(time.time()*100000)
