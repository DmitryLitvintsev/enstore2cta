dCache fie location
===================

dCache needs to know location of files in CTA. It is done by utilizing URI location style and looks like::

 cta://cta/<pnfsid>?archive_id=<archive_file_id>

When running migration, for each file in `archive_file` these locations are
back-filled into existing chimera database.
