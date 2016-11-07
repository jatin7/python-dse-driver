# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from cassandra.metadata import RegisteredTableExtension, protect_name


class RLACTableExtension(RegisteredTableExtension):
    name = "DSE_RLACA"

    @classmethod
    def after_table_cql(cls, table_meta, ext_key, ext_blob):
        return "RESTRICT ROWS ON %s.%s USING %s;" % (protect_name(table_meta.keyspace_name),
                                                    protect_name(table_meta.name),
                                                    protect_name(ext_blob.decode('utf-8')))
