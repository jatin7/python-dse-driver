``dse.encoder`` - Encoders for non-prepared Statements
============================================================

.. module:: dse.encoder

.. autoclass:: Encoder ()

   .. autoattribute:: dse.encoder.Encoder.mapping

   .. automethod:: dse.encoder.Encoder.cql_encode_none ()

   .. automethod:: dse.encoder.Encoder.cql_encode_object ()

   .. automethod:: dse.encoder.Encoder.cql_encode_all_types ()

   .. automethod:: dse.encoder.Encoder.cql_encode_sequence ()

   .. automethod:: dse.encoder.Encoder.cql_encode_str ()

   .. automethod:: dse.encoder.Encoder.cql_encode_unicode ()

   .. automethod:: dse.encoder.Encoder.cql_encode_bytes ()

      Converts strings, buffers, and bytearrays into CQL blob literals.

   .. automethod:: dse.encoder.Encoder.cql_encode_datetime ()

   .. automethod:: dse.encoder.Encoder.cql_encode_date ()

   .. automethod:: dse.encoder.Encoder.cql_encode_map_collection ()

   .. automethod:: dse.encoder.Encoder.cql_encode_list_collection ()

   .. automethod:: dse.encoder.Encoder.cql_encode_set_collection ()

   .. automethod:: cql_encode_tuple ()
