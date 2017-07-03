``dse.graph`` - Graph Statements, Options, and Row Factories
============================================================

.. module:: dse.graph

.. autofunction:: single_object_row_factory

.. autofunction:: graph_result_row_factory

.. autofunction:: graph_object_row_factory

.. autoclass:: GraphOptions

   .. autoattribute:: graph_name

   .. autoattribute:: graph_source

   .. autoattribute:: graph_language

   .. autoattribute:: graph_read_consistency_level

   .. autoattribute:: graph_write_consistency_level

   .. autoattribute:: is_default_source

   .. autoattribute:: is_analytics_source

   .. autoattribute:: is_graph_source

   .. automethod:: set_source_default

   .. automethod:: set_source_analytics

   .. automethod:: set_source_graph


.. autoclass:: SimpleGraphStatement
   :members:

.. autoclass:: Result
   :members:

.. autoclass:: Vertex
   :members:

.. autoclass:: VertexProperty
   :members:

.. autoclass:: Edge
   :members:

.. autoclass:: Path
   :members:

.. autoclass:: GraphSON1TypeSerializer
   :members:

.. autoclass:: GraphSON1TypeDeserializer

   .. automethod:: deserialize_date

   .. automethod:: deserialize_timestamp

   .. automethod:: deserialize_time

   .. automethod:: deserialize_duration

   .. automethod:: deserialize_int

   .. automethod:: deserialize_bigint

   .. automethod:: deserialize_double

   .. automethod:: deserialize_float

   .. automethod:: deserialize_uuid

   .. automethod:: deserialize_blob

   .. automethod:: deserialize_decimal

   .. automethod:: deserialize_point

   .. automethod:: deserialize_linestring

   .. automethod:: deserialize_polygon
