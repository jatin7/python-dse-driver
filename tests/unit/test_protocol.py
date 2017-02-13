try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa


from mock import Mock

from dse import ProtocolVersion
from dse.protocol import (PrepareMessage, QueryMessage, ExecuteMessage, UnsupportedOperation,
    _PAGING_OPTIONS_FLAG, _WITH_SERIAL_CONSISTENCY_FLAG, _PAGE_SIZE_FLAG, _WITH_PAGING_STATE_FLAG)
from dse.marshal import uint32_unpack
from dse.cluster import Cluster, ContinuousPagingOptions, DriverException

class MessageTest(unittest.TestCase):

    def test_prepare_message(self):
        messaage = PrepareMessage("a")
        io = Mock()
        #body = io.BytesIO()
        messaage.send_body(io,4)
        self.assertEqual(len(io.write.mock_calls),2)
        # This should fetch the args passed into the write
        self.assertEqual(io.write.mock_calls[0][1],(b'\x00\x00\x00\x01',))
        self.assertEqual(io.write.mock_calls[1][1],(b'a',))
        io = Mock()
        messaage.send_body(io,5)
        self.assertEqual(len(io.write.mock_calls),3)
        # This should fetch the args passed into the write
        self.assertEqual(io.write.mock_calls[0][1],(b'\x00\x00\x00\x01',))
        self.assertEqual(io.write.mock_calls[1][1],(b'a',))
        self.assertEqual(io.write.mock_calls[2][1],(b'\x00\x00\x00\x00',))

    def test_execute_message(self):
        messaage = ExecuteMessage('1',[],4)
        io = Mock()
        #body = io.BytesIO()
        messaage.send_body(io,4)
        self.assertEqual(len(io.write.mock_calls),5)
        # This should fetch the args passed into the write
        self.assertEqual(io.write.mock_calls[0][1],(b'\x00\x01',))
        self.assertEqual(io.write.mock_calls[1][1],(b'1',))
        self.assertEqual(io.write.mock_calls[2][1],(b'\x00\x04',))
        #must be byte
        self.assertEqual(io.write.mock_calls[3][1],(b'\x01',))
        self.assertEqual(io.write.mock_calls[4][1],(b'\x00\x00',))
        io = Mock()
        messaage.send_body(io, 5)
        self.assertEqual(len(io.write.mock_calls),5)
        # This should fetch the args passed into the write
        self.assertEqual(io.write.mock_calls[0][1],(b'\x00\x01',))
        self.assertEqual(io.write.mock_calls[1][1],(b'1',))
        self.assertEqual(io.write.mock_calls[2][1],(b'\x00\x04',))
        # must be unsigned int
        self.assertEqual(io.write.mock_calls[3][1],(b'\x00\x00\x00\x01',))
        self.assertEqual(io.write.mock_calls[4][1],(b'\x00\x00',))

    def test_query_message(self):
        message = QueryMessage("a",3)
        io = Mock()
        message.send_body(io,4)
        self.assertEqual(len(io.write.mock_calls),4)
        # This should fetch the args passed into the write
        self.assertEqual(io.write.mock_calls[0][1],(b'\x00\x00\x00\x01',))
        self.assertEqual(io.write.mock_calls[1][1],(b'a',))
        self.assertEqual(io.write.mock_calls[2][1],(b'\x00\x03',))
        self.assertEqual(io.write.mock_calls[3][1],(b'\x00',))
        io = Mock()
        message.send_body(io,5)
        self.assertEqual(len(io.write.mock_calls),4)
        # This should fetch the args passed into the write
        self.assertEqual(io.write.mock_calls[0][1],(b'\x00\x00\x00\x01',))
        self.assertEqual(io.write.mock_calls[1][1],(b'a',))
        self.assertEqual(io.write.mock_calls[2][1],(b'\x00\x03',))
        # must be unsigned int
        self.assertEqual(io.write.mock_calls[3][1],(b'\x00\x00\x00\x00',))

    def test_continuous_paging(self):
        """
        Test to check continuous paging throws an Exception if it's not supported and the correct valuesa
        are written to the buffer if the option is enabled.

        @since DSE 2.0b3 GRAPH 1.0b1
        @jira_ticket PYTHON-694
        @expected_result the values are correctly written

        @test_category connection
        """
        max_pages = 4
        max_pages_per_second = 3
        continuous_paging_options = ContinuousPagingOptions(max_pages=max_pages,
                                                            max_pages_per_second=max_pages_per_second)
        message = QueryMessage("a", 3, continuous_paging_options=continuous_paging_options)
        io = Mock()
        for version in [version for version in ProtocolVersion.SUPPORTED_VERSIONS
                        if version != ProtocolVersion.DSE_V1]:
            self.assertRaises(UnsupportedOperation, message.send_body, io, version)

        io.reset_mock()
        message.send_body(io, ProtocolVersion.DSE_V1)

        #continuous paging adds two write calls to the buffer
        self.assertEqual(len(io.write.mock_calls), 6)
        # Check that the appropriate flag is set to True
        self.assertEqual(uint32_unpack(io.write.mock_calls[3][1][0]) & _WITH_SERIAL_CONSISTENCY_FLAG, 0)
        self.assertEqual(uint32_unpack(io.write.mock_calls[3][1][0]) & _PAGE_SIZE_FLAG, 0)
        self.assertEqual(uint32_unpack(io.write.mock_calls[3][1][0]) & _WITH_PAGING_STATE_FLAG, 0)
        self.assertEqual(uint32_unpack(io.write.mock_calls[3][1][0]) & _PAGING_OPTIONS_FLAG, _PAGING_OPTIONS_FLAG)

        #Test max_pages and max_pages_per_second are correctly written
        self.assertEqual(uint32_unpack(io.write.mock_calls[4][1][0]), max_pages)
        self.assertEqual(uint32_unpack(io.write.mock_calls[5][1][0]), max_pages_per_second)

    def test_prepare_flag(self):
        """
        Test to check the prepare flag is properly set, This should only happen for V5 at the moment.

        @since DSE 2.0b3 GRAPH 1.0b1
        @jira_ticket PYTHON-694
        @expected_result the values are correctly written

        @test_category connection
        """
        message = PrepareMessage("a")
        io = Mock()
        for version in ProtocolVersion.SUPPORTED_VERSIONS:
            message.send_body(io, version)
            if ProtocolVersion.uses_prepare_flags(version):
                #This should pass after PYTHON-696
                self.assertEqual(len(io.write.mock_calls), 3)
                #self.assertEqual(uint32_unpack(io.write.mock_calls[2][1][0]) & _WITH_SERIAL_CONSISTENCY_FLAG, 1)
            else:
                self.assertEqual(len(io.write.mock_calls), 2)
            io.reset_mock()
