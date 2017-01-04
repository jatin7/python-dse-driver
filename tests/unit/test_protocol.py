try:
    import unittest2 as unittest
except ImportError:
    import unittest # noqa


from mock import Mock

from dse.protocol import PrepareMessage, QueryMessage, ExecuteMessage

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
        messaage = QueryMessage("a",3)
        io = Mock()
        messaage.send_body(io,4)
        self.assertEqual(len(io.write.mock_calls),4)
        # This should fetch the args passed into the write
        self.assertEqual(io.write.mock_calls[0][1],(b'\x00\x00\x00\x01',))
        self.assertEqual(io.write.mock_calls[1][1],(b'a',))
        self.assertEqual(io.write.mock_calls[2][1],(b'\x00\x03',))
        self.assertEqual(io.write.mock_calls[3][1],(b'\x00',))
        io = Mock()
        messaage.send_body(io,5)
        self.assertEqual(len(io.write.mock_calls),4)
        # This should fetch the args passed into the write
        self.assertEqual(io.write.mock_calls[0][1],(b'\x00\x00\x00\x01',))
        self.assertEqual(io.write.mock_calls[1][1],(b'a',))
        self.assertEqual(io.write.mock_calls[2][1],(b'\x00\x03',))
        #m ust be unsigned int
        self.assertEqual(io.write.mock_calls[3][1],(b'\x00\x00\x00\x00',))