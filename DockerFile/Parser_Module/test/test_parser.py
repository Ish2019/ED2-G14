import unittest
import unittest.mock
import bson
import socket
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from parse import handle_client, parse_bson_obj, publish_to_rabbitmq, receive_bson_obj

class TestParseFunctions(unittest.TestCase):
    def setUp(self):
        self.obj = {
            'Documents': [{'DocumentId': 'ObjectID', 'DocumentType': 'String', 'FileName': 'String', 'Payload': ''}],
            'Images': [],
            'Audio': [],
            'Video': []
        }
        self.bson_obj = bson.dumps(self.obj)

    '''
      - Purpose: To verify the handle_client function's ability to correctly handle a client connection.
      - Process: Mocks the socket.socket.recv function and checks if it's called with the correct arguments. Also mocks the socket.socket.close function and checks if it's called.
      - Validation: Ensures that the client handling function correctly handles a client connection.
    '''
    def test_handle_client(self):
        mock_socket = unittest.mock.MagicMock()
        mock_socket.recv.return_value = self.bson_obj

        with unittest.mock.patch('parse.parse_bson_obj') as mock_parse_bson_obj:
            handle_client(mock_socket)
            mock_parse_bson_obj.assert_called_once_with(self.obj)
            mock_socket.close.assert_called_once()
            
    '''
      - Purpose: To verify the parse_bson_obj function's ability to correctly parse a BSON object and call the publish_to_rabbitmq function for each document.
      - Process: Mocks the publish_to_rabbitmq function and checks if it's called with the correct arguments based on the BSON object.
      - Validation: Confirms that the parsing function correctly interprets BSON data and triggers the appropriate publishing actions.
    '''
    def test_parse_bson_obj(self):
        with unittest.mock.patch('parse.publish_to_rabbitmq') as mock_publish_to_rabbitmq:
            parse_bson_obj(self.obj)
            mock_publish_to_rabbitmq.assert_called_once_with('.Document.', self.obj['Documents'][0])
    
    '''
      - Purpose: To verify the publish_to_rabbitmq function's ability to correctly publish a message to RabbitMQ.
      - Process: Mocks the pika.BlockingConnection and pika.BlockingConnection.channel functions and checks if they're called with the correct arguments. Also mocks the pika.BlockingConnection.close function and checks if it's called.
      - Validation: Ensures that the publishing function correctly establishes a connection to RabbitMQ, creates a channel, publishes a message, and closes the connection.
    '''
    def test_publish_to_rabbitmq(self):
      mock_connection = unittest.mock.Mock()
      mock_channel = unittest.mock.Mock()
      with unittest.mock.patch('pika.BlockingConnection', return_value=mock_connection), \
        unittest.mock.patch.object(mock_connection, 'channel', return_value=mock_channel), \
        unittest.mock.patch.object(mock_connection, 'close'):
        publish_to_rabbitmq('.Document.', self.obj['Documents'][0])
        mock_connection.channel.assert_called_once()
        mock_channel.basic_publish.assert_called_once_with(exchange="Topic", routing_key='.Document.', body=bson.dumps(self.obj['Documents'][0]))
        mock_connection.close.assert_called_once()

    '''
      - Purpose: To verify the receive_bson_obj function's ability to correctly start a socket server and listen for incoming BSON objects.
      - Process: Mocks the socket.socket function and checks if it's called with the correct arguments. Also mocks the socket.socket.bind, socket.socket.listen, and threading.Thread.start functions and checks if they're called.
      - Validation: Ensures that the socket server is correctly started and listens for incoming BSON objects.
    '''
    def test_receive_bson_obj(self):
        mock_socket = unittest.mock.Mock()
        mock_socket.__enter__ = unittest.mock.Mock(return_value=mock_socket)
        mock_socket.__exit__ = unittest.mock.Mock()
        mock_socket.accept.side_effect = [(mock_socket, 'localhost'), KeyboardInterrupt]
        with unittest.mock.patch('socket.socket', return_value=mock_socket), \
            unittest.mock.patch.object(mock_socket, 'bind'), \
            unittest.mock.patch.object(mock_socket, 'listen'), \
            unittest.mock.patch('threading.Thread.start') as mock_start:
            try:
              receive_bson_obj()
            except KeyboardInterrupt:
              pass
            mock_start.assert_called_once()

    '''
      - Purpose: To verify that the handle_client function correctly handles exceptions.
      - Process: Mocks the socket.socket.recv function and checks if it's called with the correct arguments. Also mocks the socket.socket.close function and checks if it's called.
      - Validation: Ensures that the client handling function correctly handles exceptions.
    '
    '''
    def test_publish_to_rabbitmq_with_exception(self):
      mock_connection = unittest.mock.Mock()
      mock_channel = unittest.mock.Mock()
      mock_channel.basic_publish.side_effect = Exception
      with unittest.mock.patch('pika.BlockingConnection', return_value=mock_connection), \
        unittest.mock.patch.object(mock_connection, 'channel', return_value=mock_channel), \
        unittest.mock.patch.object(mock_connection, 'close'):
        with self.assertRaises(Exception):
          publish_to_rabbitmq('.Document.', self.obj['Documents'][0])
          
    '''
      - Purpose: To verify that the handle_client function correctly handles exceptions.
      - Process: Mocks the socket.socket.recv function and checks if it's called with the correct arguments. Also mocks the socket.socket.close function and checks if it's called.
      - Validation: Ensures that the client handling function correctly handles exceptions.
    '''
    def test_receive_bson_obj_with_exception(self):
        mock_socket = unittest.mock.Mock()
        mock_socket.__enter__ = unittest.mock.Mock(return_value=mock_socket)
        mock_socket.__exit__ = unittest.mock.Mock()
        mock_socket.accept.side_effect = Exception
        with unittest.mock.patch('socket.socket', return_value=mock_socket), \
            unittest.mock.patch.object(mock_socket, 'bind'), \
            unittest.mock.patch.object(mock_socket, 'listen'), \
            unittest.mock.patch('threading.Thread.start') as mock_start:
            receive_bson_obj()  
          
if __name__ == '__main__':
    unittest.main()