import unittest
import unittest.mock
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from main_server import compute_unique_id, send_bson_obj, id_generator

class TestMainServerFunctions(unittest.TestCase):
    # This test verifies that the send_bson_obj function correctly handles an empty payload.
    '''
        purpose: To verify that the send_bson_obj function correctly handles an empty payload.
        process: Mocks the socket.socket function and checks if it's called with the correct arguments. Also mocks the socket.socket.connect and socket.socket.sendall functions and checks if they're called.
        validation: Ensures that the send_bson_obj function correctly handles an empty payload.
    '''
    def test_send_bson_obj_empty_payload(self):
        job_empty_payload = {"ID": "ObjectID", "NumberOfDocuments": 1, "Documents": [{"ID": "ObjectID", "DocumentId": "ObjectID", "DocumentType": "String", "FileName": "String", "Payload": ""}]}
        with unittest.mock.patch('socket.socket') as mock_socket_empty_payload:
            instance_empty_payload = mock_socket_empty_payload.return_value
            send_bson_obj(job_empty_payload)
            instance_empty_payload.connect.assert_called_once_with(('localhost', 12345))
            instance_empty_payload.sendall.assert_called_once()
    
    # This test verifies that the send_bson_obj function correctly handles a large payload.
    '''
        purpose: To verify that the send_bson_obj function correctly handles a large payload.
        process: Mocks the socket.socket function and checks if it's called with the correct arguments. Also mocks the socket.socket.connect and socket.socket.sendall functions and checks if they're called.
        validation: Ensures that the send_bson_obj function correctly handles a large payload.
    '''
    def test_send_bson_obj_large_payload(self):
        large_payload = "X" * (1024 * 1024)  # 1 MB payload
        job_large_payload = {"ID": "ObjectID", "NumberOfDocuments": 1, "Documents": [{"ID": "ObjectID", "DocumentId": "ObjectID", "DocumentType": "String", "FileName": "String", "Payload": large_payload}]}
        with unittest.mock.patch('socket.socket') as mock_socket_large_payload:
            instance_large_payload = mock_socket_large_payload.return_value
            send_bson_obj(job_large_payload)
            instance_large_payload.connect.assert_called_once_with(('localhost', 12345))
            instance_large_payload.sendall.assert_called_once()
 
    # This test verifies that the compute_unique_id function correctly computes a unique ID for a given data object.
    '''
        purpose: To verify that the compute_unique_id function correctly computes a unique ID for a given data object.
        process: Creates two data objects and checks if the compute_unique_id function returns different IDs for them.
        validation: Ensures that the compute_unique_id function correctly computes a unique ID for a given data object.
    '''
    def test_compute_unique_id(self):
        data_object = {"key": "value"}
        id1 = compute_unique_id(data_object)
        id2 = compute_unique_id(data_object)
        self.assertNotEqual(id1, id2)

    # This test verifies that the id_generator function correctly generates an ID for a given job.
    '''
        purpose: To verify that the id_generator function correctly generates an ID for a given job.
        process: Creates a job and checks if the id_generator function returns an ID for it.
        validation: Ensures that the id_generator function correctly generates an ID for a given job. 
    '''
    def test_id_generator(self):
        # Test the id_generator function
        job = {"ID": "ObjectID", "NumberOfDocuments": 1, "Documents": [{"ID": "ObjectID", "DocumentId": "ObjectID", "DocumentType": "String", "FileName": "String", "Payload": ""}]}
        result = id_generator(job)
        self.assertIsNotNone(result)
    
    # This test verifies that the send_bson_obj function correctly handles different payloads.
    '''
        purpose: To verify that the send_bson_obj function correctly handles different payloads.
        process: Mocks the socket.socket function and checks if it's called with the correct arguments. Also mocks the socket.socket.connect and socket.socket.sendall functions and checks if they're called.
        validation: Ensures that the send_bson_obj function correctly handles different payloads.
    '''
    def test_send_bson_obj_with_different_payloads(self):
        payloads = [os.urandom(100), os.urandom(200), os.urandom(300)]
        for payload in payloads:
            with self.subTest(payload=payload):
                job = {"ID": "ObjectID", "NumberOfDocuments": 1, "Documents": [{"ID": "ObjectID", "DocumentId": "ObjectID", "DocumentType": "String", "FileName": "String", "Payload": payload}]}
                with unittest.mock.patch('socket.socket') as mock_socket:
                    instance = mock_socket.return_value
                    send_bson_obj(job)
                    instance.connect.assert_called_once_with(('localhost', 12345))
                    instance.sendall.assert_called_once()
                    
    # This test verifies that the id_generator function correctly generates IDs for documents in a given job.
    '''
        purpose: To verify that the id_generator function correctly generates IDs for documents in a given job.
        process: Creates a job and checks if the id_generator function returns an ID for it.
        validation: Ensures that the id_generator function correctly generates IDs for documents in a given job.
    '''
    def test_id_generator_documents(self):
        job = {"NumberOfDocuments": 2, "Documents": [{"DocumentId": "ObjectID", "DocumentType": "String", "FileName": "String", "Payload": ""}, {"DocumentId": "ObjectID", "DocumentType": "String", "FileName": "String", "Payload": ""}]}
        result = id_generator(job)
        self.assertIsNotNone(result['ID'])
        for document in result['Documents']:
            self.assertEqual(document['ID'], result['ID'])
            self.assertIsNotNone(document['DocumentId'])
            
    # This test verifies that the id_generator function correctly generates IDs for images in a given job.
    '''
        purpose: To verify that the id_generator function correctly generates IDs for images in a given job.
        process: Creates a job and checks if the id_generator function returns an ID for it.
        validation: Ensures that the id_generator function correctly generates IDs for images in a given job.
    '''
    def test_id_generator_images(self):
        job = {"NumberOfImages": 2, "Images": [{"PictureID": "ObjectID", "ImageType": "String", "FileName": "String", "Payload": ""}, {"PictureID": "ObjectID", "ImageType": "String", "FileName": "String", "Payload": ""}]}
        result = id_generator(job)
        self.assertIsNotNone(result['ID'])
        for image in result['Images']:
            self.assertEqual(image['ID'], result['ID'])
            self.assertIsNotNone(image['PictureID'])

    # This test verifies that the id_generator function correctly generates IDs for audio in a given job.
    '''
        purpose: To verify that the id_generator function correctly generates IDs for audio in a given job.
        process: Creates a job and checks if the id_generator function returns an ID for it.
        validation: Ensures that the id_generator function correctly generates IDs for audio in a given job.
    '''
    def test_id_generator_audio(self):
        job = {"NumberOfAudio": 2, "Audio": [{"AudioID": "ObjectID", "AudioType": "String", "FileName": "String", "Payload": ""}, {"AudioID": "ObjectID", "AudioType": "String", "FileName": "String", "Payload": ""}]}
        result = id_generator(job)
        self.assertIsNotNone(result['ID'])
        for audio in result['Audio']:
            self.assertEqual(audio['ID'], result['ID'])
            self.assertIsNotNone(audio['AudioID'])

    # This test verifies that the id_generator function correctly generates IDs for video in a given job.
    '''
        purpose: To verify that the id_generator function correctly generates IDs for video in a given job.
        process: Creates a job and checks if the id_generator function returns an ID for it.
        validation: Ensures that the id_generator function correctly generates IDs for video in a given job.
    '''
    def test_id_generator_video(self):
        job = {"NumberOfVideo": 2, "Video": [{"VideoID": "ObjectID", "VideoType": "String", "FileName": "String", "Payload": ""}, {"VideoID": "ObjectID", "VideoType": "String", "FileName": "String", "Payload": ""}]}
        result = id_generator(job)
        self.assertIsNotNone(result['ID'])
        for video in result['Video']:
            self.assertEqual(video['ID'], result['ID'])
            self.assertIsNotNone(video['VideoID'])
    
    # This test verifies that the id_generator function correctly raises a TypeError when given invalid data.
    '''
        purpose: To verify that the id_generator function correctly raises a TypeError when given invalid data.
        process: Creates a job and checks if the id_generator function raises a TypeError when given invalid data.
        validation: Ensures that the id_generator function correctly raises a TypeError when given invalid data.
    '''
    def test_id_generator_invalid_data(self):
        job = {"NumberOfDocuments": "two", "Documents": [{"DocumentId": 123, "DocumentType": "String", "FileName": "String", "Payload": ""}, {"DocumentId": "ObjectID", "DocumentType": "String", "FileName": "String", "Payload": ""}]}
        with self.assertRaises(TypeError):
            id_generator(job)
            
    # This test verifies that the id_generator function correctly generates unique IDs for all types of files in a given job.
    '''
        purpose: To verify that the id_generator function correctly generates unique IDs for all types of files in a given job.
        process: Creates a job and checks if the id_generator function returns an ID for it.
        validation: Ensures that the id_generator function correctly generates unique IDs for all types of files in a given job.
    '''
    def test_id_generator_unique_ids(self):
        job = {
            "NumberOfDocuments": 2,
            "NumberOfImages": 2,
            "NumberOfAudio": 2,
            "NumberOfVideo": 2,
            "Documents": [
                {"DocumentId": "ObjectID", "DocumentType": "String", "FileName": "String", "Payload": "Binary"},
                {"DocumentId": "ObjectID", "DocumentType": "String", "FileName": "String", "Payload": "Binary2"}
            ],
            "Images": [
                {"PictureID": "ObjectID", "PictureType": "String", "FileName": "String", "Payload": "Binary"},
                {"PictureID": "ObjectID", "PictureType": "String", "FileName": "String", "Payload": "Binary2"}
            ],
            "Audio": [
                {"AudioID": "ObjectID", "AudioType": "String", "FileName": "String", "Payload": "Binary"},
                {"AudioID": "ObjectID", "AudioType": "String", "FileName": "String", "Payload": "Binary2"}
            ],
            "Video": [
                {"VideoID": "ObjectID", "VideoType": "String", "FileName": "String", "Payload": "Binary5"},
                {"VideoID": "ObjectID", "VideoType": "String", "FileName": "String", "Payload": "Binary6"}
            ],
        }
        result = id_generator(job)

        # Check that all DocumentIds are unique
        document_ids = [doc['DocumentId'] for doc in result['Documents']]
        self.assertEqual(len(document_ids), len(set(document_ids)))

        # Check that all PictureIDs are unique
        picture_ids = [img['PictureID'] for img in result['Images']]
        self.assertEqual(len(picture_ids), len(set(picture_ids)))

        # Check that all AudioIDs are unique
        audio_ids = [aud['AudioID'] for aud in result['Audio']]
        self.assertEqual(len(audio_ids), len(set(audio_ids)))

        # Check that all VideoIDs are unique
        video_ids = [vid['VideoID'] for vid in result['Video']]
        self.assertEqual(len(video_ids), len(set(video_ids)))

    
if __name__ == '__main__':
    unittest.main()