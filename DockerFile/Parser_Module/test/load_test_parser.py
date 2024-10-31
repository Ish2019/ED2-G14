import socket
import bson
import hashlib
import datetime
import json
import random
import threading
import logging

logging.basicConfig(filename='sender_log.log', level=logging.INFO)


def compute_unique_id(data_object):
    # Convert the object to a string
    data_str = json.dumps(data_object, default=object)
    
    # Append the current date and time
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    
    combined_data = data_str + current_time + str(random.random())
    
    # Generate SHA-256 hash
    unique_id = hashlib.sha256(combined_data.encode()).hexdigest()
    
    return unique_id

def send_bson_obj(job):   
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 12345))
    s.sendall(bson.dumps(job))
    job_json = json.dumps(job)
    logging.info(f"Job ID: {job['ID']} sent at {datetime.datetime.now()}, job: {job_json}")
    s.close()

def id_generator(job):
    job['ID'] = compute_unique_id(job)  # Assigning unique ID as a string
    if 'NumberOfDocuments' in job and job['NumberOfDocuments'] > 0:
        for document in job['Documents']:
            document['ID'] = job['ID']
            document['DocumentId'] = compute_unique_id(document)
    if 'NumberOfImages' in job and job['NumberOfImages'] > 0:
        for image in job['Images']:
            image['ID'] = job['ID']
            image['PictureID'] = compute_unique_id(image)
    if 'NumberOfAudio' in job and job['NumberOfAudio'] > 0:
        for audio in job['Audio']:
            audio['ID'] = job['ID']
            audio['AudioID'] = compute_unique_id(audio)
    if 'NumberOfVideo' in job and job['NumberOfVideo'] > 0:
        for video in job['Video']:
            video['ID'] = job['ID']
            video['VideoID'] = compute_unique_id(video)
    return job

def send_jobs_concurrently(job, num_jobs, num_threads):
    def thread_function():
        for _ in range(num_jobs // num_threads):
            modified_job = id_generator(job.copy())
            send_bson_obj(modified_job)

    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=thread_function)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == '__main__':
    job = { 
    "ID": "ObjectID",  
    "NumberOfDocuments": 2,
    "NumberOfImages": 2,
    "NumberOfAudio": 2,
    "NumberOfVideo": 2,
    "Documents": [
        {
            "ID": "ObjectID",  
            "DocumentId": "ObjectID",
            "DocumentType": "String",
            "FileName": "String",
            "Payload": "Binary"
        },
        {
            "ID": "ObjectID",  
            "DocumentId": "ObjectID",
            "DocumentType": "String",
            "FileName": "String",
            "Payload": "Binary2"
        }
        
    ],
    "Images": [
        {
            "ID": "ObjectID", 
            "PictureID": "ObjectID",
            "PictureType": "String",
            "FileName": "String",
            "Payload": "Binary"
        }
       ,
        {
            "ID": "ObjectID", 
            "PictureID": "ObjectID",
            "PictureType": "String",
            "FileName": "String",
            "Payload": "Binary2"
        }
    ],
    "Audio": [
        {
            "ID": "ObjectID", 
            "AudioID": "ObjectID",
            "AudioType": "String",
            "FileName": "String",
            "Payload": "Binary"
        },
        {
            "ID": "ObjectID", 
            "AudioID": "ObjectID",
            "AudioType": "String",
            "FileName": "String",
            "Payload": "Binary2"
        }
    ],
    "Video": [
        {
            "ID": "ObjectID", 
            "VideoID": "ObjectID",
            "VideoType": "String",
            "FileName": "String",
            "Payload": "Binary5"
        },
        {
            "ID": "ObjectID", 
            "VideoID": "ObjectID",
            "VideoType": "String",
            "FileName": "String",
            "Payload": "Binary6"
        }
    ],
}
    NUM_JOBS = 5000  # Total number of jobs to send
    NUM_THREADS = 10  # Number of concurrent threads
    send_jobs_concurrently(job, NUM_JOBS, NUM_THREADS)
    




    
