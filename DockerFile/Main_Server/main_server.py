import socket
import bson
import hashlib
import datetime
import json
import random

def compute_unique_id(data_object):
    # Convert the object to a string
    data_str = str(bson.dumps(data_object))
    
    # Append the current date and time
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    
    combined_data = data_str + current_time + str(random.random())
    
    # Generate SHA-256 hash
    unique_id = hashlib.sha256(combined_data.encode()).hexdigest()
    
    return unique_id

def send_bson_obj(job):   
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 12345))  # Ensure this line is present and correctly written
    s.sendall(bson.dumps(job))
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


if __name__ == '__main__':
    job = { 
    "ID": "ObjectID",  
    "NumberOfDocuments": 1,
    "NumberOfImages": 1,
    "NumberOfAudio": 2,
    "NumberOfVideo": 1,
    "Documents": [
        {
            "ID": "ObjectID",  
            "DocumentId": "ObjectID",
            "DocumentType": "String",
            "FileName": "String",
            "Payload": "Binary"
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
    ],
}
    # take binary data from file
    with open('Project_4.pdf', 'rb') as f:
        job['Documents'][0]['Payload'] = f.read()
        job['Documents'][0]["DocumentType"] = "pdf"
        job['Documents'][0]["FileName"] = f.name
        f.close()
    with open('my_video.mp4', 'rb') as f:
        job['Video'][0]['Payload'] = f.read()
        job['Video'][0]["VideoType"] = "mp4"
        job['Video'][0]["FileName"] = f.name
        f.close()
    with open('x.png', 'rb') as f:
        job['Images'][0]['Payload'] = f.read()
        job['Images'][0]["PictureType"] = "png"
        job['Images'][0]["FileName"] = f.name
        f.close()
    with open('audio.mp3', 'rb') as f:
        job['Audio'][0]['Payload'] = f.read()
        job['Audio'][0]["AudioType"] = "mp3"
        job['Audio'][0]["FileName"] = f.name
        f.close()
    with open('audio2.mp3', 'rb') as f:
        job['Audio'][1]['Payload'] = f.read()
        job['Audio'][1]["AudioType"] = "mp3"
        job['Audio'][1]["FileName"] = f.name
        f.close()
    id_generator(job)
    send_bson_obj(job)
    




    
