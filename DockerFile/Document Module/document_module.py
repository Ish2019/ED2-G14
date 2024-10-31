import spacy
import pdfplumber
from spacy.lang.en.stop_words import STOP_WORDS
from string import punctuation
from heapq import nlargest
import os
import io
from PIL import Image
import fitz
import pika
import bson
import json
import datetime
import random
import hashlib
import traceback
from copy import deepcopy

FilePath = os.path.dirname(__file__)

# Load spaCy model
try:
    nlp = spacy.load('en_core_web_sm')
except OSError:
    print("SpaCy model 'en_core_web_sm' not found. Please install it with `python -m spacy download en_core_web_sm`.")
    raise

def openFile(the_file):
    Meta_file = Meta(the_file)
    Text_Summerizer, Keyword = ConvertFile_txt(the_file)
    return Meta_file, Text_Summerizer, Keyword

def Meta(the_file):
    try:
        with pdfplumber.open(the_file) as pdf:
            with open(FilePath + "/" + 'Meta.txt', 'w') as f:
                print(pdf.metadata, file=f)
                print("Metadata:", pdf.metadata)  # Debugging output
                print(pdf.pages, file=f)
        with open(FilePath + "/" + 'Meta.txt', 'rb') as f:
            return f
    except pdfplumber.pdfminer.pdfdocument.PDFSyntaxError as e:
        print("Error in Meta extraction:", e)
        return f"Error: {e}"

def ConvertFile_txt(the_file):
    text_pdf = ''
    with pdfplumber.open(the_file) as pdf:
        for page in pdf.pages:
            text_pdf += page.extract_text()
    Summerizer_file = Text_Summerizer(text_pdf)
    Keyword_file = KeyWord(text_pdf)
    return Summerizer_file, Keyword_file

def Text_Summerizer(text_pdf):
    nlp = spacy.load('en_core_web_sm')
    doc = nlp(text_pdf)
    tokens = [token.text for token in doc]
    word_frequencies = {}
    for word in doc:
        if word.text.lower() not in list(STOP_WORDS):
            if word.text.lower() not in punctuation:
                if word.text not in word_frequencies.keys():
                    word_frequencies[word.text] = 1
                else:
                    word_frequencies[word.text] += 1

    # Set max_frequency to 1 if word_frequencies is empty
    max_frequency = max(word_frequencies.values(), default=1)
    
    for word in word_frequencies.keys():
        word_frequencies[word] = word_frequencies[word] / max_frequency
    sentence_tokens = [sent for sent in doc.sents]
    sentence_scores = {}
    for sent in sentence_tokens:
        for word in sent:
            if word.text.lower() in word_frequencies.keys():
                if sent not in sentence_scores.keys():
                    sentence_scores[sent] = word_frequencies[word.text.lower()]
                else:
                    sentence_scores[sent] += word_frequencies[word.text.lower()]
    select_length = max(1, int(len(sentence_tokens) * 0.3))  # Ensure at least one sentence is selected
    summary = nlargest(select_length, sentence_scores, key=sentence_scores.get)
    final_summary = [word.text for word in summary]
    summary = ''.join(final_summary)
    print("Summary:", summary)  # Debugging output
    with open(FilePath + "/" + "summary.txt", 'w') as f:
        print(summary, file=f)
    with open(FilePath + "/" + 'summary.txt', 'rb') as f:
        return f


def KeyWord(text_pdf):
    pos_tag = ['PROPN', 'ADJ', 'NOUN']
    doc = nlp(text_pdf.lower())
    result = []
    for token in doc:
        if (token.text in nlp.Defaults.stop_words or token.text in punctuation):
            continue
        if (token.pos_ in pos_tag):
            result.append(token.text)
    print("Keywords:", result)  # Debugging output
    with open(FilePath + "/" + "keywords_from_document.txt", 'w') as f:
        print(result, file=f)
    with open(FilePath + "/" + 'keywords_from_document.txt', 'rb') as f:
        return f

def IteratePDF(pdf_file_path):
    pdf_file = fitz.open(pdf_file_path)
    image_len = 0
    for page_index, page in enumerate(pdf_file):
        image_list = page.get_images()
        if image_list:
            print(f"[+] Found a total of {len(image_list)} images in page {page_index}")
            image_len += 1
        else:
            print("[!] No images found on page", page_index)

        for image_index, img in enumerate(page.get_images(), start=1):
            xref = img[0]
            base_image = pdf_file.extract_image(xref)
            image_bytes = base_image["image"]
            image_ext = base_image["ext"]
            image = Image.open(io.BytesIO(image_bytes))
            image_path = os.path.join(FilePath, 'images')
            os.makedirs(image_path, exist_ok=True)
            image.save(open(f"{image_path}/image{page_index+1}_{image_index}.{image_ext}", "wb"))
    return image_len

def remove_files():
    os.remove(FilePath + "/" + 'Meta.txt')
    os.remove(FilePath + "/" + 'summary.txt')
    os.remove(FilePath + "/" + 'keywords_from_document.txt')
    for file in os.listdir(FilePath + "/images"):
        os.remove(FilePath + "/images/" + file)

def publish_to_rabbitmq(routing_key, message):
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()
    status_message = message.copy()

    if 'PictureID' in status_message:
        status_message['status'] = 'Processed Successfully in Document Module'
        status_message['Message'] = 'Message has been Processed and sent to the Image Queue'
        del status_message['Payload']
    else:
        status_message['Status'] = 'Processed Successfully in Document Module'
        status_message['Message'] = 'Message has been Processed and sent to the Store Queue'
        del status_message['Payload'], status_message['Meta'], status_message['Summary'], status_message['Keywords']

    status_message = bson.dumps(status_message)
    message = bson.dumps(message)
    channel.basic_publish(exchange="Topic", routing_key=routing_key, body=message)
    channel.basic_publish(exchange="Topic", routing_key=".Status.", body=status_message)
    connection.close()

def consumer_connection(routing_key):
    connection_parameters = pika.ConnectionParameters('localhost')
    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()
    channel.basic_consume(queue=routing_key, auto_ack=True, on_message_callback=on_message_received)
    print('Preprocess Starting Consuming')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.close()
        connection.close()

def compute_unique_id(data_object):
    data_str = str(bson.dumps(data_object))
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    combined_data = data_str + current_time + str(random.random())
    unique_id = hashlib.sha256(combined_data.encode()).hexdigest()
    return unique_id

def on_message_received(ch, method, properties, body):
    try:
        body = bson.loads(body)
        with open(FilePath + "/" + body['FileName'], 'wb') as f:
            f.write(body['Payload'])

        Meta_file, Text_Summerizer, Keyword = openFile(FilePath + "/" + body['FileName'])
        Image_file = IteratePDF(FilePath + "/" + body['FileName'])

        if Image_file > 0:
            for file in os.listdir(FilePath + "/images"):
                _, ext = os.path.splitext(file)
                ext = ext.lstrip('.')
                with open(FilePath + "/images/" + file, 'rb') as f:
                    image_payload = f.read()
                    image = {
                        "ID": body['ID'],
                        "PictureID": body['DocumentId'],
                        "PictureType": ext,
                        "FileName": file,
                        "Payload": image_payload
                    }
                    image['PictureID'] = compute_unique_id(image)
                    publish_to_rabbitmq('.Image.', image)
        else:
            print('No images found in the document')

        with open('Meta.txt', 'rb') as f:
            body['Meta'] = f.read()
        with open('summary.txt', 'rb') as f:
            body['Summary'] = f.read()
        with open('keywords_from_document.txt', 'rb') as f:
            body['Keywords'] = f.read()
        
        publish_to_rabbitmq('.Store.', body)
        #remove_files()
        #os.remove(FilePath + "/" + body['FileName'])

    except Exception as e:
        error_message = str(e) or traceback.format_exc()
        print("Processing failed:", error_message)
        status_message = body.copy()
        del status_message['Payload']
        status_message['Status'] = 'Processing Failed'
        status_message['Message'] = error_message
        publish_to_rabbitmq(".Status.", bson.dumps(status_message))

if __name__ == "__main__":
    consumer_connection('Document')
