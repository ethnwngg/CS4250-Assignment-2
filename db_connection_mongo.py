#-------------------------------------------------------------------------
# AUTHOR: Ethan Wong
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Python program using PyMongo
# FOR: CS 4250- Assignment #2
# TIME SPENT: 4 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import datetime
import string

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Invalid")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    terms = docText.lower().split(" ")

    term_count = {}
    for term in terms:
        if term in term_count:
            term_count[term] += 1
        else:
            term_count[term] = 1

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    # --> add your Python code here
    term_list = [{"term": term, 
                  "count":count, 
                  "num_chars": len(term)} for term, count in term_count.items()]

    #Producing a final document as a dictionary including all the required fields
    # --> add your Python code here
    docDate = datetime.datetime.strptime(docDate,"%Y-%m-%d")

    document = {"_id": docId,
                "text": docText,
                "title": docTitle,
                "date": docDate,
                "category": docCat,
                "terms": term_list,
                "num_chars": len(docText),
                }
    
    # Insert the document
    # --> add your Python code here
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    col.delete_one({"_id": docId})

    # Create the document with the same id
    # --> add your Python code here
    terms = docText.lower().split(" ")

    term_count = {}
    for term in terms:
        if term in term_count:
            term_count[term] += 1
        else:
            term_count[term] = 1

    term_list = [{"term": term, 
                  "count":count, 
                  "num_chars": len(term)} for term, count in term_count.items()]
    
    docDate = datetime.datetime.strptime(docDate,"%Y-%m-%d")

    col.insert_one({"_id": docId,
                "text": docText,
                "title": docTitle,
                "date": docDate,
                "category": docCat,
                "terms": term_list,
                "num_chars": len(docText),
                })


def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    # --> add your Python code here
    documents = col.find()
    inverted_index = {}

    for doc in documents:
        title = doc["title"]
        terms = doc["terms"]

        for term_data in terms:
            term = term_data["term"]
            count = term_data["count"]

            final_term = term.translate(str.maketrans('', '', string.punctuation)).lower()

            if final_term not in inverted_index:
                inverted_index[final_term] = {}

            inverted_index[final_term][title] = count

    final_index = {}
    for term, occurences in inverted_index.items():
        doc_counts = [f"{title}:{count}" for title, count in occurences.items()]
        final_index[term] = ",".join(doc_counts)
        
    return final_index
