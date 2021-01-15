import json
import logging
from MyFunctions import initial_function
import azure.functions as func


def main(event: func.EventGridEvent, msg: func.Out[str]):
    
    subject = (event.subject).split('/')
    options = {
        "fileUrl": event.get_json()['url'],
        "container": subject[4],
        "blob": subject[6]
                }

    logging.info('Python EventGrid trigger processed an event: %s', options)

    fileURL = options['fileUrl']
    logging.info(f"fileURL: {fileURL}")
    containerInput = options['container']
    logging.info(f"containerInput: {containerInput}")

    result = initial_function(
        fileURL=fileURL,
        containerInput=containerInput
    )
    logging.info(f"result: {result}")

    if result == "split":
        logging.info("File too big, so splitting needed abc")
        queueMessage = f"{fileURL}__________{containerInput}"
        logging.info(f"Message added to queue: {queueMessage}")
        msg.set(queueMessage)