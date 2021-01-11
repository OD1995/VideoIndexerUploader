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
    containerInput = options['container']

    result = initial_function(
        fileURL=fileURL,
        containerInput=containerInput
    )

    if result == "split":
        queueMessage = json.dumps({
            'fileURL' : fileURL,
            'containerInput' : containerInput
        })
        msg.set(queueMessage)
        return f"Message added to queue: {queueMessage}"
    else:
        return result