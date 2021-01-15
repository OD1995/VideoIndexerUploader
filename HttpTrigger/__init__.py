import json
import logging
from MyFunctions import initial_function
import azure.functions as func


def main(req: func.HttpRequest, msg: func.Out[str]):

    options = {}
    for f in [  
                'fileUrl',
                'container',
                'blob'
                ]:
        options[f] = req.params.get(f)

    logging.info('Python HTTP trigger processed an event: %s', options)

    fileURL = options['fileUrl']
    containerInput = options['container']

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