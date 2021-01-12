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

    if result == "split":
        queueMessage = json.dumps({
            'fileURL' : fileURL,
            'containerInput' : containerInput
        })
        msg.set(queueMessage)
        return f"Message added to queue: {queueMessage}"
    else:
        return result