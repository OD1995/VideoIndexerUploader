import json
import logging
from MyFunctions import initial_function
import azure.functions as func
from urllib.parse import unquote


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
        logging.info("File too big, so splitting needed")
        queueMessage = f"{fileURL}__________{containerInput}__________ALL"
        logging.info(f"Message added to queue: {queueMessage}")
        msg.set(queueMessage)

    elif result == "retry":
        ## Get blob name
        fileName = unquote(fileURL.split("/")[-1])
        ## Get first bit before "_"
        prefix = fileName.split("_")[0]
        if "of" not in prefix:
            raise ValueError("We have an error from a non-split file, not sure what to do....")
        else:
            logging.info("Reading in URL failed, let's try to create it again")
            ## Get rid of the "XofY_" bit at the front of the file name
            fileURL2 = fileURL.replace(f"{prefix}_","")
            queueMessage = f"{fileURL2}__________{containerInput}__________{prefix}"
            logging.info(f"Message added to queue: {queueMessage}")
            msg.set(queueMessage)

    return func.HttpResponse("done")