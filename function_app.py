import logging
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="eventhub_batch_processor")
@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="rtu-msg-hub-pre",
                               connection="twadeventhub_RootManageSharedAccessKey_EVENTHUB",
                               cardinality = func.Cardinality.MANY) 
def eventhub_batch_processor(events: list[func.EventHubEvent]):

    logging.info(f"Received batch size: {len(events)}")

    for event in events:
        body = event.get_body().decode("utf-8")
        logging.info(f"Processing message: {body}")

    logging.info("Batch processing completed.")

