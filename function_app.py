import azure.functions as func
import logging
from typing import List
import json
from datetime import datetime
import psycopg2
from psycopg2 import extras
import os
from psycopg2.pool import SimpleConnectionPool

app = func.FunctionApp()

# @app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="myeventhub",
#                                connection="twadeventhub_RootManageSharedAccessKey_EVENTHUB",
#                                cardinality = func.Cardinality.MANY) 
# def eventhub_trigger(azeventhub: func.EventHubEvent):
#     logging.info('Python EventHub trigger processed an event: ')

def get_db_connection():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        database=os.environ["PG_DB"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        port=os.environ.get("PG_PORT", 5432)
    )

pool = SimpleConnectionPool(
    1,
    2,   # max connections (IMPORTANT)
    host=os.environ["PG_HOST"],
    database=os.environ["PG_DB"],
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    port=os.environ.get("PG_PORT", 5432))


@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="rtu-msg-hub-pre",
                               connection="twadeventhub_RootManageSharedAccessKey_EVENTHUB",
                               cardinality = func.Cardinality.MANY) 
def eventhub_trigger(azeventhub: List[func.EventHubEvent]):
    logging.info(f'lenght {len(azeventhub)}')
    records = []
    for event in azeventhub:
        try:
            body = event.get_body().decode('utf-8')
            logging.info(f'Python EventHub trigger processed an event: {body}')
            data = json.loads(body)
            record = (
                        data.get("emf_device_id"),
                        data.get("emf_serial_number"),
                        data.get("rtu_device_id"),
                        data.get("rtu_serial_number"),
                        datetime.fromisoformat(data.get("timestamp")),
                        data.get("flow"),
                        data.get("totalizer_forward"),
                        data.get("totalizer_reverse"),
                        data.get("totalizer_net"),
                        data.get("alarm_code"),
                        data.get("communication_status"),
                        data.get("device_info")
                    )
            records.append(record)
        except Exception as e:
            logging.error(f'error while parsing message: {e}') 
            continue

        if not records:
            return

        try:
        # Open connection only when needed to stay under the 10-connection limit
            #conn = get_db_connection()
            conn = pool.getconn()
            with conn.cursor() as cursor:
                # Use execute_values for high-performance batching
                query = """INSERT INTO ga_integration.sa_meter_readings (
                    emf_device_id,
                    emf_serial_number,
                    rtu_device_id,
                    rtu_serial_number,
                    reading_timestamp,
                    flow,
                    totalizer_forward,
                    totalizer_reverse,
                    totalizer_net,
                    alarm_code,
                    communication_status,
                    device_info
                )
                VALUES %s
                """
                extras.execute_values(cursor, query, records)
                conn.commit()
                logging.info(f"Successfully inserted {len(records)} events.")

        except psycopg2.OperationalError as oe:
            # Handle connection limit/timeout issues
            logging.error(f"Postgres Connection Error (Check 10-conn limit): {oe}")
            raise # Rethrowing triggers the Azure Function retry policy
        except Exception as e:
            logging.error(f"Database insertion failed: {e}")
            if conn: conn.rollback()
            raise 
        finally:
            if conn:
                conn.close()   


# This example uses SDK types to directly access the underlying EventData object provided by the Event Hubs trigger.
# To use, uncomment the section below and add azurefunctions-extensions-bindings-eventhub to your requirements.txt file
# Ref: aka.ms/functions-sdk-eventhub-python
#
# import azurefunctions.extensions.bindings.eventhub as eh
# @app.event_hub_message_trigger(
#     arg_name="event", event_hub_name="myeventhub", connection="twadeventhub_RootManageSharedAccessKey_EVENTHUB"
# )
# def eventhub_trigger(event: eh.EventData):
#     logging.info(
#         "Python EventHub trigger processed an event %s",
#         event.body_as_str()
#     )
