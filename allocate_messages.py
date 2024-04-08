from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_email import EmailServerCredentials, email_send_message

# Load blocks
db_conn = SqlAlchemyConnector.load("sqlalchemy")
email_server_credentials = EmailServerCredentials.load("websmtp")


@task
def fetch_data(db_conn):
    results = db_conn.fetch_many(
        "SELECT * FROM success_messages WHERE msg_id = :id;", parameters={"id": "1"}
    )
    return results


@task
def insert_message(db_conn, table, message):
    db_conn.execute(
        f"INSERT INTO {table} (msg_id, msg_date, msg_time, message) VALUES( :id, :msg_date, :msg_time, :message ) ON CONFLICT DO NOTHING",
        parameters={
            "id": f"{message[0]}",
            "msg_date": f"{message[1]}",
            "msg_time": f"{message[2]}",
            "message": f"{message[3]}",
        },
    )


@task
def insert_message_with_category(db_conn, table, message, category):
    db_conn.execute(
        f"INSERT INTO {table} (msg_id, msg_date, msg_time, message, category) VALUES( :id, :msg_date, :msg_time, :message, :category ) ON CONFLICT DO NOTHING",
        parameters={
            "id": f"{message[0]}",
            "msg_date": f"{message[1]}",
            "msg_time": f"{message[2]}",
            "message": f"{message[3]}",
            "category": f"{category}",
        },
    )


@flow
def process_error_messages(db_conn):
    print("proccess error messages")
    message_ids = []
    message_html = ""
    messages = db_conn.fetch_many("select * from messages where msg_type = 'error'")

    print(messages)
    for message in messages:
        insert_message(db_conn, "error_messages", message)
        message_ids.append(message[0])
        message_html += f"<p> {message[1]} / {message[2]}: {message[3]}</p>"
        print(f"insert errr ID: {message[0]}")

    if message_html != "":
        email_send_message(
            email_server_credentials=email_server_credentials,
            subject="Error Message in Log",
            msg=f""" Hi, <br>
                        <p>Following messages were raised in System</p>
                        {message_html}
                        <br> Thank You. <br>
                    """,
            email_to="daniel.stuckenberger@studmail.htw-aalen.de",
        )

    print("proccess error messages")
    return message_ids


def process_warning_messages(db_conn):
    print("proccess warning messages")
    message_ids = []
    category = ""

    messages = db_conn.fetch_many("select * from messages where msg_type = 'warning'")

    for message in messages:
        text = message[3]
        if "[GLOBAL]" in text:
            category = "global"
        elif "[SPECIAL]" in text:
            category = "special"
        else:
            category = "unknown"

        insert_message_with_category(db_conn, "warning_messages", message, category)
        message_ids.append(message[0])
        print(f"insert warning ID: {message[0]}")

    return message_ids


@flow
def process_success_messages(db_conn):
    print("proccess success messages")
    message_ids = []
    messages = db_conn.fetch_many("select * from messages where msg_type = 'success'")

    for message in messages:
        insert_message(db_conn, "success_messages", message)
        message_ids.append(message[0])
        print(f"insert success ID: {message[0]}")

    return message_ids


@flow
def delete_original_messages(db_conn, message_ids):
    print("delete original messages")

    for id in message_ids:
        db_conn.execute("DELETE FROM messages WHERE msg_id = '{}'".format(id))


@flow
def allocate_messages():
    message_ids = []
    message_ids += process_success_messages(db_conn)
    message_ids += process_warning_messages(db_conn)
    message_ids += process_error_messages(db_conn)

    delete_original_messages(db_conn, message_ids)


if __name__ == "__main__":
    allocate_messages()
