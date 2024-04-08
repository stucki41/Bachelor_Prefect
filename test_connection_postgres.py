from prefect import flow, task
import psycopg2


# Function to connect to PostgreSQL database
@task
def connect_to_postgresql():
    try:
        # Establish connection
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="admin",
            host="0.0.0.0",
            port="5432",
        )
        print("Connected to PostgreSQL database successfully.")
        return connection
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")


@task
def fetch_data(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM messages")
    data = cursor.fetchall()
    cursor.close()
    return data


@flow
def main():
    connection = connect_to_postgresql()
    print(fetch_data(connection))


if __name__ == "__main__":
    main()
