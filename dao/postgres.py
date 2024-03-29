# Function to create a table in the database
from psycopg2 import Error


def create_table(connection):
    try:
        cursor = connection.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS CODE_ALARM (
            id SERIAL PRIMARY KEY,
            title VARCHAR(100) NOT NULL,
            discord_url VARCHAR(100),
        );
        '''
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully")
    except Error as e:
        print(f"Error creating table: {e}")


# Function to insert data into the table
def insert_data(connection, _id, title, discord_url):
    try:
        cursor = connection.cursor()
        insert_query = '''
        INSERT INTO public."CODE_ALARM" (id, title, discord_url) VALUES (%s, %s, %s);
        '''
        cursor.execute(insert_query, (_id, title, discord_url))
        connection.commit()
        print("Data inserted successfully")
    except Error as e:
        print(f"Error inserting data: {e}")


# Function to fetch data from the table
def fetch_data(connection):
    try:
        cursor = connection.cursor()
        select_query = '''
        SELECT * FROM public."CODE_ALARM";
        '''
        cursor.execute(select_query)
        rows = cursor.fetchall()
        print("Fetched data:")
        for row in rows:
            print(row)
    except Error as e:
        print(f"Error fetching data: {e}")


def get_data_by_id(connection, _id) -> []:
    try:
        cursor = connection.cursor()
        select_query = f'''
        SELECT * FROM public."CODE_ALARM" WHERE id = {_id};
        '''
        cursor.execute(select_query)
        return cursor.fetch()
    except Error as e:
        print(f"Error fetching data: {e}")
