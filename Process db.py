import sqlite3
from sqlite3 import Error
import json

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def select_all(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM events")

    rows = cur.fetchall()

    for row in rows:
        print(row)


def get_sender_id(conn):
    """
        Query sender id in events
        :param conn: the Connection object
        :return: rows contains sender id
        """
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT(sender_id) FROM events")

    rows = cur.fetchall()
    print("Sender IDs: ")
    for row in rows:
        print(row)

    return rows
def get_all_conservation_data(database):
    """
    Get the conversation of each customer with botchat
    :param database:
    :return: json data of conservations
    """
    conn = create_connection(database)
    cur = conn.cursor()
    with conn:
        print("1. Query all user data")
        sender_ids = get_sender_id(conn)
    conservations = {}
    id = 0
    for sender_id in sender_ids:
        conservation = {
            'user': [],
            'bot' : []
        }
        id+= 1
        print(sender_id)
        cur.execute("SELECT data FROM events WHERE type_name IN ('user', 'bot') AND sender_id = ? ORDER BY timestamp ", (sender_id[0],))
        conservation_data_rows = cur.fetchall() # return a list of tuple
        for data_tuple in conservation_data_rows:
            data = json.loads(data_tuple[0])
            if data['event'] == 'user':
                conservation['user'].append({'text': data['text'],
                                        'intent': data['parse_data']['intent']})
            elif data['event'] == 'bot':
                conservation['bot'].append({'text': data['text'],
                                       'utter_action': data['metadata']['utter_action']})
            conservations[str(id)] = conservation

    with open('json_data.json', "w", encoding="utf-8") as json_file:
        json.dump(conservations, json_file, indent=4, ensure_ascii=False)

    return conservations
def select_all_user_data(conn):
    """
    Query user data in the table events
    :param conn: the Connection object
    :return: rows contain users_data
    """
    cur = conn.cursor()
    cur.execute("SELECT data FROM events where type_name = 'user'")

    rows = cur.fetchall()

    for row in rows:
        print(row)
    return rows


def get_all_user_message(database):
    # Hàm này là để lấy message mà người dùng nhập và in vào file txt

    # create a database connection
    conn = create_connection(database)
    with conn:

        print("1. Query all user data")
        text_data_list = select_all_user_data(conn)
        sender_id = get_sender_id(conn)
    texts =[]
    print("Messages: ")
    for text_data_tuple in text_data_list:
        data_string = text_data_tuple[0]
        data = json.loads(data_string)
        text = data["text"]
        print(text)
        texts.append(text)
    with open(r"D:\Rasa\webchatbot\message_user.txt", "w",encoding="utf-8") as file:
        for text in texts:
            file.write(text+"\n")


if __name__ == '__main__':
    database_path = r"D:\Rasa\webchatbot\chatbot.db"
    # Sửa cái database path nhé
    # get_all_user_message(database_path)
    get_all_conservation_data(database_path)