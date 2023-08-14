import psycopg2
from psycopg2.sql import Identifier, SQL




def create_tables(conn):
    with conn.cursor() as cur:
      cur.execute("""
                 CREATE TABLE IF NOT EXISTS clients(
                 client_id SERIAL PRIMARY KEY,
                 first_name varchar(15) NOT NULL,
                 last_name varchar(15) NOT NULL,
                 email varchar(20)
                 );
                 """)
      conn.commit()
      cur.execute("""
                 CREATE TABLE IF NOT EXISTS phone_book(
                     client_id INTEGER NOT NULL REFERENCES clients(client_id),
                     phone varchar(16) not null
                 );
                 """)
      conn.commit()

def add_client(conn, name1, name2, e_mail=None, phones=None):
    print(phones)
    with conn.cursor() as cur:
        query = f"insert into clients(first_name,last_name,email)" \
                f" values('{name1}','{name2}','{e_mail}') RETURNING client_id;"
        cur.execute(query)
        id_new =cur.fetchone()[0]

        for ph in phones:
          query = f"insert into phone_book(client_id,phone)" \
                  f" values('{id_new}','{ph}') RETURNING client_id;"
          cur.execute(query)
    conn.commit()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
     query = f"insert into phone_book(client_id,phone)" \
            f" values('{client_id}','{phone}');"
     cur.execute(query)
     conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, e_mail=None):
    arg_list = {'first_name': first_name, "last_name": last_name, 'email': e_mail}
    with conn.cursor() as cur:
     for key, arg in arg_list.items():
        if arg:
             cur.execute(SQL("UPDATE Clients SET {}=%s WHERE client_id=%s").format(Identifier(key)), (arg, client_id))
     conn.commit()

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        query = f"delete from phone_book where client_id={client_id}"
        cur.execute(query)
        query = f"delete from clients where client_id={client_id}"
        cur.execute(query)
        conn.commit()

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        query = f"delete from phone_book where client_id='{client_id}' and phone='{phone}'"
        cur.execute(query)
        conn.commit()

def find_client(conn, first_name='', last_name='', e_mail='', phone=''):
    with conn.cursor() as cur:
        cur.execute("""
           SELECT  c.client_id, c.first_name, c.last_name, c.email 
           from clients  c  JOIN 
           phone_book p  ON  c.client_id = p.client_id  
           WHERE first_name LIKE %s  and last_name LIKE %s 
		   and email LIKE %s and p.phone LIKE %s
		   group by c.client_id, c.first_name, c.last_name, c.email
           """, ('%'+str(first_name), '%'+str(last_name), '%'+str(e_mail),'%'+str(phone)))
        return print(cur.fetchall())

def view_tbl(conn, name: str):
    with conn.cursor() as cur:
        query = f"SELECT * FROM {name};"
        cur.execute(query)
        return print(cur.fetchall())

if __name__ == '__main__':
    with psycopg2.connect(database="oxana_db", user="postgres", password="post_oxana") as conn:
         create_tables(conn)
         add_client(conn,'Ольга','Соколова','ooo@google.com',['8-911-223-45-78','8-911-223-45-99','8-911-223-45-11']  )
         add_client(conn, 'Олег', 'Фридман', 'fri@google.com','8-911-229-99-99')
         add_phone(conn,2,'8-911-111-11-11')
         change_client(conn, 2, 'Paramon')
         delete_client(conn, 1)
         find_client(conn,'','Фридман')
         view_tbl(conn, 'clients')
         view_tbl(conn, 'phone_book')
    conn.close()




