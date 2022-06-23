import psycopg2 as pg
from sshtunnel import SSHTunnelForwarder
from config import SSH_H
import datetime
#Проверка соединения и само соединение
def ConnectToDatbase():
    try:
        print('Connecting to the PostgreSQL Database...')
        ssh_tunnel = SSHTunnelForwarder(
            (SSH_H, 334), #ip address and port
            ssh_username="artem_valiakhmetov", #имя пользователя
            ssh_private_key= 'D:/keys/home/.ssh/id_rsa',# путь к файлу где расположен ssh ключ (не .pub)
            ssh_private_key_password= '',# пароль (в данном случае пароль отсутствует)
            remote_bind_address=("localhost", 5432) # не особо понял что это
        )
        ssh_tunnel.start()
        print("Success ssh connect!")
        conn = pg.connect(
            host = "localhost",
            port = ssh_tunnel.local_bind_port,
            user = "postgres",
            password = "",
            database = "postgres"
        )
        print("Successfully connection...")
        return conn
    except Exception as ex:
        print("Error connection to database...")
        print(ex)

class RequestDataBase:
    def __init__(self):
        self.conn = ConnectToDatbase()
    def add_car_log(self, operation):
        try:
            with self.conn.cursor() as cursor:
                date_cur = datetime.datetime.now()
                insert_car_log = "INSERT INTO cars_log (id, resource_id, operation, time_log_created, user_id) VALUES(%s, %s, %s, %s, %s)"
                val = [2, 3, operation, date_cur, 1]
                cursor.execute(insert_car_log, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)
    def add_order_log(self, operation):
        try:
            with self.conn.cursor() as cursor:
                date_cur = datetime.datetime.now()
                insert_order_log = "INSERT INTO order_list_log (id serial primary key, payment_transaction_id int NOT NULL, service_id int NOT NULL, service_type_id int NOT NULL, time_placed timestamptz NOT NULL, time_start timestamptz NOT NULL, time_finish_predicted timestamptz, time_finish_real timestamptz NOT NULL, order_place_start varchar(255) NOT NULL, order_place_predicted varchar(255) NOT NULL, order_place_real varchar(255) NOT NULL, price money NOT NULL, provider int NOT NULL, receiver int NOT NULL, client int NOT NULL) VALUES (1,1,1,'2022-06-17 01:02:03', '2022-06-17 01:02:03','2022-06-17 12:02:03','2022-06-17 12:02:03', 'Walmart street, 14.24', 'Times street, 13.14', 'Walmart street, 14.24', 100, 1,1,1);"
                val = [2, 3, operation, date_cur, 1]
                cursor.execute(insert_car_log, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)
    def del_car_log(self, id):
        try:
            with self.conn.cursor() as cursor:
                delete_car_log = "DELETE FROM cars_log WHERE id = %s"
                cursor.execute(delete_car_log, id)
                self.conn.commit()
            return "Успешно!"
        except Exception as ex:
            return str(ex)
    def insert_current_order(self, order_id, service_type_id, provider_id, resourse_id, time_start_predicted,time_finished_predicted):
        try:
            with self.conn.cursor() as cursor:
                gap = 60
                insert_current_order = "INSERT INTO current_timetable (order_id, service_type_id, provider_id, resourse_id, time_start_predicted, time_finished_predicted, gap) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                val = [order_id, service_type_id, provider_id, resourse_id, time_start_predicted,time_finished_predicted, gap]
                cursor.execute(insert_current_order, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def del_current_order(self, order_id):
        try:
            with self.conn.cursor() as cursor:
                delete_current_order = "DELETE FROM current_timetable WHERE order_id = %s"
                val = [order_id]
                cursor.execute(delete_current_order, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def get_upcoming_orders(self):
        try:
            with self.conn.cursor() as cursor:
                select_upcoming_orders = "SELECT * FROM current_timetable"
                cursor.execute(select_upcoming_orders)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def change_status(self, status):
        try:
            with self.conn.cursor() as cursor:
                date_cur = datetime.datetime.now()
                insert_car_log = "INSERT INTO cars_log (resource_id, operation, time_log_created, user_id) VALUES(%s, %s, %s, %s)"
                val = [2, operation, date_cur, 5]
                cursor.execute(insert_car_log, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def create_ticket(self, type, reporter, assignee, time_created, time_assigned, time_changed, time_closed, status, description):
        try:
            with self.conn.cursor() as cursor:
                date_cur = datetime.datetime.now()
                insert_car_log = "INSERT INTO cars_log (resource_id, operation, time_log_created, user_id) VALUES(%s, %s, %s, %s)"
                val = [2, operation, date_cur, 5]
                cursor.execute(insert_car_log, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def change_ticket_status(self, id, status):
        try:
            with self.conn.cursor() as cursor:
                date_cur = datetime.datetime.now()
                insert_car_log = "INSERT INTO cars_log (resource_id, operation, time_log_created, user_id) VALUES(%s, %s, %s, %s)"
                val = [2, operation, date_cur, 5]
                cursor.execute(insert_car_log, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def add_car(self, resource_id, user_id, order_status, order_id, direction, battery_level, board_voltage):
        try:
            with self.conn.cursor() as cursor:
                date_cur = datetime.datetime.now()
                insert_car_log = "INSERT INTO cars_log (resource_id, operation, time_log_created, user_id) VALUES(%s, %s, %s, %s)"
                val = [2, operation, date_cur, 5]
                cursor.execute(insert_car_log, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def del_car(self, id):
        try:
            with self.conn.cursor() as cursor:
                date_cur = datetime.datetime.now()
                insert_car_log = "INSERT INTO cars_log (resource_id, operation, time_log_created, user_id) VALUES(%s, %s, %s, %s)"
                val = [2, operation, date_cur, 5]
                cursor.execute(insert_car_log, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def change_car(self, id, resource_id, user_id, order_status, order_id, doors_output, engine_input, central_lock_input, ignition_input, doors_input, input4, controller_status, signal_gsm, operator_gsm, signal_gps, location, speed, direction, address, battery_level, board_voltage):
        try:
            with self.conn.cursor() as cursor:
                date_cur = datetime.datetime.now()
                insert_car_log = "INSERT INTO cars_log (resource_id, operation, time_log_created, user_id) VALUES(%s, %s, %s, %s)"
                val = [2, operation, date_cur, 5]
                cursor.execute(insert_car_log, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def change_current_token(self, id, current_token):
        try:
            with self.conn.cursor() as cursor:
                change_current_token = "UPDATE current_cars SET current_token = %s WHERE id = %s"
                val = [current_token, id]
                cursor.execute(change_current_token, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def update_current_order(self, order_id, service_type_id, provider_id, resource_id, time_start_predicted, time_finished_predicted):
        try:
            with self.conn.cursor() as cursor:
                update_current_order = "UPDATE current_timetable SET order_id = %s, service_type_id = %s, provider_id = %s, resource_id = %s, time_start_predicted = %s, time_finished_predicted = %s, gap = %s"
                val = [order_id, service_type_id, provider_id, resource_id, time_start_predicted,
                       time_finished_predicted]
                cursor.execute(update_current_order, val)
                self.conn.commit()
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)

    def actuality_check(self,id):
        try:
            with self.conn.cursor() as cursor:
                date_cur = datetime.datetime.now()
                insert_car_log = "INSERT INTO cars_log (resource_id, operation, time_log_created, user_id) VALUES(%s, %s, %s, %s)"
                val = [2, operation, date_cur, 5]
                cursor.execute(insert_car_log, val)
                self.conn.commit()
                select_orders = "SELECT * FROM current_orders WHERE id = %s AND state = 151 OR state = 159"
                val = [id]
                res = cursor.execute(select_orders)
                if (res == True):
                    delete_orders = "DELETE FROM current_timetable WHERE id = %s"
                    val = [id]
                    cursor.execute(delete_orders, val)
                return "Успешный запрос!"
        except Exception as ex:
            return str(ex)
        






