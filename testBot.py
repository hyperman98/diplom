#Библиотеки
import xmpp 
from config import jid, jidpassword
from BotClass import JabberBot
from RequestClass import RequestDataBase

requesDB = RequestDataBase()
#Основной метод для обработки сообщинй от пользователя
def message_handler(conn, mess):
    text = mess.getBody()
    user = mess.getFrom()

    if str(text).startswith("!"):
        commands_request(conn, mess)
    else:
        conn.send(xmpp.Message(user, "Я тебя не понимаю..."))

def commands_request(conn, mess):
    try:
        text = mess.getBody()
        user = mess.getFrom()

        arguments = str(text).split()
        if text == "!addcl":
            conn.send(xmpp.Message(user, requesDB.add_car_log(arguments[0])))
        elif text == "!delcl":
            conn.send(xmpp.Message(user, requesDB.del_car_log(int(arguments[0]))))
        elif text == '!ссt':
            conn.send(xmpp.Message(user, requesDB.change_current_token(arguments[0], arguments[1])))
        elif text == '!addol':
            conn.send(xmpp.Message(user, requesDB.add_order_log()))
        elif text == '!delol':
            conn.send(xmpp.Message(user, requesDB.del_order_log(arguments[0])))
        elif text == '!addc':
            conn.send(xmpp.Message(user, requesDB.add_car(arguments[0])))
        elif text == '!cc':
            conn.send(xmpp.Message(user, requesDB.change_car(arguments[0])))
        elif text == '!gup':
            conn.send(xmpp.Message(user, requesDB.get_upcoming_orders(arguments[0])))
        elif text == '!ac':
            conn.send(xmpp.Message(user, requesDB.actuality_check(arguments[0])))
        elif text == '!ico':
            conn.send(xmpp.Message(user, requesDB.insert_current_order(arguments[0])))
        elif text == '!dco':
            conn.send(xmpp.Message(user, requesDB.del_current_order(arguments[0])))
        elif text == '!uco':
            conn.send(xmpp.Message(user, requesDB.update_current_order(arguments[0])))
        elif text == '!cs':
            conn.send(xmpp.Message(user, requesDB.change_status(arguments[0])))
        else:
            conn.send(xmpp.Message(user, "Такой команды нет"))

    except Exception as ex:
        conn.send(xmpp.Message(user, str(ex)))
    

#Авторизация и запуск бота
bot = JabberBot(jid, jidpassword)
bot.register_handler('message', message_handler)
bot.start()
