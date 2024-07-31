import time

from postgre_db import Postgre_db
from update_ip import UpdateID

#--------------------------------------------- обнов айпи для коннекта
up_id = UpdateID('', '', 16)
up_id.main()
time.sleep(5)
#
# #---------------------------------------------полдключение к бд
pg_webapp_fastapi = Postgre_db("root", 'rvSNS#acAJ@Je0c-o23nIVe]/cc!wAS5Xvev', "89.110.90.190", '5432',
                                            "agro_olymp")


pg_webapp_fastapi.set_connection()

table_name = 'test2'
#---------------------------------------------------------
res = pg_webapp_fastapi.clear_table_and_reset_id(table_name)

if res:
    print("Очищена")
else:
    print("Нет такой")
#---------------------------------------------------------
#-------------------------------------------------------------создание табилца

columns = {
    'id': 'SERIAL PRIMARY KEY',
    'a': 'VARCHAR(100)',
    'b': 'VARCHAR(100)',
    'c': 'VARCHAR(100)',
    'd': 'VARCHAR(100)'
}

pg_webapp_fastapi.create_table("test2", columns)
#-------------------------------------------------------------

#--------------------------------------------------------- вставк строки
data = {
    'a': '1',
    'b': '1',
    'c': '1',
    'd': '1'
}
pg_webapp_fastapi.insert_row(table_name, data)

data = {
    'a': '12',
    'b': '12',
    'c': '12',
    'd': '12'
}
pg_webapp_fastapi.insert_row(table_name, data)


data = {
    'a': '12',
    'b': '12',
    'c': '12',
    'd': '12'
}
pg_webapp_fastapi.insert_row(table_name, data)
#---------------------------------------------------------

#---------------------------------------------------------все строки в принт из таблицы
recs = pg_webapp_fastapi.get_all_db_info(table_name)
if recs:
    print('show all table info')
    for r in recs:
        print(r)
#---------------------------------------------------------


#--------------------------------------------------------- поиск строки по значению в колонке
column_name = 'a'
value = '12'

found_rows = pg_webapp_fastapi.find_rows_by_column_value(table_name, column_name, value)

if found_rows:
    print("НАЙДЕННЫЕ СТРОКИ:", found_rows)
#---------------------------------------------------------


#---------------------------------------------------------удаление строки
column_name = "a"
value = '1'

pg_webapp_fastapi.delete_row_by_column(table_name, column_name, value)
#---------------------------------------------------------

#--------------------------------------------------------- поиск дубликатов
column_name = 'a'
value = '12'

duplicates = pg_webapp_fastapi.find_duplicates_by_column_value(table_name, column_name, value)

if duplicates is not None:
    print("дубликтаы ", duplicates)
else:
    print("не найдено дубликатов")
#---------------------------------------------------------

#--------------------------------------------------------- обновлении колонки по первичному ключу
primary_key_column = 'id'
new_data = {
    'id': 1,
    'a': '1234',
    'b': '1234',
    'c': '1234',
    'd': '1234'
}
success = pg_webapp_fastapi.update_row(table_name, primary_key_column, new_data)

if success:
    print("Обновлена строка")
else:
    print("не обновлена строка")
#---------------------------------------------------------


#--------------------------------------------------------- обнолвение колокни в строке
primary_key_column = 'id'
primary_key_value = 3
column_name = 'c'
new_value = '9999'

res = pg_webapp_fastapi.update_specific_column(table_name, primary_key_column, primary_key_value, column_name, new_value)

if res:
    print("Обновилось.")
else:
    print("не обновилось")
#---------------------------------------------------------

#--------------------------------------------------------- возвращает нуэные значения колонки из строки
column_name = 'a'
search_v= '123'

fr = pg_webapp_fastapi.find_specific_rows_by_column_value(table_name, column_name, search_v, 'b', 'c', 'd')

if fr:
    print("Найденные строки:")
    for row in fr:
        print(row)

#---------------------------------------------------------

#--------------------------------------------------------- поиск по строки по нескольким значениям из колонок
columns_to_ret = ['c', 'd']

search = {
    'a': '12',
    'b': '12'
}

fr = pg_webapp_fastapi.find_specific_rows_by_column_values(table_name, columns_to_ret, **search)

if fr:
    print("найденные строки:")
    for row in fr:
        print(row)
#---------------------------------------------------------
recs = pg_webapp_fastapi.get_all_db_info(table_name)
if recs:
    print('show all table info')
    for r in recs:
        print(r)

#------------------------------------------------------закрытие соединения
pg_webapp_fastapi.close_connection()
