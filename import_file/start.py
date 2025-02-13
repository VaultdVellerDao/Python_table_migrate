import os
import tarfile
import psycopg2
from configparser import ConfigParser
import re
import csv

# Загрузка конфигурации из файла
# Читаем файл конфигурации, извлекаем параметры подключения к БД
# Файл должен содержать секцию [database] с параметрами хоста, имени БД, пользователя и пароля
def load_config(filename):
    config = ConfigParser()
    config.read(filename)
    return config['database']

# Подключение к базе данных PostgreSQL
# Используем параметры из конфигурационного файла для установления соединения
def connect_db(config):
    return psycopg2.connect(
        host=config['host'],
        database=config['db'],
        user=config['user'],
        password=config['password']
    )

# Разархивирование архива с выбором пользователя
def extract_archive(archive_file, extract_to):
    if os.path.exists(archive_file):  # Проверяем существование архива
        choice = input(f"Разархивировать архив {archive_file}? (y/n): ").strip().lower()
        if choice == 'y':
            with tarfile.open(archive_file, 'r:gz') as archive:  # Открываем архив
                archive.extractall(extract_to)  # Извлекаем содержимое в указанную директорию
            print(f"Архив {archive_file} разархивирован в {extract_to}")
        else:
            print("Разархивирование пропущено пользователем.")
    else:
        print(f"Архив {archive_file} не найден, пропускаем разархивирование.")

# Очистка DDL от комментариев и пустых строк
# Удаляем SQL-комментарии и пустые строки для корректной обработки SQL-запросов
def clean_ddl_content(ddl):
    ddl = re.sub(r'--.*', '', ddl)  # Удаляем однострочные комментарии
    ddl = re.sub(r'/\*.*?\*/', '', ddl, flags=re.DOTALL)  # Удаляем многострочные комментарии
    ddl = re.sub(r'\n\s*\n', '\n', ddl)  # Удаляем пустые строки
    ddl = re.sub(r',\s*\)', ')', ddl)  # Исправляем ошибки с запятыми перед закрывающей скобкой
    return ddl

# Создание таблиц из DDL файлов
# Читаем SQL-запросы из файла, очищаем их и выполняем создание таблиц
def create_tables_from_ddl(connection, schema, ddl_file):
    if os.stat(ddl_file).st_size == 0:  # Проверяем, не пуст ли файл DDL
        print(f"Файл {ddl_file} пуст. Пропускаем создание таблиц.")
        return
    
    choice = input(f"Создать таблицы из DDL для схемы {schema}? (y/n): ").strip().lower()
    if choice != 'y':
        print("Создание таблиц пропущено пользователем.")
        return

    with open(ddl_file, 'r') as file:
        ddl = file.read()

    ddl = clean_ddl_content(ddl)  # Очищаем DDL от комментариев
    queries = ddl.split(';')  # Разбиваем на отдельные SQL-запросы

    with connection.cursor() as cursor:
        for query in queries:
            query = query.strip()
            if query:
                print(f"Выполнение запроса:\n{query};")
                try:
                    cursor.execute(query + ';')  # Выполняем SQL-запрос
                except psycopg2.Error as e:
                    print(f"Ошибка выполнения запроса: {e}")
                    print(f"Запрос: {query}")
                    continue
        print(f"Созданы таблицы из DDL для схемы {schema}.")

    connection.commit()

# Выдача прав пользователям на схемы и таблицы
def grant_permissions(connection, schemas, users):
    with connection.cursor() as cursor:
        for schema in schemas:
            for user_to_grant in users:
                usage_query = f"GRANT USAGE ON SCHEMA {schema} TO {user_to_grant};"
                cursor.execute(usage_query)
                print(f"Выданы права USAGE пользователю {user_to_grant} на схему {schema}.")

                create_query = f"GRANT CREATE ON SCHEMA {schema} TO {user_to_grant};"
                cursor.execute(create_query)
                print(f"Выданы права CREATE пользователю {user_to_grant} на схему {schema}.")

                select_query = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {user_to_grant};"
                cursor.execute(select_query)
                print(f"Выданы права SELECT пользователю {user_to_grant} на таблицы схемы {schema}.")

    connection.commit()

# Загрузка CSV в таблицы с возможностью выполнения TRUNCATE
def load_csv_to_tables(connection, schema, import_dir):
    schema_path = os.path.join(import_dir, schema)
    csv_tables = [os.path.splitext(csv_file)[0] for csv_file in os.listdir(schema_path) if csv_file.endswith('.csv')]

    truncate_choice = input(f"Выполнить TRUNCATE для всех таблиц, в которые будут загружены CSV-файлы? (y/n): ").strip().lower()
    if truncate_choice == 'y':
        with connection.cursor() as cursor:
            for table in csv_tables:
                table_name = f'"{table}"'
                try:
                    cursor.execute(f'TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY;')
                    print(f"Таблица {schema}.{table_name} очищена (TRUNCATE выполнен).")
                except Exception as e:
                    print(f"Ошибка при выполнении TRUNCATE для таблицы {schema}.{table_name}: {e}")
                    connection.rollback()
                    continue
        connection.commit()

    for csv_file in csv_tables:
        table_name = f'"{csv_file}"'
        csv_path = os.path.join(schema_path, f"{csv_file}.csv")
        print(f"Загрузка данных из {csv_path} в таблицу {schema}.{table_name}")
        try:
            with connection.cursor() as cursor:
                with open(csv_path, 'r', encoding='utf-8') as file:
                    # COPY-запрос PostgreSQL для загрузки CSV-файла в таблицу
                    # STDIN означает, что данные будут считываться из переданного файла
                    cursor.copy_expert(f'COPY {schema}.{table_name} FROM STDIN WITH (FORMAT csv, HEADER TRUE, DELIMITER ";")', file)
            connection.commit()
            print(f"Данные загружены в таблицу {schema}.{table_name}")
        except Exception as e:
            connection.rollback()
            print(f"Ошибка при загрузке данных в таблицу {schema}.{table_name}: {e}")

# Основная функция
def main():
    config = load_config('config.cfg')
    import_dir_result = config.get('import_dir_result', './result')
    schemas = config.get('schemas', '').split(',')
    archive_file = config.get('archive_file', 'download_result.tar.gz')
    users = config.get('users', '').split(',')

    extract_archive(archive_file, import_dir_result)
    connection = connect_db(config)
    print("Подключение к базе данных успешно")

    try:
        for schema in schemas:
            schema_path = os.path.join(import_dir_result, schema)
            ddl_file = os.path.join(schema_path, 'tables_ddl.txt')
            if os.path.isfile(ddl_file):
                create_tables_from_ddl(connection, schema, ddl_file)
            else:
                print(f"DDL файл для схемы {schema} не найден. Продолжаем с CSV файлами.")
            load_csv_to_tables(connection, schema, import_dir_result)
        grant_permissions(connection, schemas, users)
    finally:
        connection.close()

if __name__ == '__main__':
    main()
