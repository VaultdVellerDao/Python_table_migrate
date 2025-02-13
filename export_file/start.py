import os
import shutil
import psycopg2
from configparser import ConfigParser
import csv

# Загрузка конфигурации
def load_config(filename):
    config = ConfigParser()
    config.read(filename)
    return config['database']

# Подключение к базе данных
def connect_db(config):
    return psycopg2.connect(
        host=config['source_host'],
        database=config['source_db'],
        user=config['source_user'],
        password=config['source_password']
    )

# Получение списка таблиц в схеме
def get_table_list(connection, schema):
    query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        tables = cursor.fetchall()
    return [table[0] for table in tables]

# Экранирование строк для SQL
def escape_sql_string(value):
    if value is None:
        return "NULL"
    value = value.replace("'", "''")  # Escape single quotes
    value = value.replace('\n', '\\n')  # Replace new lines with \n
    return value

# Получение комментариев к столбцам
def get_column_comments(connection, schema, table):
    query = f"""
        SELECT a.attname AS column_name, d.description AS comment
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
        WHERE c.relname = '{table}' AND a.attnum > 0 AND NOT a.attisdropped
        AND c.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '{schema}')
    """
    comments = {}
    with connection.cursor() as cursor:
        cursor.execute(query)
        column_comments = cursor.fetchall()
        for column_name, comment in column_comments:
            comments[column_name] = comment or ""
    return comments

# Получение DDL для таблицы
def get_table_ddl(connection, schema, table):
    query = f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    ddl = f'-- DDL for table "{schema}"."{table}"\nCREATE TABLE "{schema}"."{table}" (\n'
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = cursor.fetchall()
        comments = get_column_comments(connection, schema, table)
        for column in columns:
            column_name, data_type, is_nullable, column_default = column[0], column[1], column[2], column[3]
            nullable = "NULL" if is_nullable == 'YES' else "NOT NULL"
            default = f"DEFAULT {column_default}" if column_default else ""
            ddl += f"    {column_name} {data_type} {nullable} {default},\n"
    ddl = ddl.rstrip(',\n') + '\n);\n'
    
    return ddl

# Экспорт таблиц в CSV и DDL
def export_table_to_csv_and_ddl(connection, schema, table, output_dir):
    # Экспорт данных
    query = f'SELECT * FROM "{schema}"."{table}"'
    cursor = connection.cursor()
    cursor.execute(query)
    csv_file = os.path.join(output_dir, schema, f"{table}.csv")
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)

    # Запись данных в CSV файл
    with open(csv_file, 'w', newline='', encoding='utf8') as f:
        writer = csv.writer(f, delimiter=';')
        # Записываем заголовки
        column_names = [desc[0] for desc in cursor.description]
        writer.writerow(column_names)
        # Записываем строки
        for row in cursor:
            writer.writerow(row)

    print(f"Экспорт данных из таблицы {schema}.{table} в {csv_file}")

    # Экспорт DDL
    ddl = get_table_ddl(connection, schema, table)
    ddl_file = os.path.join(output_dir, schema, 'tables_ddl.txt')
    with open(ddl_file, 'a') as f:
        f.write(ddl + '\n')
    print(f"Экспорт DDL для таблицы {schema}.{table} в {ddl_file}")

# Экспорт комментариев к столбцам в один текстовый файл для каждой схемы
def export_column_comments(connection, schema, output_dir):
    tables = get_table_list(connection, schema)
    comments_file = os.path.join(output_dir, schema, 'column_comments.txt')
    os.makedirs(os.path.dirname(comments_file), exist_ok=True)

    with open(comments_file, 'w') as f:
        for table in tables:
            comments = get_column_comments(connection, schema, table)
            for column, comment in comments.items():
                escaped_comment = escape_sql_string(comment)
                f.write(f'COMMENT ON COLUMN "{schema}"."{table}"."{column}" IS \'{escaped_comment}\';\n')
    
    print(f"Экспорт комментариев для схемы {schema} в {comments_file}")

# Архивирование и удаление временной папки
def archive_and_cleanup(source_dir, archive_file):
    shutil.make_archive(archive_file, 'gztar', source_dir)
    shutil.rmtree(source_dir)
    print(f"Архивирование и удаление {source_dir} завершено.")

# Чтение списка таблиц из файла
def load_table_list(filename):
    schema_tables = {}
    current_schema = None
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if line.startswith('[') and line.endswith(']'):
                current_schema = line[1:-1]
                schema_tables[current_schema] = []
            elif current_schema and line:
                schema_tables[current_schema].append(line)
    return schema_tables

def main():
    # Загрузка конфигурации
    config = load_config('./config.cfg')
    download_result = config.get('download_result', './result')

    # Спросить пользователя, всю схему выгружать или только таблицы из файла
    choice = input("Выгружать все таблицы схемы или только те, что указаны в table_list.txt? (all/file): ").strip().lower()

    # Если выгружаем таблицы из файла
    if choice == 'file':
        table_list_file = './table_list.txt'
        schema_tables = load_table_list(table_list_file)
        schemas = schema_tables.keys()
    else:
        schemas = config.get('schemas', '').split(',')
        schema_tables = None

    # Создание директории для результатов
    os.makedirs(download_result, exist_ok=True)

    # Подключение к базе данных
    connection = connect_db(config)

    try:
        for schema in schemas:
            print(f"Получение списка таблиц для схемы {schema}...")
            if schema_tables:
                tables = schema_tables.get(schema, [])
                if not tables:
                    print(f"В файле table_list.txt не указаны таблицы для схемы {schema}. Пропускаем.")
                    continue
            else:
                tables = get_table_list(connection, schema)

            for table in tables:
                export_table_to_csv_and_ddl(connection, schema, table, download_result)

            export_column_comments(connection, schema, download_result)

    finally:
        connection.close()
        print('Выгрузка данных завершена.')

    archive_and_cleanup(download_result, './download_result')

if __name__ == '__main__':
    main()
