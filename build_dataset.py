import time
import os
import http
import requests
import json
import csv

from dotenv import load_dotenv

load_dotenv()

CHAT_GPT_URL = "https://api.proxyapi.ru/openai/v1/chat/completions"
API_KEY = os.getenv("API_KEY")
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

OUTPUT_CSV_FILE = '/Users/igorkopylov/Projects/spbgu/diploma_work/prepared_dataset/full_test_dataset.csv'
DATA_FILE_PATH = '/Users/igorkopylov/Projects/spbgu/diploma_work/dataset_info/wikisql_test.json'
MAX_TOKEN_THRESHOLD = 30   # max_query_tokens = 39; avg_query_tokens = 11.709535205945333
LIMIT_RESPONSE_TOKENS = 35

DEFAULT_DB_PROMPT = """Я хочу создать таблицу для базы данных {db_name}.
Напиши SQL-запрос для создания этой таблицы, для названя колонок ориентируйся на переданный запрос.
Предоставь только SQL-запрос без дополнительных пояснений."""
REQUEST_FOR_GETTING_PG_DB_SHEMA = 'PostgreSQL версии 13'
REQUEST_FOR_GETTING_CH_DB_SHEMA = 'ClickHouse'
REQUEST_FOR_GETTING_CH_QUERY = """У меня есть sql запрос и примерная схема таблицы для {db_name}, построй такой же запрос к таблице.
Предоставь только SQL-запрос без дополнительных пояснений."""


def _build_db_schema_request(db_name: str, sql_query: str,) -> str:
    message = DEFAULT_DB_PROMPT.format(db_name=db_name)
    return message + sql_query


def _build_ch_query(db_name: str, ch_db_schema:str, sql_query: str,) -> str:
    return REQUEST_FOR_GETTING_CH_QUERY.format(db_name=db_name) + ch_db_schema + '\n' + sql_query


def _make_request(content_message: str) -> str:
    payload = {
        "model": "gpt-3.5-turbo",  # change model
        "max_tokens": LIMIT_RESPONSE_TOKENS,
        "messages": [
            {
                "role": "user",
                "content": content_message
            }
        ]
    }
    response = requests.post(CHAT_GPT_URL, headers=HEADERS, data=json.dumps(payload))

    if response.status_code == http.HTTPStatus.OK:
        json_response = response.json()
        return json_response['choices'][0]['message']['content']

    else:
        raise RuntimeError(f'Ошибка при выполнении запроса: {response.status_code} {response.text}')


def clean_sql_query(query):
    cleaned_query = query.replace('\n', ' ').replace('`', '')
    cleaned_query = ' '.join(cleaned_query.split())
    return cleaned_query


def main():
    with open(DATA_FILE_PATH, 'r', encoding='utf-8') as file:
        file_content = json.load(file)

    for frame_num, json_frame in enumerate(file_content[691:]):
        natural_languge = json_frame['question']
        sql_query: str = json_frame['answer']
        if len(sql_query.split()) > MAX_TOKEN_THRESHOLD:
            continue

        pg_db_response = _make_request(_build_db_schema_request(REQUEST_FOR_GETTING_PG_DB_SHEMA, sql_query))
        pg_query_response = _make_request(_build_ch_query(REQUEST_FOR_GETTING_PG_DB_SHEMA, pg_db_response, sql_query))
        ch_db_response = _make_request(_build_db_schema_request(REQUEST_FOR_GETTING_CH_DB_SHEMA, sql_query))
        ch_query_response = _make_request(_build_ch_query(REQUEST_FOR_GETTING_CH_DB_SHEMA, ch_db_response, sql_query))
        
        with open(OUTPUT_CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(
                [
                    clean_sql_query(natural_languge),
                    clean_sql_query(pg_db_response),
                    clean_sql_query(pg_query_response),
                    clean_sql_query(ch_db_response),
                    clean_sql_query(ch_query_response)
                ]
            )

        if frame_num % 30 == 0:
            print(f'Num of frame is `{frame_num}`')
        time.sleep(0.2)


if __name__ == '__main__':
    main()
