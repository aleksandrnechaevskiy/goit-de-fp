# goit-de-fp — Фінальний проєкт Data Engineering

Два пайплайни на даних про олімпійських атлетів:

1. **Streaming Pipeline** (Spark Structured Streaming + Kafka + MySQL): в реальному часі рахує середній зріст і вагу атлетів.
2. **Batch Data Lake** (Spark + Airflow): багаторівневе сховище landing → bronze → silver → gold.

## Структура

```
goit-de-fp/
├── .env                       # логіни/паролі (НЕ комітиться)
├── .env.example               # шаблон .env (комітиться)
├── .gitignore
├── requirements.txt
├── part1_streaming/
│   ├── config.py              # читання .env, налаштування Spark/Kafka/MySQL
│   ├── create_topics.py       # крок 0: створення Kafka-топіків
│   ├── produce_event_results.py  # етап 3: MySQL athlete_event_results → Kafka
│   ├── streaming_pipeline.py  # етапи 1–6: основний стрим
│   └── consume_output.py      # друк вихідного Kafka-топіку (для скриншоту)
├── part2_batch/
│   ├── landing_to_bronze.py   # FTP → landing/*.csv → bronze/{table} (parquet)
│   ├── bronze_to_silver.py    # чистка тексту + дедублікація → silver/{table}
│   ├── silver_to_gold.py      # join + avg → gold/avg_stats
│   └── project_solution.py    # Airflow DAG
└── screenshots/               # скриншоти результатів
```

## Встановлення

Потрібні Python 3.10+ і Java 8/11/17 (для Spark).

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Відкрийте `.env` у корені проєкту та впишіть свої дані доступу.
Якщо `.env` немає (наприклад, після `git clone`), створіть його з шаблону — **тільки якщо файлу ще не існує**, бо `cp` перезапише заповнений файл без попередження:

```bash
[ -f .env ] || cp .env.example .env
```

---

## Частина 1. Streaming Pipeline

### Налаштування `.env`

Впишіть дані доступу до MySQL і Kafka, які надає GoIT. У `KAFKA_TOPIC_PREFIX` і `MYSQL_OUTPUT_TABLE` додайте своє ім'я, щоб не конфліктувати з іншими студентами на спільних серверах.

JDBC-драйвер MySQL і Spark-Kafka конектор завантажуються автоматично через `spark.jars.packages` під час першого запуску (потрібен інтернет).

### Порядок запуску

```bash
cd part1_streaming

# 0. Створити вхідний і вихідний Kafka-топіки
python create_topics.py

# 1. Етап 3 (перша половина): MySQL athlete_event_results → Kafka-топік
python produce_event_results.py

# 2. Етапи 1–6: основний стрим (працює, доки не натиснете Ctrl+C)
python streaming_pipeline.py

# 3. В окремому терміналі: друк даних з вихідного Kafka-топіку
python consume_output.py
```

### Що відбувається в `streaming_pipeline.py`

| Етап | Що робиться |
|------|-------------|
| 1 | Читання `olympic_dataset.athlete_bio` з MySQL через JDBC |
| 2 | Фільтр: зріст і вага не порожні та є числами (`cast("double")` + `isNotNull`) |
| 3 | Читання стріму з Kafka-топіку `athlete_event_results`, JSON → колонки (`from_json`) |
| 4 | Join стріму зі статичною таблицею біо за `athlete_id` |
| 5 | `groupBy(sport, medal, sex, country_noc)` → `avg_height`, `avg_weight` + `timestamp` |
| 6 | `foreachBatch`: а) запис у вихідний Kafka-топік, b) запис у MySQL |

Кожен етап позначено коментарем у коді (`# Етап 6.а): ...`).

Агрегація без watermark, тому стрим працює в режимі `complete`: кожен батч містить повну актуальну таблицю середніх, а в MySQL вона дописується з новим `timestamp`.

Для перезапуску з нуля видаліть папку `checkpoint/`.

### Перевірка в MySQL

```sql
SELECT * FROM yourname_athlete_enriched_avg ORDER BY timestamp DESC LIMIT 20;
```

---

## Частина 2. Batch Data Lake

### Локальний запуск (без Airflow)

Скрипти пишуть у відносні папки `landing/`, `bronze/`, `silver/`, `gold/`, тому запускайте їх з однієї директорії і строго по черзі:

```bash
cd part2_batch
python landing_to_bronze.py   # 1. завантаження CSV з FTP → bronze (parquet)
python bronze_to_silver.py    # 2. чистка тексту, дедублікація → silver
python silver_to_gold.py      # 3. join + середні значення → gold/avg_stats
```

### Запуск через Airflow

1. Скопіюйте всі 4 файли з `part2_batch/` в одну підпапку директорії `dags/` вашого Airflow (наприклад, `dags/yourname_fp/`).
2. Переконайтеся, що встановлено `apache-airflow-providers-apache-spark` і в Airflow є Spark-конекшен `spark-default` (Admin → Connections). Якщо у вас інший ID — змініть `SPARK_CONN_ID` у `project_solution.py`.
3. За потреби змініть `dag_id` на унікальний (наприклад, з вашим ім'ям).
4. В Airflow UI знайдіть DAG `goit_de_fp_batch_datalake`, увімкніть і запустіть (▶ Trigger DAG).
5. Таски виконуються послідовно: `landing_to_bronze → bronze_to_silver → silver_to_gold`.

### Скриншоти для здачі

- Для кожної таски: клікніть на квадратик task-run → **Logs** → прокрутіть до виводу `df.show()` (має бути видно часову мітку зліва).
- Граф відпрацьованого DAG: вкладка **Graph**.

---

## Здача

1. Створіть **публічний** репозиторій `goit-de-fp`.
2. Перевірте, що `.env` **не** потрапив у коміт (`git status`): він у `.gitignore`.
3. Додайте скриншоти в `screenshots/` (див. `screenshots/README.md`).

```bash
git init
git add .
git commit -m "Final project: streaming pipeline + batch data lake"
git branch -M main
git remote add origin https://github.com/<your-username>/goit-de-fp.git
git push -u origin main
```
