# Скриншоти результатів

## Частина 1 — Streaming
- `p1_mysql_output.png` — таблиця з результатами в MySQL (`SELECT * FROM <MYSQL_OUTPUT_TABLE> LIMIT 20;`)
- `p1_kafka_output.png` — вивід `python consume_output.py` (дані з вихідного Kafka-топіку)

## Частина 2 — Batch Data Lake
- `p2_landing_to_bronze.png` — лог таски `landing_to_bronze` з `df.show()` і часовою міткою
- `p2_bronze_to_silver.png` — лог таски `bronze_to_silver` з `df.show()`
- `p2_silver_to_gold.png` — лог таски `silver_to_gold` з `df.show()`
- `p2_dag_graph.png` — граф відпрацьованого DAG
