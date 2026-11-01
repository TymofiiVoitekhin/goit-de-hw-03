from pyspark.sql import SparkSession

# Ініціалізовано Spark-сесію
spark = (
    SparkSession.builder
    .appName("GoIT-DE-HW-03-P2")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Прочитано вхідні CSV-файли
users_df = spark.read.csv("data/users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("data/purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)

# Очищено дані шляхом видалення рядків із пропущеними значеннями (NULL)
users_clean = users_df.dropna()
purchases_clean = purchases_df.dropna()
products_clean = products_df.dropna()

# Підраховано та виведено кількість записів до та після очищення
print("=== p2: Статистика очищення даних ===")
print(f"Таблиця users:     було {users_df.count()} рядків -> залишилось {users_clean.count()} рядків")
print(f"Таблиця purchases: було {purchases_df.count()} рядків -> залишилось {purchases_clean.count()} рядків")
print(f"Таблиця products:  було {products_df.count()} рядків -> залишилось {products_clean.count()} рядків\n")

# Виведено очищені DataFrame
print("=== p2: Очищено таблицю users ===")
users_clean.show(5)

print("=== p2: Очищено таблицю purchases ===")
purchases_clean.show(5)

print("=== p2: Очищено таблицю products ===")
products_clean.show(5)

# Завершено роботу Spark-сесії
spark.stop()