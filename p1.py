from pyspark.sql import SparkSession

# Ініціалізовано Spark-сесію для локального виконання
spark = (
    SparkSession.builder
    .appName("GoIT-DE-HW-03-P1")
    .master("local[*]")
    .getOrCreate()
)
# Встановлено рівень логування ERROR для очищення консолі від системних повідомлень
spark.sparkContext.setLogLevel("ERROR")

# Завантажено та прочитано кожен CSV-файл як окремий DataFrame з автоматичним визначенням схеми
users_df = spark.read.csv("data/users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("data/purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)

# Виведено перші 10 рядків кожного DataFrame для перевірки коректності зчитування
print("=== p1: Завантажено таблицю users ===")
users_df.show(10)

print("=== p1: Завантажено таблицю purchases ===")
purchases_df.show(10)

print("=== p1: Завантажено таблицю products ===")
products_df.show(10)

# Завершено роботу Spark-сесії
spark.stop()