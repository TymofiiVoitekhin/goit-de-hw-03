from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Ініціалізовано Spark-сесію
spark = (
    SparkSession.builder
    .appName("GoIT-DE-HW-03-P3")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Завантажено та очищено початкові дані від пропусків
users_clean = spark.read.csv("data/users.csv", header=True, inferSchema=True).dropna()
purchases_clean = spark.read.csv("data/purchases.csv", header=True, inferSchema=True).dropna()
products_clean = spark.read.csv("data/products.csv", header=True, inferSchema=True).dropna()

# Об'єднано очищені таблиці покупок, користувачів та продуктів за відповідними ключами
# Визначено загальну суму покупок за кожною категорією продуктів
total_by_category_df = (
    purchases_clean
    .join(users_clean, on="user_id", how="inner")
    .join(products_clean, on="product_id", how="inner")
    .withColumn("purchase_amount", F.col("quantity") * F.col("price"))
    .groupBy("category")
    .agg(F.round(F.sum("purchase_amount"), 2).alias("total_spent"))
    .orderBy(F.col("total_spent").desc())
)

# Виведено таблицю із загальною сумою покупок за категоріями
print("=== p3: Визначено загальну суму покупок за кожною категорією продуктів ===")
total_by_category_df.show()

# Завершено роботу Spark-сесії
spark.stop()