from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Ініціалізовано Spark-сесію
spark = (
    SparkSession.builder
    .appName("GoIT-DE-HW-03-P6")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Завантажено та очищено вхідні дані
users_clean = spark.read.csv("data/users.csv", header=True, inferSchema=True).dropna()
purchases_clean = spark.read.csv("data/purchases.csv", header=True, inferSchema=True).dropna()
products_clean = spark.read.csv("data/products.csv", header=True, inferSchema=True).dropna()

# Розраховано витрати за категоріями для вікової категорії 18-25 років
age_18_25_by_category_df = (
    purchases_clean
    .join(users_clean, on="user_id", how="inner")
    .join(products_clean, on="product_id", how="inner")
    .filter((F.col("age") >= 18) & (F.col("age") <= 25))
    .withColumn("purchase_amount", F.col("quantity") * F.col("price"))
    .groupBy("category")
    .agg(F.round(F.sum("purchase_amount"), 2).alias("spent_18_25"))
)

# Обчислено відсоткову частку витрат та обрано 3 категорії з найвищим показником
total_window = Window.partitionBy()

top_3_categories_df = (
    age_18_25_by_category_df
    .withColumn("total_spent_18_25", F.sum("spent_18_25").over(total_window))
    .withColumn(
        "share_percent",
        F.round((F.col("spent_18_25") / F.col("total_spent_18_25")) * 100, 2)
    )
    .select("category", "spent_18_25", "share_percent")
    .orderBy(F.col("share_percent").desc())
    .limit(3)
)

# Виведено топ-3 категорії продуктів за часткою витрат
print("=== p6: Обрано 3 категорії продуктів із найвищим відсотком витрат (вік 18-25) ===")
top_3_categories_df.show()

# Завершено роботу Spark-сесії
spark.stop()