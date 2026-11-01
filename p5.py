from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Ініціалізовано Spark-сесію
spark = (
    SparkSession.builder
    .appName("GoIT-DE-HW-03-P5")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Завантажено та очищено початкові таблиці
users_clean = spark.read.csv("data/users.csv", header=True, inferSchema=True).dropna()
purchases_clean = spark.read.csv("data/purchases.csv", header=True, inferSchema=True).dropna()
products_clean = spark.read.csv("data/products.csv", header=True, inferSchema=True).dropna()

# Обчислено суму витрат за кожною категорією для користувачів віком 18-25 років
age_18_25_by_category_df = (
    purchases_clean
    .join(users_clean, on="user_id", how="inner")
    .join(products_clean, on="product_id", how="inner")
    .filter((F.col("age") >= 18) & (F.col("age") <= 25))
    .withColumn("purchase_amount", F.col("quantity") * F.col("price"))
    .groupBy("category")
    .agg(F.round(F.sum("purchase_amount"), 2).alias("spent_18_25"))
)

# Створено віконну специфікацію по всьому датафрейму для отримання загальної суми витрат групи 18-25
total_window = Window.partitionBy()

# Визначено частку покупок за кожною категорією у відсотках (округлено до 2 знаків після коми)
share_18_25_df = (
    age_18_25_by_category_df
    .withColumn("total_spent_18_25", F.sum("spent_18_25").over(total_window))
    .withColumn(
        "share_percent",
        F.round((F.col("spent_18_25") / F.col("total_spent_18_25")) * 100, 2)
    )
    .select("category", "spent_18_25", "share_percent")
    .orderBy(F.col("share_percent").desc())
)

# Виведено таблицю з частками витрат за категоріями
print("=== p5: Визначено частку покупок (%) за кожною категорією для віку 18-25 років ===")
share_18_25_df.show()

# Завершено роботу Spark-сесії
spark.stop()