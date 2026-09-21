"""
Bài thực hành PySpark: Data Types & Schema Control
--------------------------------------------------
Nội dung:
  1. Float vs Double vs Decimal (sai số số học)
  2. Khai báo schema thủ công + Complex types (Array / Struct / Map)
  3. Cast cột String -> Decimal / Date / Timestamp + dữ liệu sai
  4. ANSI mode: cast sai -> NULL hay báo lỗi?
  5. inferSchema vs schema thủ công khi đọc CSV (PERMISSIVE / DROPMALFORMED / FAILFAST)
  6. Semi-structured data: JSON string -> from_json / explode

Chạy:  python data_types_practice.py
"""
import csv
import os
import shutil
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DecimalType, DoubleType,
    IntegerType, MapType, StringType, StructField, StructType, TimestampType,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
shutil.rmtree(DATA_DIR, ignore_errors=True)
os.makedirs(DATA_DIR)

spark = (
    SparkSession.builder.appName("pyspark-data-types")
    .master("local[*]")
    .config("spark.ui.showConsoleProgress", "false")
    # Tắt ANSI để cast sai trả về NULL (hành vi mặc định của Spark 3.x).
    # Spark 4.x bật ANSI mặc định -> cast sai sẽ báo lỗi. Phần 4 sẽ so sánh 2 chế độ.
    .config("spark.sql.ansi.enabled", "false")
    # Parse date/timestamp sai định dạng -> NULL thay vì ném SparkUpgradeException
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("OFF")  # tắt log lỗi của Spark để output bài tập gọn; lỗi vẫn được bắt bằng try/except
# Lưu ý timezone: TimestampType phụ thuộc timezone của session (mặc định = timezone hệ thống).
# Muốn cố định: .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")


def section(title):
    print("\n" + "=" * 90 + f"\n{title}\n" + "=" * 90)


# =============================================================================
# 1. FLOAT vs DOUBLE vs DECIMAL :
# =============================================================================
section("1. FLOAT vs DOUBLE vs DECIMAL - cộng 0.1 một triệu lần (kết quả đúng = 100000)")

spark.range(1_000_000).select(
    F.sum(F.lit(0.1).cast("float")).alias("sum_float"),          # sai số lớn
    F.sum(F.lit(0.1).cast("double")).alias("sum_double"),        # sai số nhỏ nhưng vẫn có
    F.sum(F.lit(0.1).cast("decimal(10,2)")).alias("sum_decimal"),  # chính xác tuyệt đối
).show(truncate=False)
# => Tiền tệ / số liệu tài chính: LUÔN dùng DecimalType(precision, scale), không dùng Double.
# DecimalType(12, 2): tổng 12 chữ số, trong đó 2 chữ số thập phân -> tối đa 9,999,999,999.99


# =============================================================================
# 2. SCHEMA THỦ CÔNG + COMPLEX TYPES:
# =============================================================================
section("2. Schema thủ công với String, Integer, Decimal, Date, Timestamp, Array, Struct, Map")

orders_schema = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer_name", StringType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("discount_rate", DoubleType(), True),
    StructField("is_paid", BooleanType(), True),
    StructField("order_date", DateType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("tags", ArrayType(StringType()), True),                  # Array
    StructField("shipping", StructType([                                 # Struct
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
    ]), True),
    StructField("attributes", MapType(StringType(), StringType()), True),  # Map
])

orders_rows = [
    (1, "Nguyen Van A", Decimal("1500000.50"), 0.05, True, date(2024, 1, 15),
     datetime(2024, 1, 15, 8, 30, 0), ["vip", "online"], ("Ha Noi", "100000"), {"color": "red", "size": "M"}),
    (2, "Tran Thi B", Decimal("230000.00"), 0.0, False, date(2024, 1, 16),
     datetime(2024, 1, 16, 14, 5, 30), ["new"], ("Da Nang", "550000"), {"color": "blue"}),
    (3, "Le Van C", Decimal("99000.99"), 0.1, True, date(2024, 2, 1),
     datetime(2024, 2, 1, 23, 59, 59), [], ("TP HCM", None), {}),
]

df_orders = spark.createDataFrame(orders_rows, schema=orders_schema)
df_orders.printSchema()
df_orders.show(truncate=False)

print(">> Truy cập Complex types:")
df_orders.select(
    "order_id",
    F.col("shipping.city").alias("city"),               # Struct: dùng dấu chấm
    F.col("tags")[0].alias("first_tag"),                # Array: dùng index
    F.size("tags").alias("n_tags"),
    F.array_contains("tags", "vip").alias("is_vip"),
    F.col("attributes")["color"].alias("color"),        # Map: dùng key
).show()

print(">> explode: 1 dòng Array -> nhiều dòng")
df_orders.select("order_id", F.explode_outer("tags").alias("tag")).show()

print(">> Schema thủ công còn giúp bắt lỗi kiểu dữ liệu ngay khi tạo DataFrame:")
try:
    spark.createDataFrame([("abc", Decimal("1.0"))], "id int, amount decimal(10,2)").show()
except Exception as e:
    print(f"   Lỗi: {type(e).__name__}: {str(e)[:150]}")


# =============================================================================
# 3. CAST String -> đúng datatype + DỮ LIỆU SAI
# =============================================================================
section("3. Cast String -> Decimal / Date / Timestamp với dữ liệu sai")

# Giả lập dữ liệu thô từ CSV / API: TẤT CẢ đều là string
raw_rows = [
    ("1",  "Nguyen Van A", "1500000.50",    "2024-01-15", "2024-01-15 08:30:00"),  # hợp lệ
    ("2",  "Tran Thi B",   "2,300,000.00",  "15/01/2024", "2024-01-15T09:45:10"),  # dấu phẩy, ngày sai format
    ("3",  "Le Van C",     "abc",           "2024-13-45", "not-a-timestamp"),      # sai hoàn toàn
    ("4",  "Pham Thi D",   "",              "",           ""),                     # chuỗi rỗng
    ("5",  "Hoang Van E",  None,            None,         None),                   # NULL thật
    ("6",  "Vu Thi F",     "12.345",        "2024-02-30", "2024-02-30 10:00:00"),  # làm tròn, ngày không tồn tại
    ("7x", "Do Van G",     "99999999999.99", "2024-03-01", "2024-03-01 23:59:59"), # id sai, tràn Decimal(12,2)
    ("8",  "Bui Thi H",    " 750.00 ",      "2024-03-05", "2024-03-05 12:00:00"),  # khoảng trắng thừa
]
raw_schema = StructType([
    StructField("order_id_raw", StringType()),
    StructField("customer_name", StringType()),
    StructField("amount_raw", StringType()),
    StructField("order_date_raw", StringType()),
    StructField("created_at_raw", StringType()),
])
df_raw = spark.createDataFrame(raw_rows, raw_schema)

print(">> 3a. Cast THẲNG (không làm sạch) - quan sát dòng nào bị NULL / làm tròn:")
df_raw.select(
    "order_id_raw", F.col("order_id_raw").cast("int").alias("order_id"),
    "amount_raw", F.col("amount_raw").cast(DecimalType(12, 2)).alias("amount"),
    "order_date_raw", F.col("order_date_raw").cast("date").alias("order_date"),
    "created_at_raw", F.col("created_at_raw").cast("timestamp").alias("created_at"),
).show(truncate=False)
# Ghi chú:
#  - "12.345" -> 12.35 (Decimal(12,2) làm tròn HALF_UP, KHÔNG báo lỗi)
#  - "99999999999.99" -> NULL (tràn precision)
#  - "2,300,000.00", "abc", "" -> NULL
#  - "15/01/2024" -> NULL (cast("date") chỉ hiểu yyyy-MM-dd)


def clean_str(col_name):
    """Trim + đổi chuỗi rỗng thành NULL để phân biệt 'thiếu dữ liệu' với 'dữ liệu sai'."""
    c = F.trim(F.col(col_name))
    return F.when(c == "", F.lit(None)).otherwise(c)


print(">> 3b. Làm sạch + cast đúng cách, gắn cờ lỗi cho từng cột:")
df_clean = (
    df_raw
    .withColumn("id_s", clean_str("order_id_raw"))
    .withColumn("amount_s", F.regexp_replace(clean_str("amount_raw"), ",", ""))  # bỏ dấu phẩy ngăn cách
    .withColumn("date_s", clean_str("order_date_raw"))
    .withColumn("ts_s", clean_str("created_at_raw"))
    # cast sang kiểu đích
    .withColumn("order_id", F.col("id_s").cast("int"))
    .withColumn("amount", F.col("amount_s").cast(DecimalType(12, 2)))
    .withColumn("order_date", F.coalesce(                       # thử nhiều định dạng ngày
        F.to_date("date_s", "yyyy-MM-dd"), F.to_date("date_s", "dd/MM/yyyy")))
    .withColumn("created_at", F.to_timestamp("ts_s"))
    # cờ lỗi: có giá trị gốc (không NULL) nhưng cast ra NULL  => dữ liệu SAI
    .withColumn("bad_id", F.col("id_s").isNotNull() & F.col("order_id").isNull())
    .withColumn("bad_amount", F.col("amount_s").isNotNull() & F.col("amount").isNull())
    .withColumn("bad_date", F.col("date_s").isNotNull() & F.col("order_date").isNull())
    .withColumn("bad_ts", F.col("ts_s").isNotNull() & F.col("created_at").isNull())
    .withColumn("error_reason", F.concat_ws(",",
        F.when(F.col("bad_id"), "order_id"),
        F.when(F.col("bad_amount"), "amount"),
        F.when(F.col("bad_date"), "order_date"),
        F.when(F.col("bad_ts"), "created_at")))
)
df_clean.select("order_id", "customer_name", "amount", "order_date", "created_at", "error_reason").show(truncate=False)

print(">> 3c. Tách dữ liệu tốt / dữ liệu lỗi (quarantine) - mẫu chuẩn trong ETL:")
is_bad = F.col("error_reason") != ""
df_good = df_clean.filter(~is_bad).select("order_id", "customer_name", "amount", "order_date", "created_at")
df_rejected = df_clean.filter(is_bad).select(
    "order_id_raw", "customer_name", "amount_raw", "order_date_raw", "created_at_raw", "error_reason")

print("GOOD:");     df_good.show(truncate=False);     df_good.printSchema()
print("REJECTED:"); df_rejected.show(truncate=False)

# Parquet lưu kèm schema -> đọc lại vẫn đúng kiểu (khác CSV chỉ là text)
good_path = os.path.join(DATA_DIR, "orders_good_parquet")
df_good.write.mode("overwrite").parquet(good_path)
print(">> Đọc lại Parquet - schema được giữ nguyên:")
spark.read.parquet(good_path).printSchema()


def export_single_csv(df, out_file):
    """Spark mặc định ghi CSV ra THƯ MỤC chứa file part-*.csv.
    Hàm này coalesce(1) rồi đổi tên part-*.csv thành 1 file CSV duy nhất."""
    tmp_dir = out_file + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True) \
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(tmp_dir)
    part = [f for f in os.listdir(tmp_dir) if f.startswith("part-") and f.endswith(".csv")][0]
    shutil.move(os.path.join(tmp_dir, part), out_file)
    shutil.rmtree(tmp_dir)


good_csv = os.path.join(DATA_DIR, "orders_good.csv")
rejected_csv = os.path.join(DATA_DIR, "orders_rejected.csv")
export_single_csv(df_good, good_csv)          # dữ liệu sạch, đúng kiểu
export_single_csv(df_rejected, rejected_csv)  # dữ liệu lỗi + error_reason để điều tra
print(f">> Đã ghi CSV:\n   {good_csv}\n   {rejected_csv}")

print(">> Đọc lại CSV (không khai báo schema) - mọi cột quay về String, kiểu dữ liệu KHÔNG được lưu:")
spark.read.option("header", True).csv(good_csv).printSchema()


# =============================================================================
# 4. ANSI MODE: cast sai -> NULL hay báo lỗi?
# =============================================================================
section("4. ANSI mode - NULL im lặng vs báo lỗi vs try_cast")

print(">> ANSI = false: cast sai trả NULL (dễ bỏ sót lỗi!)")
spark.sql("SELECT CAST('abc' AS INT) AS v").show()

spark.conf.set("spark.sql.ansi.enabled", "true")
print(">> ANSI = true: cast sai báo lỗi (fail fast)  [Spark 4.x có thể in thêm 1 dòng log JSON - bỏ qua]")
try:
    spark.sql("SELECT CAST('abc' AS INT) AS v").show()
except Exception as e:
    print(f"   Lỗi: {type(e).__name__}: {str(e)[:160]}")

print(">> ANSI = true + try_cast: chủ động chấp nhận NULL")
spark.sql("SELECT try_cast('abc' AS INT) AS v").show()
spark.conf.set("spark.sql.ansi.enabled", "false")


# =============================================================================
# 5. inferSchema vs SCHEMA THỦ CÔNG khi đọc CSV
# =============================================================================
section("5. Đọc CSV: inferSchema vs schema thủ công")

csv_path = os.path.join(DATA_DIR, "orders_raw.csv")
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["order_id", "customer_name", "amount", "order_date", "created_at"])
    for r in raw_rows:
        w.writerow(["" if v is None else v for v in r])

print(">> 5a. inferSchema=True - 1 giá trị sai làm cả cột thành String:")
df_infer = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
df_infer.printSchema()

csv_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("amount", DecimalType(12, 2)),
    StructField("order_date", DateType()),
    StructField("created_at", TimestampType()),
    StructField("_corrupt_record", StringType()),   # cột giữ nguyên dòng gốc bị lỗi
])


def read_csv(mode):
    return (spark.read.option("header", True).schema(csv_schema)
            .option("mode", mode)
            .option("dateFormat", "yyyy-MM-dd")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .csv(csv_path))


print(">> 5b. Schema thủ công + PERMISSIVE (mặc định): dòng lỗi vào _corrupt_record")
df_perm = read_csv("PERMISSIVE").cache()   # cache bắt buộc khi query cột _corrupt_record
df_perm.show(truncate=False)

print(">> 5c. DROPMALFORMED: âm thầm bỏ dòng lỗi")
read_csv("DROPMALFORMED").drop("_corrupt_record").show(truncate=False)

print(">> 5d. FAILFAST: gặp dòng lỗi là dừng job")
try:
    read_csv("FAILFAST").collect()
except Exception as e:
    print(f"   Lỗi: {type(e).__name__}: {str(e)[:160]}")


# =============================================================================
# 6. SEMI-STRUCTURED DATA (JSON)
# =============================================================================
section("6. Semi-structured: JSON string -> from_json / explode")

json_rows = [
    (1, '{"customer": {"name": "A", "age": 30}, '
        '"items": [{"sku": "P1", "qty": 2, "price": 10.50}, {"sku": "P2", "qty": 1, "price": 99.99}]}'),
    (2, '{"customer": {"name": "B", "age": "thirty"}, "items": []}'),                      # age sai kiểu
    (3, '{"customer": {"name": "C"}, "items": [{"sku": "P3", "qty": 5, "price": 1.00}], '
        '"coupon": "NEW10"}'),                                                             # thiếu age, thừa coupon
    (4, '{broken json'),                                                                   # JSON hỏng
]
df_json = spark.createDataFrame(json_rows, "id int, payload string")

print(">> 6a. schema_of_json: Spark tự suy ra schema từ 1 mẫu (chỉ để tham khảo, đừng dùng cho production)")
sample = json_rows[0][1]
spark.range(1).select(F.schema_of_json(F.lit(sample)).alias("inferred")).show(truncate=False)

json_schema = StructType([
    StructField("customer", StructType([
        StructField("name", StringType()),
        StructField("age", IntegerType()),
    ])),
    StructField("items", ArrayType(StructType([
        StructField("sku", StringType()),
        StructField("qty", IntegerType()),
        StructField("price", DecimalType(10, 2)),
    ]))),
    StructField("_corrupt_record", StringType()),   # giữ nguyên JSON gốc nếu parse lỗi
])

print(">> 6b. from_json với schema thủ công: thiếu field -> NULL, field thừa bị bỏ, JSON sai/hỏng -> vào _corrupt_record")
df_parsed = df_json.withColumn(
    "data", F.from_json("payload", json_schema, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
df_parsed.select(
    "id", "data.customer.name", "data.customer.age",
    F.when(F.col("data.items").isNotNull(), F.size("data.items")).alias("n_items"),
    F.col("data._corrupt_record").isNotNull().alias("parse_failed"),
).show(truncate=False)

print(">> 6c. explode mảng items -> mỗi item 1 dòng, tính line_total = qty * price")
(df_parsed
 .select("id", F.explode_outer("data.items").alias("item"))
 .select("id", "item.sku", "item.qty", "item.price",
         (F.col("item.qty") * F.col("item.price")).alias("line_total"))
 .show(truncate=False))

print(">> 6d. get_json_object: lấy nhanh 1 field bằng JSONPath, không cần schema (kết quả luôn là String)")
df_json.select("id", F.get_json_object("payload", "$.customer.name").alias("customer_name")).show()

spark.stop()
