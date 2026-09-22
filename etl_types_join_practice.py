"""
Bài thực hành PySpark GỘP: Data Types + Join - dùng CHUNG 1 bộ dữ liệu nguồn
------------------------------------------------------------------------------
Kịch bản: orders_raw.csv là dữ liệu thô từ hệ thống nguồn (tất cả là String,
có lỗi) và customers là bảng khách hàng. Pipeline có 2 chặng dùng CHUNG
1 dataset orders:

  CHẶNG A - DATA TYPES: cast String -> đúng kiểu (Int, Decimal, Date, Timestamp).
            Dòng nào cast lỗi -> reject_type (KHÔNG đi tiếp sang chặng B).
  CHẶNG B - JOIN: lấy các dòng đã cast đúng kiểu, join với customers.
            Dòng nào không tìm được customer -> reject_join.

  Kết quả cuối: orders_clean (đã cast đúng + join thành công),
                orders_rejected_types (lỗi kiểu dữ liệu),
                orders_rejected_join (đúng kiểu nhưng không map được customer).

Đây chính là mô hình 1 pipeline ETL thật: Bronze (raw) -> Silver (typed, validated)
-> Gold (joined, enriched), mỗi bước đều có "hàng rác" (rejected) riêng để điều tra.

Chạy:  python etl_types_join_practice.py
"""
import csv
import os
import shutil

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, StringType, StructField, StructType,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
shutil.rmtree(DATA_DIR, ignore_errors=True)
os.makedirs(DATA_DIR)

spark = (
    SparkSession.builder.appName("pyspark-etl-types-join")
    .master("local[*]")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.ansi.enabled", "false")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("OFF")


def section(title):
    print("\n" + "=" * 90 + f"\n{title}\n" + "=" * 90)


def export_single_csv(df, out_file):
    """Spark mặc định ghi CSV ra THƯ MỤC chứa file part-*.csv.
    Hàm này coalesce(1) rồi đổi tên part-*.csv thành 1 file CSV duy nhất."""
    tmp_dir = out_file + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True) \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(tmp_dir)
    part = [f for f in os.listdir(tmp_dir) if f.startswith("part-") and f.endswith(".csv")][0]
    shutil.move(os.path.join(tmp_dir, part), out_file)
    shutil.rmtree(tmp_dir)


# =============================================================================
# 0. DATASET DÙNG CHUNG: orders_raw (String) + customers
# =============================================================================
section("0. Tạo dataset dùng chung: orders_raw (tất cả là String) + customers")

# customers: 5 khách hàng, id 101-105 (KHÔNG có 999 -> để test lỗi join)
customers_data = [
    (101, "Nguyen Van A", "Ha Noi"),
    (102, "Tran Thi B", "Da Nang"),
    (103, "Le Van C", "TP HCM"),
    (104, "Pham Thi D", "Hai Phong"),
    (105, "Hoang Van E", "Can Tho"),
]
df_customers = spark.createDataFrame(customers_data, "customer_id int, customer_name string, city string")

# orders_raw: 10 dòng, TẤT CẢ dạng String (mô phỏng dữ liệu thô đọc từ CSV/API).
# Mỗi dòng cố ý mang 1 trong các loại lỗi khác nhau (ghi chú bên phải):
orders_raw_rows = [
    ("1",  "101", "500000.00",       "2024-01-15", "2024-01-15 08:30:00", "SUCCESS"),   # OK hoàn toàn
    ("2",  "102", "2,300,000.00",    "15/01/2024", "2024-01-15T09:45:10", "SUCCESS"),   # amount có dấu phẩy, date sai format -> LỖI KIỂU
    ("3",  "999", "150000.00",       "2024-01-16", "2024-01-16 10:00:00", "SUCCESS"),   # kiểu OK, nhưng customer_id=999 không tồn tại -> LỖI JOIN
    ("4",  "",    "990000.00",       "2024-01-17", "2024-01-17 11:00:00", "SUCCESS"),   # kiểu OK, customer_id rỗng -> LỖI JOIN
    ("5",  "103", "abc",             "2024-01-18", "2024-01-18 12:00:00", "SUCCESS"),   # amount không phải số -> LỖI KIỂU
    ("6",  "104", "99999999999.99",  "2024-01-19", "2024-01-19 13:00:00", "SUCCESS"),   # amount tràn Decimal(12,2) -> LỖI KIỂU
    ("7x", "105", "450000.00",       "2024-01-20", "2024-01-20 14:00:00", "SUCCESS"),   # order_id không phải số -> LỖI KIỂU
    ("8",  "101", "300000.00",       "2024-02-30", "2024-02-30 15:00:00", "SUCCESS"),   # ngày 30/02 không tồn tại -> LỖI KIỂU
    ("9",  "102", " 230000.00 ",     "2024-01-21", "2024-01-21 16:00:00", "CANCELLED"), # khoảng trắng thừa, vẫn hợp lệ -> OK
    ("10", "103", "150000.00",       "2024-01-22", "not-a-timestamp",     "SUCCESS"),   # timestamp sai -> LỖI KIỂU
]
raw_schema = StructType([
    StructField("order_id_raw", StringType()),
    StructField("customer_id_raw", StringType()),
    StructField("amount_raw", StringType()),
    StructField("order_date_raw", StringType()),
    StructField("created_at_raw", StringType()),
    StructField("status", StringType()),
])
df_raw = spark.createDataFrame(orders_raw_rows, raw_schema)

raw_csv = os.path.join(DATA_DIR, "orders_raw.csv")
with open(raw_csv, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow([fld.name for fld in raw_schema.fields])
    w.writerows(orders_raw_rows)

print(">> customers:")
df_customers.show()
print(f">> orders_raw (đọc lại từ {os.path.basename(raw_csv)}, tất cả cột là String):")
df_raw = spark.read.option("header", True).schema(raw_schema).csv(raw_csv)
df_raw.printSchema()
df_raw.show(truncate=False)


# =============================================================================
# CHẶNG A - DATA TYPES: cast String -> đúng kiểu, tách reject_type
# =============================================================================
section("CHẶNG A - Data Types: cast String -> Int/Decimal/Date/Timestamp")


def clean_str(col_name):
    """Trim + đổi chuỗi rỗng thành NULL để phân biệt 'thiếu dữ liệu' với 'dữ liệu sai'."""
    c = F.trim(F.col(col_name))
    return F.when(c == "", F.lit(None)).otherwise(c)


df_typed = (
    df_raw
    .withColumn("id_s", clean_str("order_id_raw"))
    .withColumn("cust_s", clean_str("customer_id_raw"))
    .withColumn("amount_s", F.regexp_replace(clean_str("amount_raw"), ",", ""))
    .withColumn("date_s", clean_str("order_date_raw"))
    .withColumn("ts_s", clean_str("created_at_raw"))
    # cast sang kiểu đích
    .withColumn("order_id", F.col("id_s").cast(IntegerType()))
    .withColumn("customer_id", F.col("cust_s").cast(IntegerType()))
    .withColumn("amount", F.col("amount_s").cast(DecimalType(12, 2)))
    .withColumn("order_date", F.to_date("date_s", "yyyy-MM-dd"))
    .withColumn("created_at", F.to_timestamp("ts_s"))
    # cờ lỗi: có giá trị gốc (không NULL) nhưng cast ra NULL => dữ liệu SAI KIỂU
    # (customer_id KHÔNG tính vào đây: customer_id rỗng là chuyện của chặng Join, không phải lỗi kiểu)
    .withColumn("bad_id", F.col("id_s").isNotNull() & F.col("order_id").isNull())
    .withColumn("bad_amount", F.col("amount_s").isNotNull() & F.col("amount").isNull())
    .withColumn("bad_date", F.col("date_s").isNotNull() & F.col("order_date").isNull())
    .withColumn("bad_ts", F.col("ts_s").isNotNull() & F.col("created_at").isNull())
    .withColumn("reject_type_reason", F.concat_ws(",",
        F.when(F.col("bad_id"), "order_id"),
        F.when(F.col("bad_amount"), "amount"),
        F.when(F.col("bad_date"), "order_date"),
        F.when(F.col("bad_ts"), "created_at")))
)

is_bad_type = F.col("reject_type_reason") != ""
df_type_ok = df_typed.filter(~is_bad_type).select(
    "order_id", "customer_id", "amount", "order_date", "created_at", "status")
df_rejected_types = df_typed.filter(is_bad_type).select(
    "order_id_raw", "customer_id_raw", "amount_raw", "order_date_raw", "created_at_raw",
    "status", "reject_type_reason")

print(">> Dòng cast ĐÚNG kiểu (đi tiếp sang chặng Join):")
df_type_ok.show(truncate=False)
print(">> Dòng REJECT vì lỗi kiểu dữ liệu (dừng lại ở đây, không join):")
df_rejected_types.show(truncate=False)
print(f">> {df_raw.count()} dòng raw -> {df_type_ok.count()} đúng kiểu, "
      f"{df_rejected_types.count()} bị reject vì lỗi kiểu")


# =============================================================================
# CHẶNG B - JOIN: lấy df_type_ok join với customers, tách reject_join
# =============================================================================
section("CHẶNG B - Join: ghép df_type_ok (đã đúng kiểu) với customers")

print(">> Inner Join (chỉ để so sánh - sẽ mất order 3 và 4 mà không cảnh báo):")
df_inner = df_type_ok.join(df_customers, on="customer_id", how="inner")
df_inner.select("order_id", "customer_id", "customer_name", "amount", "status").show()
print(f"   {df_type_ok.count()} dòng đúng kiểu -> chỉ còn {df_inner.count()} dòng sau Inner Join")

print(">> Left Join (giữ đủ dòng, lộ rõ chỗ không map được):")
df_left = df_type_ok.join(df_customers, on="customer_id", how="left")
df_left.select("order_id", "customer_id", "customer_name", "city", "amount", "status").show()

df_mapped = df_left.filter(F.col("customer_name").isNotNull()).select(
    "order_id", "customer_id", "customer_name", "city", "amount", "order_date", "created_at", "status")
df_rejected_join = df_left.filter(F.col("customer_name").isNull()) \
    .select("order_id", "customer_id", "amount", "order_date", "created_at", "status") \
    .withColumn("reject_join_reason", F.when(F.col("customer_id").isNull(), "customer_id bị NULL/rỗng")
                .otherwise(F.concat(F.lit("customer_id="), F.col("customer_id").cast("string"),
                                     F.lit(" không tồn tại trong customers"))))

print(">> MAPPED (join thành công - dữ liệu Gold, sẵn sàng dùng):")
df_mapped.show(truncate=False)
print(">> REJECTED_JOIN (đúng kiểu nhưng không map được customer):")
df_rejected_join.show(truncate=False)

assert df_mapped.count() + df_rejected_join.count() == df_type_ok.count(), \
    "mapped + rejected_join phải = tổng dòng đã đúng kiểu!"
print(f">> Kiểm tra: mapped({df_mapped.count()}) + rejected_join({df_rejected_join.count()}) "
      f"= đúng kiểu({df_type_ok.count()}) OK")


# =============================================================================
# TỔNG KẾT PIPELINE
# =============================================================================
section("Tổng kết: orders_raw -> qua 2 chặng lọc -> orders_clean")

n_raw = df_raw.count()
n_type_ok, n_reject_type = df_type_ok.count(), df_rejected_types.count()
n_mapped, n_reject_join = df_mapped.count(), df_rejected_join.count()
print(f"""
  orders_raw ({n_raw} dòng)
      |
      v  CHẶNG A: cast type
      +--> reject_type   : {n_reject_type} dòng  (order_id/amount/date/timestamp sai định dạng)
      |
      v  ({n_type_ok} dòng đúng kiểu)
      |
      v  CHẶNG B: join với customers
      +--> reject_join   : {n_reject_join} dòng  (customer_id NULL hoặc không tồn tại)
      |
      v
  orders_clean ({n_mapped} dòng)  <- dữ liệu sạch, đúng kiểu, đã enrich thông tin khách hàng
""")
print(">> Nhận xét: nếu KHÔNG tách 2 chặng mà chỉ đo 1 con số 'tổng orders hợp lệ',"
      " sẽ không biết được lỗi nằm ở khâu làm sạch dữ liệu (Chặng A) hay khâu join (Chặng B)"
      " -> tách riêng 2 loại reject giúp debug đúng chỗ.")


# =============================================================================
# XUẤT KẾT QUẢ RA CSV
# =============================================================================
section("Xuất kết quả ra CSV")

clean_csv = os.path.join(DATA_DIR, "orders_clean.csv")
rej_type_csv = os.path.join(DATA_DIR, "orders_rejected_types.csv")
rej_join_csv = os.path.join(DATA_DIR, "orders_rejected_join.csv")
export_single_csv(df_mapped, clean_csv)
export_single_csv(df_rejected_types, rej_type_csv)
export_single_csv(df_rejected_join, rej_join_csv)
print(f">> Đã ghi:\n   {clean_csv}\n   {rej_type_csv}\n   {rej_join_csv}")

spark.stop()
