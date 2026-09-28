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

  CHẶNG C - PERFORMANCE (mới): áp dụng lên chính pipeline trên + 1 bảng orders lớn giả lập:
    C1. Shuffle          : operation nào gây Exchange (join/groupBy) và cái nào không
    C2. repartition/coalesce : khác nhau ở plan, khi nào dùng cái nào
    C3. Broadcast Join   : SortMergeJoin (shuffle 2 phía) vs BroadcastHashJoin (không shuffle bảng lớn)
    C4. Cache/Persist    : đã dùng thật trong Chặng A/B (df_typed, df_left dùng lại nhiều lần)
    C5. Data Skew        : 80% dữ liệu dồn 1 key -> 1 partition "gánh" -> salting để chia nhỏ
    C6. explain()        : cách đọc plan để biết Spark đang làm gì

Chạy:  python etl_types_join_practice.py
"""
import csv
import os
import re
import shutil
import time

from pyspark import StorageLevel
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
    # --- cấu hình phục vụ phần Performance ---
    # Mặc định spark.sql.shuffle.partitions = 200: quá nhiều với dữ liệu nhỏ chạy local
    # (200 task rỗng sau mỗi shuffle). Hạ xuống 8 cho hợp với máy 1 node.
    .config("spark.sql.shuffle.partitions", "8")
    # Tắt AQE trong bài thực hành để plan hiển thị đúng như code viết (dễ học).
    # Production nên BẬT: AQE tự gộp partition nhỏ, đổi sang broadcast join, xử lý skew join.
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("OFF")


def section(title):
    print("\n" + "=" * 90 + f"\n{title}\n" + "=" * 90)


def plan_str(df):
    """Lấy physical plan dạng text (PySpark classic/local)."""
    return df._jdf.queryExecution().executedPlan().toString()


def count_shuffles(df):
    """Đếm số Exchange gây shuffle thật (không tính BroadcastExchange)."""
    return len(re.findall(r"Exchange (?:hashpartitioning|rangepartitioning|RoundRobinPartitioning|SinglePartition)",
                          plan_str(df)))


def join_strategy(df):
    p = plan_str(df)
    for name in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin"):
        if name in p:
            return name
    return "không có join"


def partition_sizes(df):
    """Số dòng trong từng partition -> nhìn ra skew."""
    rows = df.withColumn("pid", F.spark_partition_id()).groupBy("pid").count().orderBy("pid").collect()
    return {r["pid"]: r["count"] for r in rows}


def timed(label, fn):
    t0 = time.time()
    result = fn()
    print(f"   [{label}] {time.time() - t0:.2f}s")
    return result


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

# CACHE (C4): df_typed được dùng lại ≥ 5 lần bên dưới (2 lần filter + nhiều lần count/show).
# Không cache -> mỗi action Spark đọc lại CSV và tính lại toàn bộ phép cast.
# Cache là LAZY: chỉ thực sự lưu vào bộ nhớ ở action đầu tiên.
df_typed = df_typed.persist(StorageLevel.MEMORY_AND_DISK)

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
n_raw = df_raw.count()
n_type_ok, n_reject_type = df_type_ok.count(), df_rejected_types.count()
print(f">> {n_raw} dòng raw -> {n_type_ok} đúng kiểu, {n_reject_type} bị reject vì lỗi kiểu")
assert n_type_ok + n_reject_type == n_raw, "đúng kiểu + reject_type phải = tổng raw (không được mất dòng)!"


# =============================================================================
# CHẶNG B - JOIN: lấy df_type_ok join với customers, tách reject_join
# =============================================================================
section("CHẶNG B - Join: ghép df_type_ok (đã đúng kiểu) với customers")

print(">> Inner Join (chỉ để so sánh - sẽ mất order 3 và 4 mà không cảnh báo):")
df_inner = df_type_ok.join(df_customers, on="customer_id", how="inner")
df_inner.select("order_id", "customer_id", "customer_name", "amount", "status").show()
print(f"   {n_type_ok} dòng đúng kiểu -> chỉ còn {df_inner.count()} dòng sau Inner Join")

print(">> Left Join (giữ đủ dòng, lộ rõ chỗ không map được):")
df_left = df_type_ok.join(df_customers, on="customer_id", how="left").persist(StorageLevel.MEMORY_AND_DISK)
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

n_mapped, n_reject_join = df_mapped.count(), df_rejected_join.count()
assert n_mapped + n_reject_join == n_type_ok, "mapped + rejected_join phải = tổng dòng đã đúng kiểu!"
print(f">> Kiểm tra: mapped({n_mapped}) + rejected_join({n_reject_join}) = đúng kiểu({n_type_ok}) OK")


# =============================================================================
# TỔNG KẾT PIPELINE
# =============================================================================
section("Tổng kết: orders_raw -> qua 2 chặng lọc -> orders_clean")

# (các biến n_* đã tính ở trên, không gọi lại count() -> tránh chạy lại DAG)
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
# CHẶNG C - PERFORMANCE: Shuffle, repartition/coalesce, Broadcast, Skew, explain
# =============================================================================
section("CHẶNG C - Performance: tạo bảng orders lớn giả lập (có skew) để quan sát")

N_BIG = 400_000
# 80% đơn hàng thuộc customer 101 (khách "VIP"), 20% chia cho 102-105 => SKEW nặng.
df_big = (
    spark.range(0, N_BIG, 1, 8)  # 8 partition ban đầu (mặc định local chỉ có 1 -> groupBy sẽ không cần shuffle)
    .withColumn("customer_id",
                F.when(F.col("id") % 10 < 8, F.lit(101)).otherwise(102 + (F.col("id") % 4)).cast("int"))
    .withColumn("amount", (F.rand(seed=42) * 1_000_000).cast(DecimalType(12, 2)))
    .withColumnRenamed("id", "order_id")
)
print(f">> df_big: {N_BIG:,} dòng. Phân bố theo customer_id:")
df_big.groupBy("customer_id").count().orderBy(F.desc("count")).show()


# ---------------------------------------------------------------- C1. SHUFFLE
section("C1. Shuffle - operation nào gây Exchange?")
print("""Shuffle = Spark phải chuyển dữ liệu giữa các executor qua mạng/đĩa để gom các dòng
cùng key về cùng partition. Đây thường là bước ĐẮT NHẤT của job.
  - Narrow (KHÔNG shuffle): select, filter, withColumn, cast, union...
  - Wide   (CÓ shuffle)   : join, groupBy/agg, distinct, orderBy, repartition...""")

narrow = df_big.filter(F.col("amount") > 1000).withColumn("amount_k", F.col("amount") / 1000)
wide_group = df_big.groupBy("customer_id").agg(F.sum("amount").alias("total"))
wide_join = df_big.join(df_customers, "customer_id", "left")
for name, d in [("filter + withColumn (narrow)", narrow),
                ("groupBy + sum (wide)", wide_group),
                ("join với customers (wide)", wide_join)]:
    print(f"   {name:<32} -> số Exchange (shuffle): {count_shuffles(d)}")

print("\n>> Plan của groupBy (tìm dòng 'Exchange hashpartitioning'):")
wide_group.explain()
print("""Ghi nhớ: groupBy sinh 2 bước HashAggregate (partial -> shuffle -> final).
Partial aggregate chạy TRƯỚC shuffle nên dữ liệu gửi đi đã được thu nhỏ: đó là lý do
agg(sum/count) rẻ hơn nhiều so với groupBy rồi mới xử lý từng nhóm.""")


# ------------------------------------------------- C2. REPARTITION vs COALESCE
section("C2. repartition vs coalesce")
print(f">> df_big hiện có {df_big.rdd.getNumPartitions()} partition")

df_rep = df_big.repartition(8)
df_rep_key = df_big.repartition(4, "customer_id")
df_coal = df_rep.coalesce(2)
print(f"   repartition(8)                 -> {df_rep.rdd.getNumPartitions()} partition, shuffle: {count_shuffles(df_rep)}")
print(f"   repartition(4, 'customer_id')  -> {df_rep_key.rdd.getNumPartitions()} partition, shuffle: {count_shuffles(df_rep_key)}")
print(f"   repartition(8).coalesce(2)     -> {df_coal.rdd.getNumPartitions()} partition")

print("\n>> Plan coalesce trực tiếp trên df_big (chú ý: KHÔNG có Exchange mới cho coalesce):")
df_big.coalesce(2).explain()
print("""So sánh:
  repartition(n) : FULL SHUFFLE, chia đều được, TĂNG hoặc GIẢM số partition.
                   Dùng khi: cần tăng song song, sửa partition lệch, hoặc chia theo cột
                   (repartition(n, 'col')) trước join/groupBy/ghi file partitionBy.
  coalesce(n)    : KHÔNG shuffle, chỉ gộp partition cạnh nhau -> chỉ GIẢM được,
                   partition có thể không đều. Dùng khi: giảm số file nhỏ trước khi ghi.
  Lưu ý: export_single_csv() ở dưới dùng coalesce(1) -> toàn bộ dữ liệu ghi bởi 1 task.
         OK với bài nhỏ này, nhưng với dữ liệu lớn sẽ là nút thắt (chậm/OOM).""")


# --------------------------------------------------------- C3. BROADCAST JOIN
section("C3. Broadcast Join - bảng lớn join bảng nhỏ")
# Tắt auto-broadcast để thấy rõ chiến lược mặc định khi Spark KHÔNG biết bảng nhỏ
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
j_smj = df_big.join(df_customers, "customer_id", "left")
j_bhj = df_big.join(F.broadcast(df_customers), "customer_id", "left")
print(f"   Không hint          : {join_strategy(j_smj):<18} | shuffle: {count_shuffles(j_smj)} (cả 2 bảng bị shuffle theo customer_id)")
print(f"   F.broadcast(nhỏ)    : {join_strategy(j_bhj):<18} | shuffle: {count_shuffles(j_bhj)} (bảng lớn KHÔNG bị shuffle)")

print("\n>> Plan BroadcastHashJoin (tìm BroadcastExchange):")
j_bhj.explain()

res_smj = timed("SortMergeJoin   ", lambda: j_smj.groupBy("city").count().collect())
res_bhj = timed("BroadcastHashJoin", lambda: j_bhj.groupBy("city").count().collect())
assert sorted(map(tuple, res_smj), key=str) == sorted(map(tuple, res_bhj), key=str), "2 cách join phải cho cùng kết quả"
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))  # trả về mặc định 10MB
print("""Ghi nhớ:
  - Broadcast: gửi nguyên bảng nhỏ tới MỌI executor -> bảng lớn join tại chỗ, không shuffle.
  - Chỉ nên broadcast bảng thực sự nhỏ (ngưỡng mặc định 10MB); quá lớn -> OOM driver/executor.
  - Left join chỉ broadcast được bảng BÊN PHẢI (bảng bị giữ lại toàn bộ không broadcast được).
  - Bảng customers ở bài này (5 dòng) là ứng viên broadcast điển hình (dimension table).""")


# ------------------------------------------------------------- C4. CACHE/PERSIST
section("C4. Cache / Persist - DataFrame dùng lại nhiều lần")
print(f">> df_typed đã persist ở Chặng A: storageLevel = {df_typed.storageLevel}")
print(f">> df_left  đã persist ở Chặng B: storageLevel = {df_left.storageLevel}")
print("\n>> Plan của df_type_ok: thấy InMemoryTableScan = đọc từ cache thay vì tính lại từ CSV:")
df_type_ok.explain()

df_expensive = df_big.groupBy("customer_id").agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
print(">> Demo trên df_big: cùng 1 DataFrame tính toán nặng, gọi 3 action liên tiếp")
timed("KHÔNG cache, 3 action", lambda: [df_expensive.count(), df_expensive.count(), df_expensive.count()])
df_expensive.cache()
df_expensive.count()  # action đầu tiên: nạp vào cache
timed("CÓ cache,   3 action", lambda: [df_expensive.count(), df_expensive.count(), df_expensive.count()])
df_expensive.unpersist()
print("""Ghi nhớ:
  - cache() = persist(MEMORY_AND_DISK) cho DataFrame. Cache là LAZY, cần 1 action để nạp.
  - Chỉ cache khi DataFrame được DÙNG LẠI (≥2 action/nhánh) và tính toán đủ đắt.
    Cache thứ chỉ dùng 1 lần chỉ tốn RAM.
  - Xong việc nhớ unpersist() để nhả bộ nhớ.""")


# ------------------------------------------------------------- C5. DATA SKEW
section("C5. Data Skew - dữ liệu lệch làm 1 task chạy lâu hơn hẳn")
sz = partition_sizes(df_big.repartition(4, "customer_id"))
total = sum(sz.values())
print(">> Chia df_big theo customer_id thành 4 partition (như khi shuffle cho groupBy/join):")
for pid, n in sz.items():
    bar = "#" * int(40 * n / total)
    print(f"   partition {pid}: {n:>8,} dòng ({100 * n / total:5.1f}%) {bar}")
print(f"   => partition nặng nhất gấp {max(sz.values()) / (total / len(sz)):.1f}x mức trung bình."
      " Cả stage phải chờ task chậm nhất (straggler).")

# Kỹ thuật SALTING: thêm 'muối' ngẫu nhiên để chẻ key nóng thành nhiều key con
SALT = 4
df_salted = df_big.withColumn("salt", (F.rand(seed=7) * SALT).cast("int"))
sz2 = partition_sizes(df_salted.repartition(4, "customer_id", "salt"))
print("\n>> Sau khi thêm salt (chia theo customer_id + salt):")
for pid, n in sz2.items():
    print(f"   partition {pid}: {n:>8,} dòng ({100 * n / total:5.1f}%) " + "#" * int(40 * n / total))
print(f"   => nặng nhất còn {max(sz2.values()) / (total / len(sz2)):.1f}x mức trung bình")

# Aggregate 2 bước với salt: kết quả PHẢI bằng aggregate trực tiếp
direct = df_big.groupBy("customer_id").agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
salted = (
    df_salted.groupBy("customer_id", "salt").agg(F.sum("amount").alias("s"), F.count("*").alias("c"))  # bước 1: key con
    .groupBy("customer_id").agg(F.sum("s").alias("total"), F.sum("c").alias("n"))                        # bước 2: gộp lại
)
a = {r["customer_id"]: (r["total"], r["n"]) for r in direct.collect()}
b = {r["customer_id"]: (r["total"], r["n"]) for r in salted.collect()}
assert a == b, "salting không được làm thay đổi kết quả!"
print("\n>> Kiểm tra: aggregate có salt (2 bước) == aggregate trực tiếp  OK")
salted.orderBy("customer_id").show()
print("""Ghi nhớ:
  - Nhận biết skew: 1 task trong Spark UI (tab Stages) chạy lâu/đọc nhiều dữ liệu hơn hẳn,
    hoặc groupBy(key).count() cho thấy 1 key chiếm tỉ lệ rất lớn (như customer 101 ở đây).
  - Cách xử lý: (1) BẬT AQE + spark.sql.adaptive.skewJoin.enabled (Spark 3+ tự chẻ partition lệch khi join),
    (2) salting như trên cho aggregate, (3) broadcast bảng nhỏ để né shuffle,
    (4) tách riêng key nóng xử lý riêng rồi union lại.
  - NULL / giá trị mặc định (0, -1, '') cũng hay là 'key nóng' vô tình: kiểm tra trước khi join.""")


# ------------------------------------------------------------------ C6. EXPLAIN
section("C6. explain() - đọc plan để tìm chỗ tối ưu")
print(">> explain(mode='formatted') cho bước Left Join của pipeline chính (df_type_ok LEFT JOIN customers):")
df_type_ok.join(df_customers, "customer_id", "left").explain(mode="formatted")
print("""Cách đọc plan:
  1. ĐỌC TỪ DƯỚI LÊN TRÊN (nguồn dữ liệu ở dưới, kết quả cuối ở trên).
  2. Các từ khoá cần tìm:
       Scan / InMemoryTableScan : nguồn đọc (file hay cache)
       Filter / Project         : lọc / chọn cột (nên đẩy xuống gần Scan = predicate/column pruning)
       Exchange ...             : SHUFFLE  -> càng ít càng tốt
       BroadcastExchange        : broadcast bảng nhỏ (tốt)
       SortMergeJoin / BroadcastHashJoin : chiến lược join Spark chọn
       HashAggregate            : groupBy/agg (partial + final)
  3. Các chế độ: explain() = physical | explain(True) = đủ 4 plan (parsed/analyzed/optimized/physical)
                 explain('formatted') = dạng cây gọn + chi tiết từng node | explain('cost') = có thống kê.
  4. Checklist khi job chậm:
       [ ] Có bao nhiêu Exchange? Bỏ được cái nào không (broadcast, lọc sớm, chọn cột sớm)?
       [ ] Join đang là SortMergeJoin dù 1 bên rất nhỏ? -> broadcast
       [ ] Cùng 1 nhánh được tính lại nhiều lần? -> cache
       [ ] Spark UI có task lệch (skew)? -> salting / AQE skew join
       [ ] Quá nhiều partition nhỏ hoặc quá ít partition lớn? -> shuffle.partitions / repartition / coalesce""")

print("\n>> explain(True) của Inner Join đơn giản (để thấy 4 giai đoạn plan):")
df_type_ok.join(df_customers, "customer_id").explain(True)

# giải phóng cache đã dùng trong pipeline
df_typed.unpersist()
df_left.unpersist()

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
