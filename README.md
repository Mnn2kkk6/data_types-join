# PySpark Data Types & Schema Control

**Mục tiêu:** hiểu data type trong PySpark và vì sao ETL cần kiểm soát schema thay vì để Spark tự đoán.

## Chạy

```bash
pip install pyspark      # cần Java 17+ cho Spark 4.x (Java 8/11/17 cho Spark 3.x)
python data_types_practice.py
```

Script tự tạo thư mục `data/`, chạy lại nhiều lần được:

| File | Nội dung |
|------|----------|
| `data/orders_raw.csv` | dữ liệu thô có lỗi cố ý (đầu vào phần 5) |
| `data/orders_good.csv` | dữ liệu sạch sau khi cast (đầu ra CSV) |
| `data/orders_rejected.csv` | dòng lỗi kèm cột `error_reason` |
| `data/orders_good_parquet/` | dữ liệu sạch dạng Parquet (giữ nguyên schema) |

## Các phần trong bài

| # | Nội dung | Điểm cần quan sát |
|---|----------|-------------------|
| 1 | Float vs Double vs Decimal | Cộng 0.1 một triệu lần: float/double lệch, `decimal(10,2)` đúng 100000.00 |
| 2 | Schema thủ công + Array/Struct/Map | `StructType` khai báo đủ String, Int, Decimal, Date, Timestamp, Boolean, Array, Struct, Map; truy cập `col("shipping.city")`, `col("tags")[0]`, `explode` |
| 3 | Cast String → Decimal/Date/Timestamp | 8 dòng dữ liệu thô có lỗi; cast thẳng vs làm sạch + gắn cờ lỗi + tách `good` / `rejected` |
| 4 | ANSI mode | `CAST('abc' AS INT)` → NULL (ANSI off) hay lỗi (ANSI on); `try_cast` |
| 5 | Đọc CSV: inferSchema vs schema thủ công | `inferSchema` biến cả cột thành String vì 1 giá trị sai; so sánh PERMISSIVE / DROPMALFORMED / FAILFAST |
| 6 | Semi-structured (JSON) | `schema_of_json`, `from_json` + `_corrupt_record`, `explode` mảng lồng, `get_json_object` |

## Dữ liệu sai đã cài (phần 3 và 5)

| order_id | Lỗi cố ý | Cast thẳng cho kết quả |
|----------|----------|------------------------|
| 2 | `"2,300,000.00"` (dấu phẩy), ngày `15/01/2024` | amount = NULL, date = NULL |
| 3 | `"abc"`, `2024-13-45`, `not-a-timestamp` | tất cả NULL |
| 4 | chuỗi rỗng `""` | NULL (thiếu dữ liệu, không phải sai) |
| 5 | NULL thật | NULL |
| 6 | `"12.345"`, ngày `2024-02-30` | amount = **12.35** (làm tròn), date = NULL |
| 7x | id `"7x"`, `99999999999.99` (tràn Decimal(12,2)) | id = NULL, amount = NULL |
| 8 | `" 750.00 "` (khoảng trắng) | cast OK = 750.00 nhưng CSV reader thì coi là lỗi |

## Bài học rút ra cho ETL

1. **Không dùng `inferSchema` cho pipeline thật:** chậm (phải quét dữ liệu), và kết quả thay đổi theo dữ liệu đầu vào (1 giá trị `"abc"` làm cả cột `amount` thành String).
2. **Tiền tệ dùng `DecimalType`, không dùng Double:** tránh sai số cộng dồn. Chọn precision/scale đủ rộng, vì tràn precision trả NULL.
3. **Cast sai không báo lỗi (ANSI off):** giá trị hỏng biến thành NULL, và `12.345` bị làm tròn. Phải tự kiểm tra: *có giá trị gốc nhưng cast ra NULL ⇒ dữ liệu sai*.
4. **Phân biệt "thiếu" và "sai":** chuỗi rỗng/NULL là thiếu; có nội dung mà cast thất bại là sai. Hàm `clean_str` + các cột `bad_*` xử lý việc này.
5. **Tách dữ liệu lỗi ra bảng riêng (quarantine)** thay vì vứt đi hoặc để NULL lẫn vào dữ liệu tốt.
6. **Chọn chế độ đọc phù hợp:** PERMISSIVE + `_corrupt_record` (giữ dòng lỗi để điều tra), DROPMALFORMED (mất dữ liệu), FAILFAST (dừng ngay, hợp với pipeline yêu cầu nghiêm ngặt).
7. **Định dạng ngày:** `cast("date")` chỉ hiểu `yyyy-MM-dd`; các định dạng khác dùng `to_date(col, fmt)`, thử nhiều format bằng `coalesce`.
8. **Parquet giữ schema, CSV thì không:** đọc lại `orders_good.csv` không khai báo schema thì mọi cột thành String.

## Lưu ý

- Script đã chạy thử với PySpark 4.2.0. Một số hành vi khác nhau giữa Spark 3.x và 4.x (ANSI mặc định, cách CSV/`from_json` trả kết quả từng phần khi có lỗi), nên output có thể lệch nhẹ; script đã đặt cấu hình `spark.sql.ansi.enabled=false` và `spark.sql.legacy.timeParserPolicy=CORRECTED` để hành vi ổn định.
- `TimestampType` phụ thuộc timezone session (mặc định là timezone hệ thống); muốn cố định thì đặt `spark.sql.session.timeZone`.
