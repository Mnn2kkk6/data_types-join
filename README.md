# PySpark ETL Practice — Data Types + Join

Bài này gộp 2 bài trước (Data Types và Join) thành 1 file, dùng chung 1 bộ dữ liệu.

## Cách chạy

**Bước 1 - Cài PySpark (chỉ làm 1 lần):**
```bash
pip install pyspark
```

**Bước 2 - Vào đúng thư mục chứa file, rồi chạy:**
```bash
cd đường_dẫn_tới_thư_mục_chứa_file
python etl_types_join_practice.py
```

Ví dụ nếu file nằm trong `Downloads`:
```bash
cd C:\Users\ADMIN\Downloads
python etl_types_join_practice.py
```

Chạy xong, script tự tạo thư mục `data/` chứa:

- `orders_raw.csv` — dữ liệu thô ban đầu (đầu vào)
- `orders_clean.csv` — dữ liệu sạch cuối cùng
- `orders_rejected_types.csv` — các dòng bị loại vì sai kiểu dữ liệu
- `orders_rejected_join.csv` — các dòng bị loại vì không tìm được khách hàng

## Bài này làm gì

Dữ liệu đi qua 2 bước lọc liên tiếp:

1. **Bước 1 - Kiểm tra kiểu dữ liệu:** chuyển các cột từ dạng chữ (String) sang đúng kiểu (số, ngày, giờ...). Dòng nào có giá trị sai (ví dụ ngày không tồn tại, số bị lỗi) thì bị loại ra, lưu vào `orders_rejected_types.csv`.
2. **Bước 2 - Ghép với bảng khách hàng (Join):** những dòng đã qua bước 1 sẽ được ghép với bảng `customers`. Dòng nào không tìm được khách hàng tương ứng thì bị loại ra, lưu vào `orders_rejected_join.csv`.

Dòng nào qua được cả 2 bước mới được coi là dữ liệu sạch, nằm trong `orders_clean.csv`.

