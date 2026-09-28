# PySpark ETL Practice — Data Types + Join + Performance

## Cách chạy

**Bước 1 - Cài PySpark** (cần có Java 17 trở lên):
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

Muốn lưu lại toàn bộ kết quả in ra màn hình để đọc:
```bash
python etl_types_join_practice.py > output.txt
```

Chạy xong, script tự tạo thư mục `data/` chứa:

- `orders_raw.csv` — dữ liệu thô ban đầu (đầu vào)
- `orders_clean.csv` — dữ liệu sạch cuối cùng
- `orders_rejected_types.csv` — các dòng bị loại vì sai kiểu dữ liệu
- `orders_rejected_join.csv` — các dòng bị loại vì không tìm được khách hàng

File `output.txt` trong repo là kết quả in ra màn hình của một lần chạy mẫu.

## Bài này làm gì

Bài gồm 3 chặng dùng chung một bộ dữ liệu.

### Chặng A - Kiểm tra kiểu dữ liệu (Data Types)
Chuyển các cột từ dạng chữ (String) sang đúng kiểu (số nguyên, Decimal, ngày, giờ). Dòng nào có giá trị sai (ngày không tồn tại, số bị lỗi, số quá lớn...) thì bị loại ra, lưu vào `orders_rejected_types.csv` kèm lý do lỗi ở từng cột.

### Chặng B - Ghép với bảng khách hàng (Join)
Những dòng đã qua chặng A được ghép với bảng `customers`. So sánh `INNER JOIN` (âm thầm mất dòng) với `LEFT JOIN` (lộ rõ dòng không map được). Dòng nào không tìm được khách hàng (customer_id rỗng hoặc không tồn tại) thì bị loại ra, lưu vào `orders_rejected_join.csv`.

Dòng nào qua được cả 2 chặng mới được coi là dữ liệu sạch, nằm trong `orders_clean.csv`.

Kết quả với dữ liệu mẫu: 10 dòng raw → 4 dòng đúng kiểu (6 dòng reject vì kiểu) → 2 dòng sạch (2 dòng reject vì join).

### Chặng C - Performance (phần tìm hiểu thêm)
Dùng thêm một bảng `df_big` gồm 400.000 đơn hàng giả lập (80% thuộc một khách hàng để tạo skew) để quan sát:

| Mục | Nội dung |
|---|---|
| C1. Shuffle | Operation nào gây shuffle (join, groupBy) và nào không (filter, withColumn); đếm số `Exchange` trong plan |
| C2. repartition vs coalesce | `repartition` shuffle toàn bộ và tăng/giảm được số partition; `coalesce` không shuffle và chỉ giảm được |
| C3. Broadcast Join | So sánh `SortMergeJoin` (shuffle 2 bảng) với `BroadcastHashJoin` (không shuffle bảng lớn) |
| C4. Cache/Persist | Cache DataFrame dùng lại nhiều lần (`df_typed`, `df_left`), xem `InMemoryTableScan` trong plan, `unpersist()` khi xong |
| C5. Data Skew | Xem phân bố partition bị lệch, xử lý bằng salting (aggregate 2 bước), kiểm tra kết quả không đổi |
| C6. `explain()` | Cách đọc physical plan từ dưới lên, các chế độ `explain()` và checklist khi job chạy chậm |

## Ghi chú

- `spark.sql.shuffle.partitions` được đặt là 8 (mặc định 200 quá nhiều khi chạy local) và AQE được tắt để plan hiển thị đúng như code. Khi chạy production nên bật AQE.
- Thời gian ở các mục so sánh (broadcast, cache) chỉ mang tính minh hoạ, thay đổi theo máy và dữ liệu nhỏ nên chênh lệch có thể không rõ; nên nhìn vào plan (`explain()`) để kết luận.
- Hàm `plan_str()` dùng `df._jdf` nên chỉ chạy với PySpark local/classic, không chạy với Spark Connect.
