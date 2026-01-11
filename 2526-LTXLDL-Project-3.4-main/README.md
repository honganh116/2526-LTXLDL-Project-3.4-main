# Weather & Air Quality Analysis Pipeline (HCMC 2024)

Một quy trình xử lý dữ liệu **End-to-End** nhằm phân tích mối liên hệ giữa **thời tiết** và **chất lượng không khí (AQI, PM2.5)** tại **TP.HCM năm 2024**.

---

## 📁 Cấu Trúc Dự Án

```
├── raw/                       # Dữ liệu thô đầu vào
├── processed/                 # Dữ liệu sau xử lý
├── reports/                   # Báo cáo QA + summary
├── figures/                   # Biểu đồ trực quan
│   ├── 1_pm25_timeseries_dual_axis.png
│   ├── 2_monthly_rain_vs_pollution.png
│   ├── 3_heatmap_pm25.png
│   ├── 4_wind_rose.png
│   └── 5_scatter_rain_pm25.png
├── src/                       # Mã nguồn chính
│   ├── cleaning_data_src/
│   │   ├── QA_rules.py
│   │   └── data_processing.py
│   ├── Download_data/
│   │   └── download_raw_data.ipynb
│   ├── QA_summary_gen/
│   │   └── report_generating.py
│   ├── visualization/
│   │   ├── Visualization.py
│   │   └── __init__.py
│   └── runner.ipynb           # Main pipeline nằm trong src
├── requirements.txt
└── README.md
```



---

##  Cài Đặt Môi Trường
Yêu cầu: **Python ≥ 3.8**

### Bước 1: Clone repository
```bash
git clone <repo-url>
cd <repo-folder>
```

### Bước 2: Cài đặt phụ thuộc

```bash
pip install -r requirements.txt
```

> Lưu ý: Thư viện **windrose** là bắt buộc để vẽ biểu đồ *Wind Rose*.

---

##  Hướng Dẫn Chạy Pipeline

### **Bước 0 — Tải dữ liệu thô (khuyến nghị)**

Trước khi chạy pipeline, hãy mở notebook:

```
src/Download_data/download_raw_data.ipynb
```

Trong notebook này, bạn có thể:

* Gọi API để tải dữ liệu thời tiết/không khí mới.
* Xuất dữ liệu vào thư mục `raw/`.

Sau khi hoàn tất, mới chuyển sang Bước 1.

Toàn bộ thao tác chạy pipeline nằm trong notebook **`runner.ipynb`**.

### Bước 1 — Khởi động

Mở notebook bằng Jupyter hoặc VS Code.

### Bước 2 — Thiết lập tham số

```python
LAT = "10.823"
LON = "106.6296"
YEAR = "2024"
```

### Bước 3 — Chạy toàn bộ pipeline xử lý dữ liệu 

Cell này trong `runner.ipynb` đã được viết sẵn để tự động:

* Gọi pipeline xử lý (`run_processing_pipeline()`)
* Điều phối các bước ingestion → QA → cleaning → aggregation → imputation → export
  (Cell 2)**
  Gọi hàm:

```python
run_processing_pipeline()
```

Bên trong thực hiện:

* **Ingestion**: đọc CSV từ `raw/`, chuẩn hóa timezone `Asia/Ho_Chi_Minh`.
* **Quality Check (QA)**: áp dụng từ `QA_rules.py`, gắn cờ lỗi tại `qa_flags`.
* **Xuất báo cáo lỗi** → `reports/qa_summary_name.json` .
* **Cleaning**: xoá trùng lặp, chỉnh lỗi logic (ví dụ: UV ban đêm = 0).
* **Aggregation**:

  * Chuyển từ hourly → daily/weekly/monthly.
  * Tính vector mean cho hướng gió.
* **Imputation**: mưa = 0, các giá trị khác nội suy.
* **Xuất dữ liệu** → thư mục `processed/`.

### **Bước 4 — Tạo báo cáo QA Summary (Cell 4)**

Notebook sẽ gọi:

```
from src.QA_summary_gen.report_generating import generate_qa_report
generate_qa_report()
```

Hàm này:

* Đọc toàn bộ các file QA log
* Hợp nhất
* Xuất báo cáo cuối cùng vào `reports/`

### Bước 5 — Vẽ toàn bộ biểu đồ

Gọi hàm:

```
from src.visualizaton.Visualization import visualization_fun
visualization_fun()
```

Kết quả lưu trong thư mục **`figures/`**:

+ 1_pm25_timeseries_dual_axis.png
+ 2_monthly_rain_vs_pollution.png
+ 3_heatmap_pm25.png
+ 4_wind_rose.png
+ 5_scatter_rain_pm25.png


### Bước 6 — Vẽ biểu đồ nâng cao phân tích tác động của hiệu ứng tết đối với nồng độ P2.5

Gọi hàm:

```
run_advanced_analysis()
```

Kết quả lưu trong thư mục **`figures/`**:
figures\6_advanced_forecast_tet.png

##  Điểm Nhấn Kỹ Thuật

### ✔ Flagging Strategy

Dữ liệu lỗi không bị xoá ngay mà được **đánh cờ (qa_flags)** để truy vết.

### ✔ Vector Mean cho hướng gió

Dùng **u/v components** để tính trung bình vật lý chính xác.

### ✔ Timezone Handling

Xử lý timezone nghiêm ngặt để đảm bảo logic **ngày/đêm**, đặc biệt cho UV.

---

##  Liên Hệ

Nếu gặp lỗi khi tái lập kết quả, vui lòng:

* Tạo Issue trên GitHub.

---
