# Metadata về các tệp dữ liệu đã xử lý
## 1. Tệp Daily: `daily_weather_aqi_10.823_106.6296_2024.csv`

Tệp này chứa các thống kê chi tiết *trong ngày*, được tính bằng cách tổng hợp (cuộn) 24 giá trị **hàng giờ** thành 1 giá trị **hàng ngày**.

| Tên Cột | Mục tiêu | Công thức (từ dữ liệu giờ) | Đơn vị |
| :--- | :--- | :--- | :--- |
| `time` | Mốc thời gian (ngày) | `resample('D')` | ISO 8601 |
| `temperature_mean` | Nhiệt độ trung bình trong 24 giờ. | `mean(temp_hourly)` | °C |
| `temperature_p50` | Nhiệt độ trung vị (P50) trong 24 giờ. | `median(temp_hourly)` | °C |
| `temperature_p95` | Ngưỡng nhiệt độ cao (P95) trong 24 giờ. | `quantile(temp_hourly, 0.95)` | °C |
| `precipitation_sum` | Tổng lượng mưa tích lũy trong 24 giờ. | `sum(precip_hourly)` | mm |
| `wind_speed_mean` | Tốc độ gió trung bình trong 24 giờ. | `mean(wind_speed_hourly)` | m/s |
| `wind_direction_mean`| Hướng gió trung bình (vector mean) trong 24 giờ. | `Vector Mean (wind_dir_hourly)` | Độ (°) |
| `air_pressure` | Áp suất không khí trung bình trong 24 giờ. | `mean(pressure_hourly)` | hPa |
| `pm10_mean` | Trung bình PM10 trong 24 giờ. | `mean(pm10_hourly)` | µg/m³ |
| `pm10_p95` | Ngưỡng phơi nhiễm PM10 cao (P95) trong 24 giờ. | `quantile(pm10_hourly, 0.95)` | µg/m³ |
| `pm2_5_mean` | Trung bình PM2.5 trong 24 giờ. | `mean(pm2_5_hourly)` | µg/m³ |
| `pm2_5_p95` | Ngưỡng phơi nhiễm PM2.5 cao (P95) trong 24 giờ. | `quantile(pm2_5_hourly, 0.95)` | µg/m³ |
| `uv_index_max` | Chỉ số UV tối đa trong 24 giờ. | `max(uv_index_hourly)` | (Chỉ số) |
| `ozone_mean` | Trung bình Ozone trong 24 giờ. | `mean(ozone_hourly)` | µg/m³ |
| `carbon_monoxide_mean`| Trung bình CO trong 24 giờ. | `mean(co_hourly)` | µg/m³ |
| `qa_flags` | Danh sách cờ QA được gộp trong ngày. | `merge_flags(flags_hourly)` | (List) |

---

## 2. Tệp Weekly: `weekly_weather_aqi_10.823_106.6296_2024.csv`

Tệp này chứa các thống kê được tính bằng cách tổng hợp 7 giá trị **hàng ngày** (từ tệp Daily) thành 1 giá trị **hàng tuần**.

| Tên Cột | Mục tiêu | Công thức (từ dữ liệu ngày) | Đơn vị |
| :--- | :--- | :--- | :--- |
| `time` | Mốc thời gian (ngày bắt đầu tuần) | `resample('W-Mon')` | ISO 8601 |
| `pressure_mean` | Áp suất trung bình trong tuần. | `mean(daily.air_pressure)` | hPa |
| `pressure_std` | Độ lệch chuẩn áp suất trong tuần (đo độ biến động). | `std(daily.air_pressure)` | hPa |

---

## 3. Tệp Monthly: `monthly_weather_aqi_10.823_106.6296_2024.csv`

Tệp này chứa các thống kê được tính bằng cách tổng hợp ~30 giá trị **hàng ngày** (từ tệp Daily) thành 1 giá trị **hàng tháng**.

| Tên Cột | Mục tiêu | Công thức (từ dữ liệu ngày) | Đơn vị |
| :--- | :--- | :--- | :--- |
| `time` | Mốc thời gian (ngày bắt đầu tháng) | `resample('MS')` | ISO 8601 |
| `temperature_monthly_mean`| Nhiệt độ trung bình của tháng. | `mean(daily.temperature_mean)` | °C |
| `temperature_p50_of_daily_means`| Trung vị (P50) của các giá trị nhiệt độ trung bình ngày. | `median(daily.temperature_mean)`| °C |
| `temperature_p95_of_daily_means`| Phân vị 95 (P95) của các giá trị nhiệt độ trung bình ngày. | `quantile(daily.temperature_mean, 0.95)`| °C |
| `precipitation_sum_monthly` | Tổng lượng mưa tích lũy của cả tháng. | `sum(daily.precipitation_sum)` | mm |
| `rainy_day_gt_1mm_sum` | Đếm số ngày trong tháng có mưa >= 1mm. | `sum(if daily.precipitation_sum >= 1)`| ngày |
| `wind_speed_mean_monthly`| Tốc độ gió trung bình của tháng. | `mean(daily.wind_speed_mean)` | m/s |
| `pm2_5_montly_mean` | Nồng độ PM2.5 trung bình của tháng. | `mean(daily.pm2_5_mean)` | µg/m³ |
| `pm25_exceeds_mean_threshold_sum`| Đếm số ngày trong tháng có PM2.5 vượt ngưỡng (ví dụ: >50). | `sum(if daily.pm2_5_mean > 50)` | ngày |
| `pm25_index_100` | Chỉ số chuẩn hóa (so với trung bình năm). | `(monthly_pm2_5 / annual_pm2_5) * 100` | (Index) |