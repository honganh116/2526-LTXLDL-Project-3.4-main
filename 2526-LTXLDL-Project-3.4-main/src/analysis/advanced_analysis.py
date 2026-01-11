import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score

def run_advanced_analysis(lat, lon, year, processed_dir='processed', figures_dir='figures'):
    print("\n BẮT ĐẦU PHÂN TÍCH NÂNG CAO: DỰ BÁO PM2.5 (TẬP TRUNG TẾT)")
    
    # 1. Load dữ liệu
    file_path = f"{processed_dir}/daily_weather_aqi_{lat}_{lon}_{year}.csv"
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print("Lỗi: Không tìm thấy file dữ liệu.")
        return

    # --- SỬA ĐỔI QUAN TRỌNG: Xử lý thời gian ---
    # Chuyển cột time sang datetime ngay từ đầu để lọc
    df['time'] = pd.to_datetime(df['time'])
    
    # 2. Định nghĩa giai đoạn Tết và giai đoạn "Bình thường"
    # Giả sử Tết 2024: 08/02 - 14/02 (29 Tết đến Mùng 5)
    # Ta lấy rộng ra một chút để vẽ biểu đồ cho đẹp (01/02 - 29/02)
    tet_start_date = '2024-02-08'
    tet_end_date = '2024-02-14'
    
    analysis_start = '2024-02-01'
    analysis_end = '2024-02-29'

    # Tạo mask để lọc ngày Tết
    mask_tet_holiday = (df['time'] >= tet_start_date) & (df['time'] <= tet_end_date)
    
    # 3. Chọn Features và Target
    features = ['precipitation_sum', 'wind_speed_mean', 'temperature_mean', 'air_pressure']
    target = 'pm2_5_mean'
    
    # Lọc bỏ dòng thiếu dữ liệu
    data = df[features + [target, 'time']].dropna()

    # --- SỬA ĐỔI CHIẾN LƯỢC HUẤN LUYỆN ---
    # Train: Dùng TẤT CẢ các ngày KHÔNG PHẢI TẾT trong năm (để học quy luật bình thường)
    # Test/Predict: Dùng giai đoạn tháng 2 (bao gồm Tết) để so sánh
    
    X_train = data.loc[~mask_tet_holiday, features] # Train trên ngày thường
    y_train = data.loc[~mask_tet_holiday, target]
    
    # Tập dữ liệu để vẽ biểu đồ (Tháng 2)
    mask_analysis = (data['time'] >= analysis_start) & (data['time'] <= analysis_end)
    df_analysis = data.loc[mask_analysis].sort_values('time')
    
    X_analysis = df_analysis[features]
    y_actual = df_analysis[target]
    time_analysis = df_analysis['time']

    # 4. Huấn luyện mô hình
    model = LinearRegression()
    model.fit(X_train, y_train)

    # 5. Dự báo lại cho tháng 2 (Bao gồm cả Tết)
    # Đây là giá trị "Nếu không nghỉ Tết thì bụi sẽ là bao nhiêu?"
    y_pred = model.predict(X_analysis)

    # 6. Đánh giá sơ bộ (Chỉ để tham khảo)
    mae = mean_absolute_error(y_actual, y_pred)
    print(f" Kết quả chạy mô hình giả lập:")
    print(f"   - Hệ số tác động của Mưa: {model.coef_[0]:.2f}")
    
    # Tính chênh lệch trung bình trong tuần Tết
    df_analysis['predicted'] = y_pred
    tet_only = df_analysis[(df_analysis['time'] >= tet_start_date) & (df_analysis['time'] <= tet_end_date)]
    diff = tet_only['predicted'].mean() - tet_only[target].mean()
    print(f"   -> Trong tuần Tết, mô hình dự báo cao hơn thực tế trung bình: {diff:.2f} µg/m³")

    # 7. Trực quan hóa kết quả (ZOOM VÀO THÁNG 2)
    plt.figure(figsize=(12, 6))
    
    # Vẽ đường dự báo và thực tế
    plt.plot(time_analysis, y_pred, label='Dự báo (Mô hình Khí tượng)', color="#ff0e0e", linestyle='--', linewidth=2)
    plt.plot(time_analysis, y_actual, label='Thực tế (Quan trắc)', color="#081d58", linewidth=2, marker='o', markersize=4)
    
    # Tô màu nền vùng nghỉ Tết để làm nổi bật
    plt.axvspan(pd.to_datetime(tet_start_date), pd.to_datetime(tet_end_date), 
                color='gray', alpha=0.2, label='Tuần nghỉ Tết Nguyên Đán')

    # Format trục thời gian cho đẹp
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=2))
    
    plt.title(f'Phân tích tác động "Hiệu ứng Tết" đến PM2.5 (Tháng 02/{year})\n(Phần chênh lệch giữa đường cam và xanh là lượng ô nhiễm giảm do con người)')
    plt.ylabel('PM2.5 (µg/m³)')
    plt.xlabel('Thời gian (Ngày/Tháng)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    save_path = f"{figures_dir}/6_advanced_forecast_tet.png"
    plt.savefig(save_path)
    plt.close()
    print(f"Đã lưu biểu đồ phân tích Tết: {save_path}")