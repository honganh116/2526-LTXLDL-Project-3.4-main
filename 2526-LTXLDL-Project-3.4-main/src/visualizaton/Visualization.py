import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np
import matplotlib.dates as mdates

# Thư viện Windrose
try:
    from windrose import WindroseAxes
except ImportError:
    print("CẢNH BÁO: Chưa cài thư viện 'windrose'.")
    WindroseAxes = None

# --- CẤU HÌNH PHONG CÁCH ĐỒNG NHẤT ---
# Sử dụng bảng màu 'YlGnBu': Giá trị thấp là màu Vàng/Xanh nhạt, giá trị cao là Xanh đậm.
MAU_CHU_DAO = "YlGnBu" 
MAU_DUONG_LINE = "#081d58" # Xanh đậm nhất trong bảng YlGnBu
MAU_COT_BIEU_DO = "#41b6c4" # Xanh trung tính

def visualization_fun():
    print("--- Bắt đầu Mục 4: Trực quan hoá (Đồng nhất màu sắc & Tiếng Việt) ---")

    FIGURES_DIR = 'figures'
    if not os.path.exists(FIGURES_DIR):
        os.makedirs(FIGURES_DIR)

    # Thiết lập font và style
    sns.set_theme(style="whitegrid")
    plt.rcParams['axes.unicode_minus'] = False 

    # Thông tin file
    LAT, LON, YEAR = "10.823", "106.6296", "2024"

    # --- 1. TẢI DỮ LIỆU ---
    try:
        daily_file = f'processed/daily_weather_aqi_{LAT}_{LON}_{YEAR}.csv'
        df_daily = pd.read_csv(daily_file, parse_dates=['time'])
        if df_daily['time'].dt.tz is not None:
            df_daily['time'] = df_daily['time'].dt.tz_localize(None)
        df_daily.set_index('time', inplace=True)

        monthly_file = f'processed/monthly_weather_aqi_{LAT}_{LON}_{YEAR}.csv'
        df_monthly = pd.read_csv(monthly_file, parse_dates=['time'])
        if df_monthly['time'].dt.tz is not None:
            df_monthly['time'] = df_monthly['time'].dt.tz_localize(None)
        df_monthly.set_index('time', inplace=True)
    except Exception as e:
        print(f" LỖI: Không tìm thấy file: {e}")
        return

    # ==============================================================================
    # 1. BIỂU ĐỒ ĐƯỜNG: XU HƯỚNG PM2.5 (HÀNG THÁNG)
    # ==============================================================================
    print("1. Vẽ Biểu đồ đường (PM2.5 & Chỉ số chuẩn hóa)...")
    try:
        fig, ax1 = plt.subplots(figsize=(12, 6))

        sns.lineplot(data=df_monthly, x=df_monthly.index, y='pm2_5_mean', ax=ax1, 
                    marker='o', color=MAU_DUONG_LINE, label='Nồng độ PM2.5 trung bình', zorder=3)
        ax1.set_ylabel('Nồng độ PM2.5 (µg/m³)', color=MAU_DUONG_LINE)
        ax1.tick_params(axis='y', labelcolor=MAU_DUONG_LINE)

        ax2 = ax1.twinx()
        ax2.bar(df_monthly.index, df_monthly['AQI_index_100'], width=20, 
                alpha=0.3, color='gray', label='Chỉ số chuẩn hóa', zorder=1)
        ax2.set_ylabel('Chỉ số chuẩn hóa (Mức 100)', color='gray')
        ax2.axhline(100, color='red', linestyle='--', linewidth=1, label='Ngưỡng trung bình năm')

        plt.title(f'Xu hướng bụi mịn PM2.5 và Chỉ số chuẩn hóa tại TP.HCM ({YEAR})', fontsize=14, fontweight='bold')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%Y'))
        ax1.set_xlabel('Thời gian (Tháng/Năm)')
        
        # Gộp chú thích tiếng Việt
        import matplotlib.patches as mpatches
        gray_patch = mpatches.Patch(color='gray', alpha=0.3, label='Chỉ số 100')
        line_handle, line_label = ax1.get_legend_handles_labels()
        ax1.legend(line_handle + [gray_patch], ['Bụi mịn PM2.5 (µg/m³)', 'Chỉ số chuẩn hóa'], loc='upper left')

        plt.tight_layout()
        plt.savefig(os.path.join(FIGURES_DIR, '1_pm25_xu_huong.png'), dpi=300)
        plt.close()
    except Exception as e: print(f"Lỗi BĐ1: {e}")

    # ==============================================================================
    # 2. BIỂU ĐỒ CỘT: NGÀY MƯA VS NGÀY Ô NHIỄM
    # ==============================================================================
    print("2. Vẽ Biểu đồ cột (Mưa và Ô nhiễm)...")
    try:
        df_plot = df_monthly[['rainy_days_count', 'polluted_days_count']].copy()
        df_plot.index = df_plot.index.strftime('%m/%Y') 

        ax = df_plot.plot(kind='bar', figsize=(12, 6), width=0.8, color=[MAU_COT_BIEU_DO, MAU_DUONG_LINE])
        plt.title('So sánh số ngày mưa và số ngày ô nhiễm không khí', fontsize=14, fontweight='bold')
        plt.ylabel('Số ngày trong tháng')
        plt.xlabel('Tháng/Năm')
        plt.legend(['Số ngày có mưa', 'Số ngày bị ô nhiễm (PM2.5 > 50)'])
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(FIGURES_DIR, '2_monthly_mua_vs_o_nhiem.png'), dpi=300)
        plt.close()
    except Exception as e: print(f"Lỗi BĐ2: {e}")

    # ==============================================================================
    # 3. BẢN ĐỒ NHIỆT: Ô NHIỄM THEO THỨ VÀ THÁNG
    # ==============================================================================
    print("3. Vẽ Bản đồ nhiệt (Màu đậm là nồng độ cao)...")
    try:
        df_daily['thang'] = df_daily.index.month
        # Chuyển tên thứ sang tiếng Việt
        ten_thu = {
            'Monday': 'Thứ 2', 'Tuesday': 'Thứ 3', 'Wednesday': 'Thứ 4',
            'Thursday': 'Thứ 5', 'Friday': 'Thứ 6', 'Saturday': 'Thứ 7', 'Sunday': 'Chủ nhật'
        }
        df_daily['thu'] = df_daily.index.day_name().map(ten_thu)
        thu_tu_thu = ['Thứ 2', 'Thứ 3', 'Thứ 4', 'Thứ 5', 'Thứ 6', 'Thứ 7', 'Chủ nhật']
        
        heatmap_data = df_daily.pivot_table(values='pm2_5_mean', index='thang', columns='thu', aggfunc='mean')
        heatmap_data = heatmap_data.reindex(columns=thu_tu_thu)

        plt.figure(figsize=(10, 8))
        sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap=MAU_CHU_DAO, linewidths=.5, 
                    cbar_kws={'label': 'Nồng độ PM2.5 (µg/m³)'})
        
        plt.title('Bản đồ nhiệt: Nồng độ bụi PM2.5 trung bình theo Thứ và Tháng', fontsize=14, fontweight='bold')
        plt.ylabel('Tháng')
        plt.xlabel('Thứ trong tuần')
        plt.tight_layout()
        plt.savefig(os.path.join(FIGURES_DIR, '3_heatmap_pm25.png'), dpi=300)
        plt.close()
    except Exception as e: print(f"Lỗi BĐ3: {e}")

    # ==============================================================================
    # 4. HOA GIÓ: HƯỚNG GIÓ VÀ TỐC ĐỘ (Màu đậm là gió mạnh)
    # ==============================================================================
    print("4. Vẽ Hoa gió (Đồng nhất màu xanh đậm)...")
    if WindroseAxes:
        try:
            df_wind = df_daily[['wind_direction_mean', 'wind_speed_mean']].dropna()
            wd = df_wind['wind_direction_mean']
            ws = df_wind['wind_speed_mean']

            fig = plt.figure(figsize=(9, 8))
            ax = WindroseAxes(fig, [0.05, 0.1, 0.75, 0.8])
            fig.add_axes(ax)
            
            # Sử dụng cmap đồng nhất, màu đậm ứng với tốc độ cao
            ax.bar(wd, ws, normed=True, opening=0.8, edgecolor='white', cmap=plt.cm.get_cmap(MAU_CHU_DAO))
            
            ax.set_legend(title="Tốc độ gió (m/s)", loc='lower right', bbox_to_anchor=(1.25, 0.1))
            plt.title(f'Biểu đồ Hoa Gió TP.HCM {YEAR}', fontsize=14, fontweight='bold', y=1.08)
            plt.savefig(os.path.join(FIGURES_DIR, '4_hoa_gio.png'), dpi=400)
            plt.close()
        except Exception as e: print(f"Lỗi BĐ4: {e}")

    # ==============================================================================
    # 5. BIỂU ĐỒ PHÂN TÁN: MƯA VÀ BỤI MỊN
    # ==============================================================================
    print("5. Vẽ Biểu đồ phân tán...")
    try:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(data=df_daily, x='precipitation_sum', y='pm2_5_mean', 
                        hue='pm2_5_mean', palette=MAU_CHU_DAO, alpha=0.7, legend=False)
        
        df_rain = df_daily[df_daily['precipitation_sum'] > 0]

        sns.regplot(data=df_rain, x='precipitation_sum', y='pm2_5_mean', 
                    scatter=False, color='red', line_kws={'linestyle':'--'})

        plt.title('Tương quan giữa Lượng mưa và Nồng độ bụi mịn PM2.5', fontsize=14, fontweight='bold')
        plt.xlabel('Tổng lượng mưa trong ngày (mm)')
        plt.ylabel('PM2.5 Trung bình (µg/m³)')
        plt.xlim(-1, 100) 
        plt.tight_layout()
        plt.savefig(os.path.join(FIGURES_DIR, '5_phan_tan_mua_bui.png'), dpi=300)
        plt.close()
    except Exception as e: print(f"Lỗi BĐ5: {e}")

    print(f"\n HOÀN THÀNH! Kiểm tra thư mục '{FIGURES_DIR}' để xem kết quả.")

