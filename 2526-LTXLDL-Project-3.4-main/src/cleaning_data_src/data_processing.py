import pandas as pd
import os
import numpy as np
import json
import math

# ----- 1. KÉO CÁC LUẬT QA VÀO -----
try:
    from src.cleaning_data_src import QA_rules as qa
    print("Thông báo: Đã lôi cổ được ông 'QA_rules.py' vào rồi.")
except ImportError:
    print("TOANG RỒI: Không tìm thấy file 'QA_rules.py'. Kiểm tra lại đường dẫn đi bạn ơi.")
    exit()


# --- 2. CÁC HÀM HỖ TRỢ (HELPER FUNCTIONS) ---
RAW_DIR = 'raw'
METEOSTAT_FILE_PATH = os.path.join(RAW_DIR, 'meteostat_hcm_2024.csv')
OPENMETEO_FILE_PATH = os.path.join(RAW_DIR, 'openmeteo_hcm_2024.csv')

def load_data():
    """Load dữ liệu và ép về múi giờ Việt Nam."""
    print(f"\nĐang đọc dữ liệu thời tiết từ: {METEOSTAT_FILE_PATH}")
    print(f"Đang đọc dữ liệu không khí từ: {OPENMETEO_FILE_PATH}")
    
    try:
        # 1. Đọc weather
        df_weather = pd.read_csv(METEOSTAT_FILE_PATH)
        time_col_w = 'time' if 'time' in df_weather.columns else 'date'
        df_weather[time_col_w] = pd.to_datetime(df_weather[time_col_w])
        df_weather.set_index(time_col_w, inplace=True)

        # 2. Đọc air
        df_air = pd.read_csv(OPENMETEO_FILE_PATH)
        time_col_a = 'time' if 'time' in df_air.columns else 'date'
        df_air[time_col_a] = pd.to_datetime(df_air[time_col_a])
        df_air.set_index(time_col_a, inplace=True)
        
        print("Đang đồng bộ múi giờ sang 'Asia/Ho_Chi_Minh'...")
        
        def localize_tz(df):
            if df.index.tz is None:
                return df.tz_localize('UTC').tz_convert('Asia/Ho_Chi_Minh')
            else:
                return df.tz_convert('Asia/Ho_Chi_Minh')

        df_weather = localize_tz(df_weather)
        df_air = localize_tz(df_air)

        return df_weather, df_air
        
    except Exception as e:
        print(f"\nLỖI khi tải dữ liệu: {e}")
        return None, None

def calculate_vector_mean_wind_direction(degrees):
    """Tính hướng gió bằng Vector (sin/cos)."""
    if len(degrees) == 0:
        return np.nan
    rads = np.deg2rad(degrees)
    sin_mean = np.mean(np.sin(rads))
    cos_mean = np.mean(np.cos(rads))
    mean_rad = np.arctan2(sin_mean, cos_mean)
    mean_deg = np.rad2deg(mean_rad)
    if mean_deg < 0:
        mean_deg += 360
    return mean_deg

# [QUAN TRỌNG] Hàm mới để gộp cờ khi resample
def merge_flags(series):
    """Gộp các list cờ lại thành một list duy nhất không trùng lặp."""
    combined = []
    for flags in series:
        if isinstance(flags, list):
            combined.extend(flags)
        elif isinstance(flags, str) and flags:
            combined.append(flags)
    return list(sorted(set(combined)))

def run_general_rules(df, numeric_cols, report_name):
    df_flagged = df.copy()
    if 'qa_flags' not in df_flagged.columns:
        df_flagged['qa_flags'] = [[] for _ in range(len(df_flagged))]

    summary_report = {}
    rules_to_run = {
        'check_numeric_types': (qa.check_numeric_types, [numeric_cols]),
        'check_missing_values': (qa.check_missing_values, [numeric_cols]),
        'check_g_duplicated_timestamp': (qa.check_g_duplicated_timestamp, []),
        'check_g_invalid_timezone': (qa.check_g_invalid_timezone, []),
        'check_g_missing_hours_2024': (qa.check_g_missing_hours_2024, [])
    }
    
    print(f"Đang chạy bộ test tổng quát ({report_name})...")
    for func_name, (rule_function, args) in rules_to_run.items():
        all_args = [df_flagged] + args
        result = rule_function(*all_args)
        
        rule_id = result['id']
        reason = result['reason']
        failing_indices = result['indices']
        count = len(failing_indices)
        
        summary_report[rule_id] = {
            'description': reason,
            'count': count,
            'percentage': (count / len(df_flagged)) * 100 if len(df_flagged) > 0 else 0
        }
        if count > 0:
            # Fix lỗi cảnh báo SettingWithCopy của Pandas bằng cách gán trực tiếp list
            # Cách an toàn: dùng loop hoặc apply cẩn thận
            for idx in failing_indices:
                current_flags = df_flagged.at[idx, 'qa_flags']
                if rule_id not in current_flags:
                    current_flags.append(rule_id)
                    df_flagged.at[idx, 'qa_flags'] = current_flags

    # Lưu báo cáo JSON QA
    report_file_json = f'reports/qa_summary_{report_name}.json'
    try:
        with open(report_file_json, 'w', encoding='utf-8') as f:
            json.dump(summary_report, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Lỗi ghi file báo cáo: {e}")
        
    return df_flagged, summary_report


# --- 3. CHƯƠNG TRÌNH CHÍNH (PIPELINE) ---
def run_processing_pipeline(LAT, LON, YEAR):
    print("--- Bắt đầu quy trình 'Làm sạch & Tổng hợp' dữ liệu ---")
    
    df_weather, df_air = load_data()
    
    if df_weather is not None and df_air is not None:
        if not os.path.exists('reports'): os.makedirs('reports')
        if not os.path.exists('processed'): os.makedirs('processed')

        # ------------------------------------------------------
        # [BƯỚC 1] SOI LỖI (QA) VÀ GẮN CỜ
        # ------------------------------------------------------
        print("\n[1/5] Bắt đầu soi lỗi (QA)...")
        
        # Weather QA
        df_weather_flagged, _ = qa.apply_qa_rules(df_weather, qa.WEATHER_RULES_SET, "weather_specific")
        weather_cols = ['temp', 'prcp', 'wspd', 'wdir', 'pres']
        df_weather_flagged, _ = run_general_rules(df_weather_flagged, weather_cols, "weather_general")

        # Air QA
        df_air_flagged, _ = qa.apply_qa_rules(df_air, qa.AIR_QUALITY_SET, "air_quality_specific")
        air_cols = ['pm10', 'pm2_5', 'uv_index', 'ozone', 'carbon_monoxide']
        df_air_flagged, _ = run_general_rules(df_air_flagged, air_cols, "air_quality_general")

        # KHỞI TẠO IMPACT REPORT
        impact_report = {
            "initial_state": {
                "weather_rows": len(df_weather_flagged),
                "air_quality_rows": len(df_air_flagged),
                "weather_nan_cells": int(df_weather_flagged.drop(columns='qa_flags', errors='ignore').isna().sum().sum()),
                "air_quality_nan_cells": int(df_air_flagged.drop(columns='qa_flags', errors='ignore').isna().sum().sum())
            },
            "cleaning_actions": {
                "rows_deleted_duplicates": {},
                "cells_nullified_by_qa": {}, 
                "cells_corrected_by_qa": {} 
            },
            "fill_actions": {
                "resampling_effect": {},
                "precipitation_filled_zero": 0,
                "cells_interpolated_linear": {},
            },
            "final_state": {}
        }

        # ------------------------------------------------------
        # [BƯỚC 2] DỌN DẸP (CLEANING)
        # ------------------------------------------------------
        print("\n[2/5] Dọn dẹp lỗi...")
        
        weather_nan_before_clean = impact_report["initial_state"]["weather_nan_cells"]
        air_nan_before_clean = impact_report["initial_state"]["air_quality_nan_cells"]

        df_weather_cleaned = df_weather_flagged.copy()
        df_air_cleaned = df_air_flagged.copy()

        # 1. Xóa trùng lặp
        weather_dupes = df_weather_cleaned['qa_flags'].apply(lambda x: 'GEN-DUP-1' in x)
        air_dupes = df_air_cleaned['qa_flags'].apply(lambda x: 'GEN-DUP-1' in x)
        
        impact_report["cleaning_actions"]["rows_deleted_duplicates"] = {
            "weather": int(weather_dupes.sum()),
            "air_quality": int(air_dupes.sum())
        }
        df_weather_cleaned = df_weather_cleaned[~weather_dupes]
        df_air_cleaned = df_air_cleaned[~air_dupes]

        # 2. Sửa lỗi Specific
        # Weather
        df_weather_cleaned.loc[df_weather_cleaned['qa_flags'].apply(lambda x: 'W-NEG-1' in x), ['prcp', 'wspd']] = np.nan
        df_weather_cleaned.loc[df_weather_cleaned['qa_flags'].apply(lambda x: 'W-BOUND-1' in x), 'temp'] = np.nan
        df_weather_cleaned.loc[df_weather_cleaned['qa_flags'].apply(lambda x: 'W-BOUND-2' in x), 'wdir'] = np.nan
        
        w_logic_1_mask = df_weather_cleaned['qa_flags'].apply(lambda x: 'W-LOGIC-1' in x)
        impact_report["cleaning_actions"]["cells_corrected_by_qa"]["W-LOGIC-1 (wdir=0)"] = int(w_logic_1_mask.sum())
        df_weather_cleaned.loc[w_logic_1_mask, 'wdir'] = 0

        # Air
        df_air_cleaned.loc[df_air_cleaned['qa_flags'].apply(lambda x: 'AQ-NEG-1' in x), air_cols] = np.nan
        df_air_cleaned.loc[df_air_cleaned['qa_flags'].apply(lambda x: 'AQ-LOGIC-1' in x), ['pm10', 'pm2_5']] = np.nan
        
        aq_logic_2_mask = df_air_cleaned['qa_flags'].apply(lambda x: 'AQ-LOGIC-2' in x)
        impact_report["cleaning_actions"]["cells_corrected_by_qa"]["AQ-LOGIC-2 (uv_index=0)"] = int(aq_logic_2_mask.sum())
        df_air_cleaned.loc[aq_logic_2_mask, 'uv_index'] = 0
        
        # Đếm lại NaN
        weather_nan_after_clean = df_weather_cleaned.drop(columns='qa_flags', errors='ignore').isna().sum().sum()
        air_nan_after_clean = df_air_cleaned.drop(columns='qa_flags', errors='ignore').isna().sum().sum()
        
        impact_report["cleaning_actions"]["cells_nullified_by_qa"] = {
            "weather": int(weather_nan_after_clean - weather_nan_before_clean),
            "air_quality": int(air_nan_after_clean - air_nan_before_clean)
        }
        
        print("Dọn dẹp xong. Đã ghi nhận vào báo cáo.")

        # ------------------------------------------------------
        # [BƯỚC 3] GOM DỮ LIỆU: GIỜ -> NGÀY (RESAMPLE)
        # ------------------------------------------------------
        print("\n[3/5] Gom dữ liệu Hourly -> Daily (Vector Wind)...")
        
        impact_report["fill_actions"]["resampling_effect"] = {
            "weather_rows_hourly": len(df_weather_cleaned),
            "air_rows_hourly": len(df_air_cleaned)
        }

        # [SỬA ĐỔI] Thêm 'qa_flags': merge_flags vào quy tắc
        weather_agg_rules = {
            'temp': ['mean', 'median', lambda x: x.quantile(0.95)],
            'prcp': 'sum',
            'wspd': 'mean',
            'wdir': calculate_vector_mean_wind_direction,
            'pres': 'mean',
            'qa_flags': merge_flags # <--- GIỮ LẠI FLAG
        }
        air_agg_rules = {
            'pm10': ['mean', lambda x: x.quantile(0.95)],
            'pm2_5': ['mean', lambda x: x.quantile(0.95)],
            'uv_index': 'max',
            'ozone': 'mean',
            'carbon_monoxide': 'mean',
            'qa_flags': merge_flags 
        }

        daily_weather = df_weather_cleaned.resample('D').agg(weather_agg_rules)
        daily_air = df_air_cleaned.resample('D').agg(air_agg_rules)
        
        impact_report["fill_actions"]["resampling_effect"]["daily_rows"] = len(daily_weather)

        # Làm phẳng tên cột MultiIndex
        daily_weather.columns = ['_'.join(col).strip('_') for col in daily_weather.columns.values]
        daily_air.columns = ['_'.join(col).strip('_') for col in daily_air.columns.values]

        # Rename cột cho chuẩn
        daily_weather = daily_weather.rename(columns={
            'temp_mean': 'temperature_mean',
            'temp_median': 'temperature_p50',
            'temp_<lambda_0>': 'temperature_p95',
            'prcp_sum': 'precipitation_sum',
            'wspd_mean': 'wind_speed_mean',
            'wdir_calculate_vector_mean_wind_direction': 'wind_direction_mean',
            'pres_mean': 'air_pressure',
            'qa_flags_merge_flags': 'qa_flags' 
        })
        daily_air = daily_air.rename(columns={
            'pm10_mean': 'pm10_mean',
            'pm10_<lambda_0>': 'pm10_p95',
            'pm2_5_mean': 'pm2_5_mean',
            'pm2_5_<lambda_0>': 'pm2_5_p95',
            'uv_index_max': 'uv_index_max',
            'ozone_mean': 'ozone_mean',
            'carbon_monoxide_mean': 'carbon_monoxide_mean',
            'qa_flags_merge_flags': 'qa_flags' 
        })

        # --- FILL DỮ LIỆU ---
        # 1. Fill mưa
        precip_nan_before = daily_weather['precipitation_sum'].isna().sum()
        daily_weather['precipitation_sum'] = daily_weather['precipitation_sum'].fillna(0)
        impact_report["fill_actions"]["precipitation_filled_zero"] = int(precip_nan_before)

        # 2. Interpolate các cột số (Trừ qa_flags)
        cols_to_interp_w = [c for c in daily_weather.columns if c != 'qa_flags']
        cols_to_interp_a = [c for c in daily_air.columns if c != 'qa_flags']
        
        nan_before_interp_w = daily_weather[cols_to_interp_w].isna().sum().sum()
        nan_before_interp_a = daily_air[cols_to_interp_a].isna().sum().sum()

        daily_weather[cols_to_interp_w] = daily_weather[cols_to_interp_w].interpolate(method='linear').ffill().bfill()
        daily_air[cols_to_interp_a] = daily_air[cols_to_interp_a].interpolate(method='linear').ffill().bfill()
        
        nan_after_interp_w = daily_weather[cols_to_interp_w].isna().sum().sum()
        nan_after_interp_a = daily_air[cols_to_interp_a].isna().sum().sum()

        impact_report["fill_actions"]["cells_interpolated_linear"] = {
            "weather": int(nan_before_interp_w - nan_after_interp_w),
            "air": int(nan_before_interp_a - nan_after_interp_a)
        }

        # Ghép bảng (Merge) Daily
        df_daily_final = pd.merge(daily_weather, daily_air, left_index=True, right_index=True, how='outer')
        
        # [SỬA ĐỔI] Gộp cờ sau khi merge (xử lý qa_flags_x và qa_flags_y)
        def combine_daily_flags(row):
            flags = []
            if 'qa_flags_x' in row and isinstance(row['qa_flags_x'], list): 
                flags.extend(row['qa_flags_x'])
            if 'qa_flags_y' in row and isinstance(row['qa_flags_y'], list): 
                flags.extend(row['qa_flags_y'])
            # Cũng có thể nó chưa bị đổi tên nếu merge index
            if 'qa_flags' in row and isinstance(row['qa_flags'], list):
                 flags.extend(row['qa_flags'])
            return list(sorted(set(flags)))

        # Pandas merge có thể tạo ra suffixes _x, _y nếu trùng tên
        df_daily_final['qa_flags'] = df_daily_final.apply(combine_daily_flags, axis=1)
        # Xóa các cột thừa sau gộp
        df_daily_final = df_daily_final.drop(columns=['qa_flags_x', 'qa_flags_y'], errors='ignore')

        # ------------------------------------------------------
        # [BƯỚC 4] TẠO BẢNG TUẦN VÀ THÁNG
        # ------------------------------------------------------
        print("\n[4/5] Tính toán Weekly & Monthly...")

        # A. Weekly (Thêm qa_flags vào aggregation)
        weekly_aggs = {
            'air_pressure': ['mean', 'std'],
            'qa_flags': merge_flags  # <--- GIỮ LẠI FLAG
        }
        df_weekly = df_daily_final.resample('W').agg(weekly_aggs)
        
        # Làm phẳng tên cột
        df_weekly.columns = ['_'.join(col).strip('_') for col in df_weekly.columns.values]
        
        # Đổi tên
        df_weekly = df_weekly.rename(columns={
            'air_pressure_mean': 'pressure_mean',
            'air_pressure_std': 'pressure_std',
            'qa_flags_merge_flags': 'qa_flags' 
        })
        df_weekly['pressure_std'] = df_weekly['pressure_std'].fillna(0)

        # B. Monthly (Thêm qa_flags vào aggregation)
        df_daily_final['rainy_day'] = (df_daily_final['precipitation_sum'] >= 1).astype(int)
        df_daily_final['polluted_day'] = (df_daily_final['pm2_5_mean'] > 50).astype(int)

        monthly_aggs = {
            'temperature_mean': 'mean',
            'temperature_p50': 'median',
            'temperature_p95': lambda x: x.quantile(0.95),
            'precipitation_sum': 'sum',
            'rainy_day': 'sum',
            'wind_speed_mean': 'mean',
            'pm2_5_mean': 'mean',
            'polluted_day': 'sum',
            'qa_flags': merge_flags }
        df_monthly = df_daily_final.resample('MS').agg(monthly_aggs)
        df_monthly = df_monthly.rename(columns={
            'precipitation_sum': 'precipitation_total',
            'rainy_day': 'rainy_days_count',
            'polluted_day': 'polluted_days_count',
            'qa_flags_merge_flags': 'qa_flags' # Phòng trường hợp tên bị đổi
        })
        
        # Đổi tên lại nếu cần (do có thể nó ko sinh ra _merge_flags nếu dict đơn giản, nhưng an toàn là trên hết)
        if 'qa_flags' not in df_monthly.columns and 'qa_flags' in monthly_aggs:
             # Tìm cột nào là flag
             pass # Thường pandas giữ nguyên tên nếu không có multi-agg trên 1 cột

        # Index 100
        baseline_pm25 = df_monthly['pm2_5_mean'].mean()
        df_monthly['AQI_index_100'] = (df_monthly['pm2_5_mean'] / baseline_pm25) * 100

        # ------------------------------------------------------
        # [BƯỚC 5] LÀM TRÒN, FORMAT FLAGS & LƯU FILE
        # ------------------------------------------------------
        print("\n[5/5] Ghi báo cáo và xuất file...")

        # 1. Reset Index
        df_daily_final = df_daily_final.reset_index().rename(columns={'index': 'time'})
        df_weekly = df_weekly.reset_index().rename(columns={'time': 'time'}) 
        df_monthly = df_monthly.reset_index().rename(columns={'time': 'time'})

        # 2. Dọn cột tạm
        if 'rainy_day' in df_daily_final.columns:
            df_daily_final.drop(columns=['rainy_day', 'polluted_day'], inplace=True)

        # 3. Ghi Report
        impact_report["final_state"]["total_rows"] = len(df_daily_final)
        impact_report["final_state"]["total_columns"] = len(df_daily_final.columns)
        impact_report["final_state"]["remaining_nan_cells"] = int(df_daily_final.drop(columns=['qa_flags', 'time'], errors='ignore').isna().sum().sum())

        impact_report_path = 'reports/qa_impact_report.json'
        try:
            with open(impact_report_path, 'w', encoding='utf-8') as f:
                json.dump(impact_report, f, ensure_ascii=False, indent=4)
            print(f" -> Đã lưu Báo cáo Tác động: {impact_report_path}")
        except Exception as e:
            print(f"Lỗi lưu report: {e}")

        # 4. Format cột qa_flags thành chuỗi (String)
        def format_flags_to_string(val):
            if isinstance(val, list):
                if len(val) == 0: return ""
                return "; ".join(val)
            return str(val) if val else ""

        if 'qa_flags' in df_daily_final.columns:
            df_daily_final['qa_flags'] = df_daily_final['qa_flags'].apply(format_flags_to_string)
        
        if 'qa_flags' in df_weekly.columns:
            df_weekly['qa_flags'] = df_weekly['qa_flags'].apply(format_flags_to_string)

        if 'qa_flags' in df_monthly.columns:
            df_monthly['qa_flags'] = df_monthly['qa_flags'].apply(format_flags_to_string)

        # 5. Làm tròn số (Chỉ cột số)
        for df in [df_daily_final, df_weekly, df_monthly]:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            df[numeric_cols] = df[numeric_cols].round(2)

        # 6. Lưu CSV 
        path_daily = f'processed/daily_weather_aqi_{LAT}_{LON}_{YEAR}.csv'
        path_weekly = f'processed/weekly_weather_aqi_{LAT}_{LON}_{YEAR}.csv'
        path_monthly = f'processed/monthly_weather_aqi_{LAT}_{LON}_{YEAR}.csv'

        df_daily_final.to_csv(path_daily, index=False)
        df_weekly.to_csv(path_weekly, index=False)
        df_monthly.to_csv(path_monthly, index=False)

        print(f" -> Xong file Ngày: {path_daily}")
        print(f" -> Xong file Tuần: {path_weekly}")
        print(f" -> Xong file Tháng: {path_monthly}")
        
        print("\n--- DONE ---")