import pandas as pd
import numpy as np
import json
"""
File: QA_rule.py
Mô tả: Thư viện chứa các quy tắc Đảm bảo Chất lượng (QA) 
để kiểm tra dữ liệu thời tiết và chất lượng không khí.
<lưu ý cho cleaning: khi bắt đầu clean hãy đưa cột time làm index
Mỗi hàm quy tắc (check_...) phải trả về một dictionary với 3 khóa:
    - 'id': Mã định danh quy tắc (flag)
    - 'reason': Chuỗi mô tả lý do (human-readable)
    - 'indices': Danh sách các chỉ mục (index) của các dòng vi phạm quy tắc
"""
#=======================================================
# [PHẦN 1 : BỘ QUI TẮC CHUNG <áp dụng cho cả hai file> ]
def check_missing_values(df: pd.DataFrame, cols_to_check: list) -> dict:
    """
    (MISSING-1) Kiểm tra các giá" "trị bị thiếu (NaN) trong các cột cụ thể.
    """
    RULE_ID = "MISSING-1"
    REASON = "Phát hiện giá trị bị thiếu (NaN)."
    
    # .isnull() trả về True nếu là NaN
    # .any(axis=1) trả về True cho bất kỳ dòng nào có ít nhất một NaN
    # trong các cột được chọn (cols_to_check)
    failing_mask = df[cols_to_check].isnull().any(axis=1)
    
    failing_indices = df[failing_mask].index.tolist()
    
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

def check_g_invalid_timezone(df: pd.DataFrame) -> dict:
   """(GEN-TZ-1) Kiểm tra xem tất cả các dòng trong cột time có tuân thủ múi giờ +07:00 hay không."""
   RULE_ID = "GEN-TZ-1"
   REASON = "Không tuân thủ múi giờ +07:00"

   index_as_string = df.index.astype(str)
   failing_indices = df[~index_as_string.str.endswith('+07:00')]
   
   return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

def check_g_duplicated_timestamp(df:pd.DataFrame) -> dict:
   """(GEN-DUP-1) Phát hiện các dòng có cùng giá trị time chính xác."""
   RULE_ID = "GEN-DUP-1"
   REASON = "Mỗi giờ chỉ nên có một bản ghi duy nhất."

   duplicates_mask = df.index.duplicated(keep='first')
   failing_indices = df[duplicates_mask]
   
   return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}


def check_g_missing_hours_2024(df: pd.DataFrame) -> dict:
    """
    (GEN-GAP-1) Kiểm tra xem dữ liệu có đủ 8784 giờ của năm nhuận 2024 hay không.
    Trả về danh sách các giờ (timestamp) bị thiếu.
    """
    RULE_ID = "GEN-GAP-1"
    REASON = "Thiếu dữ liệu giờ (không đủ 8784 giờ cho năm nhuận 2024)"

    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
    except Exception as e:
        return {'id': RULE_ID, 'reason': f"Lỗi chuyển đổi index sang datetime: {e}", 'indices': []}
    start_2024 = pd.Timestamp('2024-01-01 00:00:00', tz='Asia/Bangkok')
    end_2024 = pd.Timestamp('2024-12-31 23:00:00', tz='Asia/Bangkok')
    
    full_2024_index = pd.date_range(start=start_2024, end=end_2024, freq='h')
    df_2024_data_index = df[(df.index.year == 2024)].index# đảm bảo dữ liệu là năm 2024 < cho chắc>

    failing_indices = full_2024_index.difference(df_2024_data_index).tolist()
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}


def check_numeric_types(df: pd.DataFrame, numeric_cols: list) -> dict:
    """
    (DTYPE-1) Kiểm tra các giá trị phi số (text) trong các cột số học.
    """
    RULE_ID = "DTYPE-1"
    REASON = "Phát hiện giá trị phi số trong cột số học."
    
    # Dùng set để tránh trùng lặp chỉ mục (index)
    all_failing_indices = set()
    
    for col in numeric_cols:
        # Nếu cột đã là kiểu số (ví dụ float64), nó đã sạch. Bỏ qua.
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
            
        # Nếu cột là 'object' (string), chúng ta bắt đầu kiểm tra
        
        # 1. Ép kiểu sang số, biến mọi text (như "Error") thành NaN
        coerced_series = pd.to_numeric(df[col], errors='coerce')
        
        # 2. Tìm các giá trị gốc không phải là NaN (ví dụ: "Error", "N/A")
        original_not_nan = df[col].notna()
        
        # 3. Tìm các giá trị sau khi ép kiểu lại là NaN
        coerced_is_nan = coerced_series.isna()
        
        # 4. Lỗi = (Giá trị gốc không phải NaN) VÀ (Giá trị mới là NaN)
        failing_mask = original_not_nan & coerced_is_nan
        
        # Lấy chỉ mục (index) của các dòng lỗi
        failing_indices = df[failing_mask].index
        
        # Thêm các chỉ mục lỗi vào bộ (set) tổng
        all_failing_indices.update(failing_indices)
        
    return {'id': RULE_ID, 'reason': REASON, 'indices': list(all_failing_indices)}
    
#=======================================================
# [PHẦN 2 : BỘ QUI TẮC cho dữ liệu thời tiết <meteostat_hcm_2024.csv> ]

def check_w_negative_values(df: pd.DataFrame) -> dict:
    """
    (W-NEG-1) Kiểm tra các giá trị âm không hợp lệ cho lượng mưa (prcp) 
    hoặc tốc độ gió (wspd).
    """
    RULE_ID = "W-NEG-1"
    REASON = "Giá trị âm không hợp lệ (prcp < 0 hoặc wspd < 0)."
    
    # .query() tự động xử lý NaN (NaN < 0 là False)
    failing_indices = df.query("prcp < 0 or wspd < 0").index
    
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

def check_w_temp_bounds(df: pd.DataFrame, min_t=0, max_t=45) -> dict:
    """
    (W-BOUND-1) Kiểm tra nhiệt độ (temp) có nằm ngoài ngưỡng hợp lý không.
    """
    RULE_ID = "W-BOUND-1"
    REASON = f"Nhiệt độ (temp) ngoài ngưỡng ({min_t}°C - {max_t}°C)."
    
    failing_indices = df.query("temp < @min_t or temp > @max_t").index
    
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

def check_w_wdir_bounds(df: pd.DataFrame) -> dict:
    """
    (W-BOUND-2) Kiểm tra hướng gió (wdir) có nằm ngoài ngưỡng [0, 360] không.
    """
    RULE_ID = "W-BOUND-2"
    REASON = "Hướng gió (wdir) ngoài ngưỡng [0, 360] độ."
    
    # Giá trị 0 và 360 đều hợp lệ
    failing_indices = df.query("wdir < 0 or wdir > 360").index.to_list()
    
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

def check_w_pres_bounds(df: pd.DataFrame, min_p=950, max_p=1050) -> dict:
    """
    (W-BOUND-3) Kiểm tra áp suất (pres) có nằm ngoài ngưỡng hợp lý không.
    """
    RULE_ID = "W-BOUND-3"
    REASON = f"Áp suất (pres) ngoài ngưỡng ({min_p} - {max_p} hPa)."
    
    failing_indices = df.query("pres < @min_p or pres > @max_p").index.tolist()
    
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

def check_w_wind_logic(df: pd.DataFrame) -> dict:
    """
    (W-LOGIC-1) Kiểm tra logic gió: nếu tốc độ = 0, hướng cũng phải = 0.
    """
    RULE_ID = "W-LOGIC-1"
    REASON = "Logic gió không nhất quán (wspd == 0 nhưng wdir != 0)."
    
    # Tìm các dòng vi phạm: tốc độ là 0 VÀ hướng khác 0
    failing_indices = df.query("wspd == 0 and wdir != 0").index.tolist()
    
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

#=======================================================
# [PHẦN 3 : BỘ QUI TẮC cho dữ liệu chất lượng không khí <openmeteo_hcm_2024.csv>]

def check_aq_negative_values(df: pd.DataFrame) -> dict:
    """
    (AQ-NEG-1) Kiểm tra các giá trị âm không hợp lệ cho bất kỳ cột đo lường nào.
    """
    RULE_ID = "AQ-NEG-1"
    REASON = "Giá trị âm không hợp lệ (PM10, PM2.5, UV, Ozone, hoặc CO < 0)."
    
    # Danh sách các cột để kiểm tra
    cols_to_check = ['pm10', 'pm2_5', 'uv_index', 'ozone', 'carbon_monoxide']
    
    # Xây dựng câu query
    query_string = " or ".join([f"`{col}` < 0" for col in cols_to_check])
    
    failing_indices = df.query(query_string).index.tolist()
    
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

def check_aq_pm_logic(df: pd.DataFrame) -> dict:
    """
    (AQ-LOGIC-1) Kiểm tra logic PM2.5 > PM10.
    """
    RULE_ID = "AQ-LOGIC-1"
    REASON = "Logic không nhất quán (PM2.5 > PM10)."
    
    # Chỉ so sánh khi cả hai giá trị đều không phải là NaN
    # (df.query sẽ tự động bỏ qua nếu 1 trong 2 là NaN)
    failing_indices = df.query("pm2_5 > pm10").index.tolist()
    
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

def check_aq_uv_night_logic(df: pd.DataFrame, night_start=19, night_end=5, uv_threshold=0.1) -> dict:
    """
    (AQ-LOGIC-2) Kiểm tra logic UV Index vào ban đêm.
    *** YÊU CẦU: df.index phải là DatetimeIndex ***
    """
    RULE_ID = "AQ-LOGIC-2"
    REASON = f"Logic không nhất quán (UV Index > {uv_threshold} vào ban đêm)."

    # Xác định giờ ban đêm (ví dụ: từ 19h tối đến 5h sáng)
    is_night = (df.index.hour >= night_start) | (df.index.hour <= night_end)
    
    # Xác định UV dương tính (lớn hơn một ngưỡng nhỏ)
    is_uv_positive = df['uv_index'] > uv_threshold
    
    # Kết hợp hai điều kiện
    failing_mask = is_night & is_uv_positive
    
    failing_indices = df[failing_mask].index
    
    return {'id': RULE_ID, 'reason': REASON, 'indices': failing_indices}

#============================================================================
# HÀM ÁP DỤNG RULES.

GENERAL_RULES_SET=[check_numeric_types, 
                   check_g_missing_hours_2024,
                   check_g_duplicated_timestamp,
                   check_g_invalid_timezone,
                   check_missing_values]

WEATHER_RULES_SET=[check_w_negative_values,
                   check_w_pres_bounds,
                   check_w_temp_bounds,
                   check_w_wdir_bounds,
                   check_w_wind_logic]

AIR_QUALITY_SET=[check_aq_negative_values,check_aq_pm_logic,check_aq_uv_night_logic]

def apply_qa_rules(df: pd.DataFrame, rule_set: list,name_rule_set:str):
    """
    Hàm chính để áp dụng một bộ quy tắc QA vào DataFrame.
    
    Hàm này sẽ:
    1. Thêm cột 'qa_flags' vào DataFrame.
    2. Chạy từng quy tắc trong 'rule_set'.
    3. Gắn cờ (flag) vào cột 'qa_flags' cho các dòng vi phạm.
    4. Tạo một báo cáo tóm tắt về số lượng lỗi.
    """

    # Tạo bản sao để tránh thay đổi DataFrame gốc (SettingWithCopyWarning)
    df_flagged = df.copy()
    
    # Khởi tạo cột qa_flags. Dùng kiểu 'object' để chứa list
    df_flagged['qa_flags'] = [[] for _ in range(len(df_flagged))]
    
    # Khởi tạo dictionary báo cáo
    summary_report = {}

    print(f"Bắt đầu chạy {len(rule_set)} quy tắc QA...")

    for rule_function in rule_set:
        result = rule_function(df_flagged)
        
        rule_id = result['id']
        reason = result['reason']
        failing_indices = result['indices']
        
        count = len(failing_indices)
        
        # Cập nhật báo cáo tóm tắt
        summary_report[rule_id] = {
            'description': reason,
            'count': count,
            'percentage': (count / len(df_flagged)) * 100
        }
        
        # Gắn cờ vào các dòng vi phạm
        if count > 0:
            print(f"  > Phát hiện {count} lỗi cho quy tắc: {rule_id}")
            df_flagged.loc[failing_indices, 'qa_flags'] = \
                df_flagged.loc[failing_indices, 'qa_flags'].apply(lambda x: x + [rule_id])
            
        report_file_json = f'reports/qa_summary_{name_rule_set}.json'

        # 3. Mở file và dùng json.dump()
        try:
            with open(report_file_json, 'w', encoding='utf-8') as f:
                json.dump(
                    summary_report, 
                    f,                  
                    ensure_ascii=False, # <-- Rất quan trọng để lưu tiếng Việt
                    indent=4          
                )
            print("Đã lưu báo cáo JSON thành công.")
        except Exception as e:
            print(f"Không thể lưu báo cáo JSON: {e}")
                        
    print("Hoàn tất chạy QA.")
    return df_flagged, summary_report