import pandas as pd
import json
import os

def _parse_qa_summary(report_data):
    """Helper: parse các file qa_summary...json."""
    records = []
    for id_key, metrics in report_data.items():
        record = {
            'id': id_key,
            'description': metrics.get('description', 'N/A'),
            'count': metrics.get('count', 0),
            'percentage': metrics.get('percentage', 0.0)
        }
        records.append(record)
    return records

def _parse_impact_report(report_data):
    """Helper: parse và 'làm phẳng' file qa_impact_report.json."""
    records = []
    
    def safe_get(data_dict, key_path, default=0):
        """Lấy giá trị lồng nhau (nested dict) an toàn, tránh KeyError."""
        keys = key_path.split('.')
        val = data_dict
        try:
            for key in keys:
                val = val[key]
            return val
        except (KeyError, TypeError):
            return default

    # 1. Stats ban đầu
    records.append({
        'id': 'IMPACT-INIT-WEATHER-ROWS',
        'description': 'Số dòng weather ban đầu',
        'count': safe_get(report_data, 'initial_state.weather_rows'),
        'percentage': None
    })
    records.append({
        'id': 'IMPACT-INIT-AIR-ROWS',
        'description': 'Số dòng air quality ban đầu',
        'count': safe_get(report_data, 'initial_state.air_quality_rows'),
        'percentage': None
    })
    records.append({
        'id': 'IMPACT-INIT-WEATHER-NAN',
        'description': 'Số ô NaN (weather) ban đầu',
        'count': safe_get(report_data, 'initial_state.weather_nan_cells'),
        'percentage': None
    })
    records.append({
        'id': 'IMPACT-INIT-AIR-NAN',
        'description': 'Số ô NaN (air quality) ban đầu',
        'count': safe_get(report_data, 'initial_state.air_quality_nan_cells'),
        'percentage': None
    })

    # 2. Stats hành động clean
    records.append({
        'id': 'IMPACT-CLEAN-DUP-WEATHER',
        'description': 'Số dòng weather trùng lặp đã xóa',
        'count': safe_get(report_data, 'cleaning_actions.rows_deleted_duplicates.weather'),
        'percentage': None
    })
    records.append({
        'id': 'IMPACT-CLEAN-DUP-AIR',
        'description': 'Số dòng air quality trùng lặp đã xóa',
        'count': safe_get(report_data, 'cleaning_actions.rows_deleted_duplicates.air_quality'),
        'percentage': None
    })
    records.append({
        'id': 'IMPACT-CLEAN-NULL-WEATHER',
        'description': 'Số ô weather bị chuyển thành NaN do QA',
        'count': safe_get(report_data, 'cleaning_actions.cells_nullified_by_qa.weather'),
        'percentage': None
    })
    records.append({
        'id': 'IMPACT-CLEAN-NULL-AIR',
        'description': 'Số ô air quality bị chuyển thành NaN do QA',
        'count': safe_get(report_data, 'cleaning_actions.cells_nullified_by_qa.air_quality'),
        'percentage': None
    })
    
    # Ghi lại các hành động sửa lỗi (correct)
    corrected_cells = safe_get(report_data, 'cleaning_actions.cells_corrected_by_qa', {})
    for rule_id, count in corrected_cells.items():
        records.append({
            'id': f'IMPACT-CLEAN-CORRECT-{rule_id}',
            'description': f'Số ô đã sửa (Rule: {rule_id})',
            'count': count,
            'percentage': None
        })

    # 3. Stats hành động fill
    records.append({
        'id': 'IMPACT-FILL-PRECIP-ZERO',
        'description': 'Số ô mưa (precipitation) được fill bằng 0',
        'count': safe_get(report_data, 'fill_actions.precipitation_filled_zero'),
        'percentage': None
    })
    records.append({
        'id': 'IMPACT-FILL-INTERP-WEATHER',
        'description': 'Số ô weather được nội suy (linear)',
        'count': safe_get(report_data, 'fill_actions.cells_interpolated_linear.weather'),
        'percentage': None
    })
    records.append({
        'id': 'IMPACT-FILL-INTERP-AIR',
        'description': 'Số ô air quality được nội suy (linear)',
        'count': safe_get(report_data, 'fill_actions.cells_interpolated_linear.air_quality'),
        'percentage': None
    })

    # 4. Stats cuối cùng
    records.append({
        'id': 'IMPACT-FINAL-ROWS',
        'description': 'Tổng số dòng trong file cuối cùng',
        'count': safe_get(report_data, 'final_state.total_rows'),
        'percentage': None
    })
    records.append({
        'id': 'IMPACT-FINAL-NAN',
        'description': 'Số ô NaN còn lại trong file cuối cùng',
        'count': safe_get(report_data, 'final_state.remaining_nan_cells'),
        'percentage': None
    })

    return records


def generate_qa_report(report_files, output_path='reports/qa_summary.csv'):
    """
    Hợp nhất tất cả file JSON (summary + impact) thành 1 file CSV tổng hợp.
    """
    all_records = []
    
    for file_path in report_files:
        if not os.path.exists(file_path):
            print(f"Cảnh báo: Không tìm thấy tệp báo cáo tại: {file_path}. Bỏ qua.")
            continue
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                report_data = json.load(f)
            
            file_name = os.path.basename(file_path)

            # Phân loại file (summary hay impact) để parse
            if 'qa_summary' in file_name:
                print(f"Đang xử lý (Summary): {file_name}")
                records = _parse_qa_summary(report_data)
                all_records.extend(records)
                
            elif 'qa_impact_report' in file_name:
                print(f"Đang xử lý (Impact): {file_name}")
                records = _parse_impact_report(report_data)
                all_records.extend(records)
                
            else:
                print(f"Cảnh báo: Không nhận dạng được loại tệp: {file_name}. Bỏ qua.")
                
        except json.JSONDecodeError:
            print(f"Lỗi: Không thể giải mã tệp JSON: {file_path}. Bỏ qua.")
        except Exception as e:
            print(f"Lỗi không xác định khi xử lý tệp {file_path}: {e}. Bỏ qua.")

    consolidated_df = pd.DataFrame(all_records)
    
    if consolidated_df.empty:
        print("Lỗi: Không có dữ liệu nào được hợp nhất.")
        return consolidated_df

    # Sắp xếp theo ID cho dễ đọc
    consolidated_df = consolidated_df.sort_values(by='id')

    # Đảm bảo thư mục tồn tại và lưu tệp CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    consolidated_df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"Đã hợp nhất thành công {len(consolidated_df)} bản ghi vào tệp CSV tại: {output_path}")
    return consolidated_df

if __name__ == '__main__':
    # Danh sách các file report cần gộp
    target_reports = [
        'reports/qa_summary_weather_general.json',
        'reports/qa_summary_air_quality_general.json',
        'reports/qa_summary_weather_specific.json',
        'reports/qa_summary_air_quality_specific.json',
        'reports/qa_impact_report.json'
    ]
    
    # Chạy hàm hợp nhất
    consolidated_df = generate_qa_report(target_reports)
    
    if not consolidated_df.empty:
        print("\n--- BẢNG TỔNG HỢP (15 DÒNG ĐẦU) ---")
        print(consolidated_df.head(15))