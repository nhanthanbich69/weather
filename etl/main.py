# ============================================================
# WEATHER DATA AUTO-UPDATER — FULL VIỆT NAM (63 TỈNH)
# - Gom 63 tỉnh vào 1 file: Thời_tiết_Việt_Nam.csv
# - Anti-429 kiểu RATE_LIMIT_HARD:
#     + Retry nhiều lần, chờ lâu
#     + Nếu vẫn 429 -> dừng chương trình, lưu file hiện tại
#     + Lần sau chạy lại: tự crawl tiếp cho từng tỉnh (theo Tinh_thanh)
# ============================================================

import requests, pandas as pd, numpy as np, time, random, os, re
from datetime import datetime, timedelta, date
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from geopy.geocoders import Nominatim

# =============================
# 1. FOLDER LƯU FILE
# =============================
# Use project-relative paths instead of hardcoded absolute paths
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)

# Define data directories
data_lakehouse_dir = os.path.join(project_root, "data", "data lakehouse")
location_dir = os.path.join(project_root, "data", "location")

os.makedirs(data_lakehouse_dir, exist_ok=True)
os.makedirs(location_dir, exist_ok=True)

loc_path = os.path.join(location_dir, "vn_locations.csv")
out_vn_path = os.path.join(data_lakehouse_dir, "data.csv")

# =============================
# 2. DANH SÁCH 63 TỈNH
# =============================
provinces = [
    "An Giang","Bắc Giang","Bắc Kạn","Bạc Liêu","Bắc Ninh","Bến Tre","Bình Định",
    "Bình Dương","Bình Thuận","Bình Phước","Cà Mau","Cần Thơ","Cao Bằng","Đà Nẵng",
    "Đắk Lắk","Đắk Nông","Điện Biên","Đồng Nai","Đồng Tháp","Gia Lai","Hà Giang",
    "Hà Nam","Hà Nội","Hà Tĩnh","Hải Dương","Hải Phòng","Hậu Giang","Hoà Bình",
    "Thừa Thiên Huế","Hưng Yên","Khánh Hoà","Kiên Giang","Kon Tum","Lai Châu",
    "Lâm Đồng","Lạng Sơn","Lào Cai","Long An","Nam Định","Nghệ An","Ninh Bình",
    "Ninh Thuận","Phú Thọ","Phú Yên","Quảng Bình","Quảng Nam","Quảng Ngãi",
    "Quảng Ninh","Quảng Trị","Sóc Trăng","Sơn La","Tây Ninh","Thái Bình",
    "Thái Nguyên","Thanh Hoá","Tiền Giang","Hồ Chí Minh","Trà Vinh",
    "Tuyên Quang","Vĩnh Long","Vĩnh Phúc","Bà Rịa - Vũng Tàu","Yên Bái"
]

# =============================
# 3. TẠO FILE TỌA ĐỘ (CHỈ CHẠY 1 LẦN)
# =============================
if not os.path.exists(loc_path):
    print("Đang tạo file tọa độ (chạy 1 lần)...")
    geolocator = Nominatim(user_agent="vn_weather_locator")
    rows = []

    for p in provinces:
        try:
            loc = geolocator.geocode(f"{p}, Vietnam", timeout=10)
            if loc:
                print(f"  ✔ {p}: {loc.latitude:.4f}, {loc.longitude:.4f}")
                rows.append({
                    "Tinh_thanh": p,
                    "lat": round(loc.latitude, 4),
                    "lon": round(loc.longitude, 4)
                })
            else:
                print(f"  ✖ Không tìm được: {p}")
            time.sleep(random.uniform(1.5, 3.0))
        except Exception as e:
            print(f"  ✖ Lỗi geocode {p}: {e}")
            time.sleep(random.uniform(3.0, 5.0))

    pd.DataFrame(rows).to_csv(loc_path, index=False, encoding="utf-8-sig")
    print(f"✔ Đã lưu file tọa độ: {loc_path}")

# =============================
# 4. LOAD TỌA ĐỘ
# =============================
loc_df = pd.read_csv(loc_path)

# =============================
# 5. MAPPING CỘT HOURLY + DAILY (TIẾNG VIỆT, CÓ DẤU)
# =============================

COLUMN_MAP = {
    "temperature_2m": "Nhiệt độ (°C)",
    "relative_humidity_2m": "Độ ẩm tương đối (%)",
    "dew_point_2m": "Điểm sương (°C)",
    "apparent_temperature": "Nhiệt độ cảm nhận (°C)",
    "pressure_msl": "Áp suất mực biển (hPa)",
    "surface_pressure": "Áp suất bề mặt (hPa)",
    "precipitation": "Lượng mưa (mm)",
    "cloud_cover": "Độ phủ mây (%)",
    "cloud_cover_low": "Mây thấp (%)",
    "cloud_cover_mid": "Mây trung (%)",
    "cloud_cover_high": "Mây cao (%)",
    "wind_speed_10m": "Tốc độ gió 10m (m/s)",
    "wind_speed_100m": "Tốc độ gió 100m (m/s)",
    "wind_direction_10m": "Hướng gió 10m (°)",
    "wind_direction_100m": "Hướng gió 100m (°)",
    "wind_gusts_10m": "Tốc độ gió giật 10m (m/s)",
    "weather_code": "Mã thời tiết",
    "shortwave_radiation": "Bức xạ ngắn (W/m2)",
    "sunshine_duration": "Thời gian nắng (s)",
    "et0_fao_evapotranspiration": "Bốc hơi tham chiếu FAO giờ (mm)",
    "vapour_pressure_deficit": "Thiếu hụt áp suất hơi (kPa)"
}

DAILY_COLUMN_MAP = {
    "weather_code": "Mã thời tiết ngày",
    "temperature_2m_max": "Nhiệt độ tối đa ngày (°C)",
    "temperature_2m_min": "Nhiệt độ tối thiểu ngày (°C)",
    "temperature_2m_mean": "Nhiệt độ trung bình ngày (°C)",
    "apparent_temperature_max": "Nhiệt độ cảm nhận tối đa ngày (°C)",
    "apparent_temperature_min": "Nhiệt độ cảm nhận tối thiểu ngày (°C)",
    "apparent_temperature_mean": "Nhiệt độ cảm nhận trung bình ngày (°C)",
    "precipitation_sum": "Tổng lượng mưa ngày (mm)",
    "precipitation_hours": "Số giờ có mưa (h)",
    "daylight_duration": "Thời lượng ban ngày (s)",
    "sunshine_duration": "Tổng thời gian nắng ngày (s)",
    "shortwave_radiation_sum": "Tổng bức xạ ngắn ngày (W/m2)",
    "wind_speed_10m_max": "Tốc độ gió 10m tối đa ngày (m/s)",
    "wind_gusts_10m_max": "Tốc độ gió giật 10m tối đa ngày (m/s)",
    "wind_direction_10m_dominant": "Hướng gió ưu thế 10m (°)",
    "relative_humidity_2m_mean": "Độ ẩm trung bình ngày (%)",
    "dew_point_2m_mean": "Điểm sương trung bình ngày (°C)",
    "cloud_cover_mean": "Mây trung bình (%)",
    "surface_pressure_mean": "Áp suất bề mặt trung bình (hPa)",
    "et0_fao_evapotranspiration": "Bốc hơi tham chiếu FAO ngày (mm)",
    "sunrise": "Giờ mặt trời mọc",
    "sunset": "Giờ mặt trời lặn"
}

# =============================
# 6. SESSION CHUNG (DÙNG LẠI KẾT NỐI)
# =============================
def make_session():
    session = requests.Session()
    retry_cfg = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_cfg)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

session_global = make_session()

# =============================
# 7. HÀM FETCH 1 BLOCK VỚI RATE_LIMIT_HARD
# =============================
def fetch_block(lat, lon, start_str, end_str, tinh_thanh,
                max_attempts=6, base_wait_429=60):
    """
    Lấy dữ liệu 1 block (tối đa ~365 ngày) cho 1 tỉnh.
    - Gặp 429: chờ lâu + retry nhiều lần.
    - Nếu vẫn 429 sau max_attempts -> raise RuntimeError("RATE_LIMIT_HARD").
    - Gặp lỗi mạng: retry có backoff.
    - Sau merge: Giờ mặt trời mọc / lặn chỉ giữ HH:MM.
    - Sắp xếp cột: Tinh_thanh | Mã thời tiết | Mã thời tiết ngày | ... | Datetime
    """
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_str,
        "end_date": end_str,
        "hourly": ",".join(COLUMN_MAP.keys()),
        "daily": ",".join(DAILY_COLUMN_MAP.keys()),
        "timezone": "Asia/Ho_Chi_Minh",
        "pressure_unit": "hPa",
        "temperature_unit": "celsius",
        "wind_speed_unit": "ms"
    }

    for attempt in range(1, max_attempts + 1):
        try:
            r = session_global.get(url, params=params, timeout=120)
        except Exception as e:
            wait = 10 * attempt
            print(f"    ⚠ Lỗi kết nối (attempt {attempt}/{max_attempts}): {e} – nghỉ {wait}s")
            time.sleep(wait)
            continue

        status = r.status_code

        # === 200 OK ===
        if status == 200:
            data = r.json()

            # HOURLY
            df_h = pd.DataFrame(data["hourly"])
            df_h["Datetime"] = pd.to_datetime(df_h["time"], errors="coerce")
            df_h.drop(columns=["time"], inplace=True)
            df_h.rename(columns=COLUMN_MAP, inplace=True)
            df_h["Tinh_thanh"] = tinh_thanh
            df_h["Date"] = df_h["Datetime"].dt.date

            # DAILY
            df_d = pd.DataFrame(data["daily"])
            df_d["Date"] = pd.to_datetime(df_d["time"], errors="coerce").dt.date
            df_d.drop(columns=["time"], inplace=True)
            df_d.rename(columns=DAILY_COLUMN_MAP, inplace=True)

            if "Giờ mặt trời mọc" in df_d.columns:
                df_d["Giờ mặt trời mọc"] = pd.to_datetime(
                    df_d["Giờ mặt trời mọc"], errors="coerce"
                ).dt.strftime("%H:%M")

            if "Giờ mặt trời lặn" in df_d.columns:
                df_d["Giờ mặt trời lặn"] = pd.to_datetime(
                    df_d["Giờ mặt trời lặn"], errors="coerce"
                ).dt.strftime("%H:%M")

            # MERGE HOURLY + DAILY THEO NGÀY
            df = df_h.merge(df_d, on="Date", how="left")
            df.drop(columns=["Date"], inplace=True)

            # SẮP XẾP CỘT:
            # - Tinh_thanh đầu
            # - Sau đó: Mã thời tiết, Mã thời tiết ngày (nếu có)
            # - Các cột còn lại
            # - Datetime cuối
            special_cols = ["Mã thời tiết", "Mã thời tiết ngày"]

            cols = ["Tinh_thanh"]
            for sc in special_cols:
                if sc in df.columns:
                    cols.append(sc)

            middle_cols = [
                c for c in df.columns
                if c not in (["Tinh_thanh", "Datetime"] + special_cols)
            ]

            cols += middle_cols
            cols.append("Datetime")
            df = df[cols]

            return df

        # === 429 RATE LIMIT ===
        if status == 429:
            wait = base_wait_429 * attempt + random.randint(0, 60)
            print(f"    ⚠ 429 (rate limit) attempt {attempt}/{max_attempts} – nghỉ {wait}s rồi thử lại...")
            time.sleep(wait)
            continue

        # === 5xx LỖI SERVER TẠM ===
        if status in (500, 502, 503, 504):
            wait = 20 * attempt
            print(f"    ⚠ Lỗi server {status} attempt {attempt}/{max_attempts} – nghỉ {wait}s rồi thử lại...")
            time.sleep(wait)
            continue

        # === LỖI KHÁC (400, 403, ...) -> BỎ BLOCK ===
        print(f"    ❌ Lỗi API {status} cho block {start_str} → {end_str}, bỏ block.")
        return None

    # Nếu tới đây: 429 / 5xx liên tục -> RATE_LIMIT_HARD
    print(f"    ❌ Thử {max_attempts} lần vẫn lỗi (429/5xx). RATE_LIMIT_HARD tại block {start_str} → {end_str}.")
    raise RuntimeError("RATE_LIMIT_HARD")

# =============================
# 8. MAIN – CRAWL TOÀN BỘ VIỆT NAM
# =============================
start_all = date(2000, 1, 1)
today = datetime.now().date()
yesterday = today - timedelta(days=1)

# Load file tổng nếu đã tồn tại
if os.path.exists(out_vn_path):
    df_vn = pd.read_csv(out_vn_path)
    if "Datetime" in df_vn.columns:
        df_vn["Datetime"] = pd.to_datetime(df_vn["Datetime"], errors="coerce")
        df_vn.dropna(subset=["Datetime"], inplace=True)
        df_vn.sort_values(["Tinh_thanh", "Datetime"], inplace=True)
        print(f"Đã có file tổng: {out_vn_path}")
    else:
        print("File tổng không có cột Datetime, crawl lại từ 2000.")
        df_vn = pd.DataFrame()
else:
    print("Chưa có file tổng — crawl từ 2000-01-01.")
    df_vn = pd.DataFrame()

print("=== BẮT ĐẦU CRAWL TOÀN BỘ VIỆT NAM ===")

try:
    # Loop từng tỉnh
    for idx, row in loc_df.iterrows():
        tinh = row["Tinh_thanh"]
        lat, lon = row["lat"], row["lon"]

        print(f"\n({idx+1}/{len(loc_df)}) Tỉnh: {tinh} ({lat}, {lon})")

        # Xác định ngày bắt đầu cho riêng tỉnh này (dựa trên file tổng hiện có)
        if not df_vn.empty and tinh in df_vn["Tinh_thanh"].unique():
            df_tinh = df_vn[df_vn["Tinh_thanh"] == tinh].copy()
            df_tinh["Datetime"] = pd.to_datetime(df_tinh["Datetime"], errors="coerce")
            df_tinh.dropna(subset=["Datetime"], inplace=True)

            if not df_tinh.empty:
                last_date_tinh = df_tinh["Datetime"].max().date()
                start_date = last_date_tinh + timedelta(days=1)
                print(f"  Đã có dữ liệu {tinh} tới: {last_date_tinh}")
            else:
                start_date = start_all
        else:
            start_date = start_all

        # Nếu đã đủ tới hôm qua thì bỏ qua
        if start_date > yesterday:
            print(f"  {tinh} đã đủ dữ liệu tới hôm qua ({yesterday}), bỏ qua.")
            continue

        print(f"  Sẽ crawl thêm cho {tinh} từ {start_date} → {yesterday}")

        df_blocks = []
        cur = start_date

        try:
            while cur <= yesterday:
                # Block 365 ngày (1 năm) để giảm 429
                block_end = min(cur + timedelta(days=365), yesterday)
                s_str = cur.strftime("%Y-%m-%d")
                e_str = block_end.strftime("%Y-%m-%d")

                print(f"  → Block {s_str} → {e_str}")
                df_blk = fetch_block(lat, lon, s_str, e_str, tinh)

                if df_blk is not None and not df_blk.empty:
                    df_blocks.append(df_blk)
                    print(f"    ✔ Lấy được {len(df_blk)} dòng")
                else:
                    print("    ✖ Block này không có dữ liệu / bị bỏ qua")

                cur = block_end + timedelta(days=1)
                # delay giữa các block
                time.sleep(random.uniform(5, 10))

        except RuntimeError as e:
            # RATE_LIMIT_HARD: dừng toàn bộ, nhưng phải lưu file trước
            if "RATE_LIMIT_HARD" in str(e):
                print("  ⛔ Gặp RATE_LIMIT_HARD (429 liên tục). DỪNG TOÀN BỘ CHƯƠNG TRÌNH.")
                # Gộp những block đã lấy được cho tỉnh hiện tại (nếu có)
                if df_blocks:
                    df_new_tinh = pd.concat(df_blocks, ignore_index=True)
                    if df_vn.empty:
                        df_vn = df_new_tinh
                    else:
                        df_vn = pd.concat([df_vn, df_new_tinh], ignore_index=True)

                # Làm sạch + lưu file tổng trước khi dừng
                if not df_vn.empty:
                    df_vn.drop_duplicates(subset=["Tinh_thanh", "Datetime"], inplace=True)
                    df_vn["Datetime"] = pd.to_datetime(df_vn["Datetime"], errors="coerce")
                    df_vn.dropna(subset=["Datetime"], inplace=True)
                    df_vn.sort_values(["Tinh_thanh", "Datetime"], inplace=True)

                    # Sắp xếp lại cột: Tinh_thanh | Mã thời tiết | Mã thời tiết ngày | ... | Datetime
                    special_cols = ["Mã thời tiết", "Mã thời tiết ngày"]
                    cols = ["Tinh_thanh"]
                    for sc in special_cols:
                        if sc in df_vn.columns:
                            cols.append(sc)

                    middle_cols = [
                        c for c in df_vn.columns
                        if c not in (["Tinh_thanh", "Datetime"] + special_cols)
                    ]

                    cols += middle_cols
                    cols.append("Datetime")
                    df_vn = df_vn[cols]

                    df_vn.to_csv(out_vn_path, index=False, encoding="utf-8-sig")
                    print(f"  ✔ ĐÃ LƯU TẠM FILE TỔNG TRƯỚC KHI DỪNG: {out_vn_path}")
                raise
            else:
                raise

        # Hết while, không có RATE_LIMIT_HARD -> gộp dữ liệu tỉnh này vào file tổng
        if df_blocks:
            df_new = pd.concat(df_blocks, ignore_index=True)

            if df_vn.empty:
                df_vn = df_new
            else:
                df_vn = pd.concat([df_vn, df_new], ignore_index=True)

            # Làm sạch + sort + Datetime cuối
            df_vn.drop_duplicates(subset=["Tinh_thanh", "Datetime"], inplace=True)
            df_vn["Datetime"] = pd.to_datetime(df_vn["Datetime"], errors="coerce")
            df_vn.dropna(subset=["Datetime"], inplace=True)
            df_vn.sort_values(["Tinh_thanh", "Datetime"], inplace=True)

            # Sắp xếp lại cột: Tinh_thanh | Mã thời tiết | Mã thời tiết ngày | ... | Datetime
            special_cols = ["Mã thời tiết", "Mã thời tiết ngày"]
            cols = ["Tinh_thanh"]
            for sc in special_cols:
                if sc in df_vn.columns:
                    cols.append(sc)

            middle_cols = [
                c for c in df_vn.columns
                if c not in (["Tinh_thanh", "Datetime"] + special_cols)
            ]

            cols += middle_cols
            cols.append("Datetime")
            df_vn = df_vn[cols]

            df_vn.to_csv(out_vn_path, index=False, encoding="utf-8-sig")
            print(f"  ✔ Đã cập nhật file tổng: {out_vn_path} (tổng {len(df_vn)} dòng)")
        else:
            print(f"  ✖ Không có dữ liệu mới cho {tinh}, không update file tổng.")

        # delay giữa các tỉnh: dài để giảm 429 gần như hết
        wait_province = random.uniform(40, 80)
        print(f"  Nghỉ {int(wait_province)}s trước khi sang tỉnh tiếp theo...")
        time.sleep(wait_province)

except RuntimeError as e:
    if "RATE_LIMIT_HARD" in str(e):
        print("\n⛔ CHƯƠNG TRÌNH DỪNG DO RATE_LIMIT_HARD. HÃY CHẠY LẠI SAU ÍT LÂU.")
    else:
        print(f"\n⛔ DỪNG DO LỖI KHÁC: {e}")
else:
    print("\n🎉 HOÀN TẤT CRAWL TOÀN BỘ VIỆT NAM!")
    if not df_vn.empty:
        print(f"Tổng số dòng cuối cùng: {len(df_vn)}")
