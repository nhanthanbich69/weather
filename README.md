# Weather Data Pipeline - Việt Nam (2000–nay)

Pipeline tự động crawl dữ liệu thời tiết lịch sử **63 tỉnh thành Việt Nam** từ **Open-Meteo Archive API**, xử lý incremental (chỉ lấy phần còn thiếu) và đẩy thẳng vào bảng **Supabase PostgreSQL** (`data_raw` + `vn_locations`).

**Đặc điểm chính**:
- Incremental crawl: Chỉ lấy dữ liệu mới mỗi ngày
- Không lưu file CSV/DuckDB local → tất cả trên cloud Supabase
- Tự động xử lý rate limit (dừng khi nặng, chạy lại sau)
- Chạy tự động hàng ngày qua GitHub Actions

## Mục lục
- [Quick Start](#quick-start)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Pipeline chi tiết](#pipeline-chi-tiết)
- [GitHub Actions tự động](#github-actions-tự-động)
- [Kết nối Power BI](#kết-nối-power-bi)
- [Khắc phục sự cố](#khắc-phục-sự-cố)
- [Bảo mật](#bảo-mật)

## Quick Start

1. Clone dự án
   ```bash
   git clone <your-repo-url>
   cd <repo-name>

Tạo môi trường ảo & cài góiBashpython -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
Tạo file .env ở thư mục gốc (bắt buộc)envSUPABASE_HOST=aws-1-ap-south-1.pooler.supabase.com
SUPABASE_PORT=6543
SUPABASE_DB=postgres
SUPABASE_USER=postgres.ltnpdhycwvilknhsktps
SUPABASE_PASSWORD=duyanhvivo15
Kiểm tra & chạy thửBashpython validate_pipeline.py   # Kiểm tra setup
python main.py                # Crawl & sync vào Supabase

Cấu trúc dự án
textproject/
├── config.py               # Cấu hình (63 tỉnh, column map,...)
├── utils.py                # Hàm tiện ích (fetch API, upsert Supabase)
├── main.py                 # Pipeline chính (crawl incremental → Supabase)
├── validate_pipeline.py    # Kiểm tra setup trước khi chạy
├── requirements.txt        # Dependencies tối thiểu
├── .env.example            # Mẫu file .env
└── .github/workflows/
    └── pipeline.yml        # Tự động chạy hàng ngày
Pipeline chi tiết

main.py:
Kiểm tra ngày cuối cùng đã có trong bảng data_raw trên Supabase
Crawl incremental từ ngày còn thiếu đến hôm qua
Fetch theo block 365 ngày để giảm rate limit
Xử lý lỗi 429 (retry lâu), dừng khi quá nặng (RATE_LIMIT_HARD)
Đẩy dữ liệu mới bằng lệnh COPY (rất nhanh)

Bảng trên Supabase:
vn_locations → Tọa độ 63 tỉnh (lat, long) – tĩnh
data_raw → Dữ liệu thời tiết chi tiết (tỉnh, datetime, nhiệt độ, mưa, gió,...)
