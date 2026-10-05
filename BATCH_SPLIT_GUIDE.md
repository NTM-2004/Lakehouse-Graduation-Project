# Hướng dẫn chia batch dữ liệu (Clickstream, Olist, Marketing Funnel)

Tài liệu này giải thích cách 3 nguồn dữ liệu được chia thành batch **bulk / incremental** dùng chung một ranh giới thời gian, và hướng dẫn chạy script để tái tạo đúng bộ batch.

## 1. Ranh giới batch (dùng chung cho cả 3 nguồn)

| Batch | Khoảng thời gian | Vai trò |
|---|---|---|
| `bulk_2016-09_2018-05` | đầu dữ liệu → 2018-05-31 23:59:59 | Nạp lịch sử một lần |
| `2018-06` | 2018-06-01 → 2018-06-30 | Incremental #1 |
| `2018-07` | 2018-07-01 → 2018-07-31 | Incremental #2 |
| `2018-08` | 2018-08-01 → 2018-08-31 | Incremental #3 |

Cả 3 script đều khai báo `BATCH_BOUNDARIES` giống hệt nhau. **Không sửa riêng lẻ**: nếu đổi ranh giới thì phải đổi ở cả 3 file, nếu không các nguồn sẽ lệch batch và join sai.

## 2. Tổng quan cách chia

| Nguồn | Cột thời gian dùng để chia | Bulk chứa | Incremental chứa |
|---|---|---|---|
| Clickstream | timestamp sinh theo `order_purchase_timestamp` | Log của các đơn ≤ 2018-05-31 | Log của các đơn trong tháng tương ứng |
| Olist – bảng **fact** | `order_purchase_timestamp` | 4 bảng đã lọc theo thời gian | 4 bảng đã lọc theo thời gian |
| Olist – bảng **dimension** | không có | **Toàn bộ, không lọc** | Không có |
| Marketing Funnel – closed deals | `won_date` | Deals ≤ 2018-05-31 | Deals trong tháng tương ứng |
| Marketing Funnel – MQL | `first_contact_date` | **Toàn bộ 8,000 dòng** | Không có |

## 3. Clickstream

Script: `generate_clickstream.py`. Log được sinh **theo từng đơn hàng thật** trong batch, nên mỗi dòng gắn được về Olist.

- `USER_ID` = `customer_id` thật của Olist.
- `ORDER_ID` = `order_id` thật (`N/A` với session không mua).
- Sự kiện `purchase` có `AMOUNT` = tổng `price` các item của đơn và timestamp trùng `order_purchase_timestamp`.
- Có thêm *bounce session* (chỉ browse, không mua) theo `--bounce-ratio` (mặc định 1.5 × số converting session) để funnel có tỷ lệ rơi rụng thực tế.
- Đơn hàng **không có item** trong `order_items` bị bỏ qua (không sinh session).

Số converting session mỗi batch (đã kiểm chứng khớp với số đơn thật có item):

| Batch | Converting sessions |
|---|---|
| bulk | 79,780 |
| 2018-06 | 6,160 |
| 2018-07 | 6,273 |
| 2018-08 | 6,452 |

Chạy:

```bash
python generate_clickstream.py --olist-dir olist --all --output-root raw/clickstream
```

Kiểm tra khớp với Olist gốc:

```bash
python check_clickstream.py --olist-dir olist --clickstream-dir raw/clickstream
```

## 4. Olist

Script: `split_olist_batches.py`.

**Bảng fact** (chia theo `order_purchase_timestamp`, có mặt ở mọi batch):
`olist_orders_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_order_reviews_dataset.csv`. Các bảng con được lọc theo `order_id` thuộc batch.

**Bảng dimension / reference** (chỉ nạp ở bulk, **toàn bộ, không lọc**):
`olist_customers_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv`, `olist_geolocation_dataset.csv`, `product_category_name_translation.csv`.

Vì sao không có orphan key: file dimension gốc là bản export tĩnh, đầy đủ ngay từ đầu (99,441 customers khớp 99,441 orders). Mọi `customer_id`, `product_id`, `seller_id` của đơn tháng 6–8/2018 đều đã có sẵn trong dimension nạp ở bulk.

> Lưu ý: nếu ai đó lọc dimension theo đơn của bulk thì sẽ bị orphan (100% `customer_id` của incremental không có trong bulk, vì Olist sinh `customer_id` mới cho mỗi đơn). Luôn nạp **toàn bộ** file dimension.

Số dòng orders / items / payments / reviews:

| Batch | orders | order_items | payments | reviews |
|---|---|---|---|---|
| bulk | 80,450 | 91,231 | 84,242 | 80,302 |
| 2018-06 | 6,167 | 7,078 | 6,419 | 6,147 |
| 2018-07 | 6,292 | 7,092 | 6,507 | 6,269 |
| 2018-08 | 6,512 | 7,248 | 6,698 | 6,487 |

Chạy:

```bash
python split_olist_batches.py --olist-dir olist --output-root raw/olist --all
```

## 5. Marketing Funnel by Olist

Dataset có 2 bảng:

| Bảng | Số dòng | Khoá | Cột thời gian |
|---|---|---|---|
| `olist_marketing_qualified_leads_dataset.csv` (MQL) | 8,000 | `mql_id` | `first_contact_date` (2017-06-14 → 2018-05-31) |
| `olist_closed_deals_dataset.csv` | 842 | `mql_id`, `seller_id` | `won_date` (2017-12-05 → 2018-11-14) |

Quy tắc chia (cùng ranh giới ở mục 1):

- **MQL** chia theo `first_contact_date`. Toàn bộ 8,000 lead đều ≤ 2018-05-31, nên **tất cả nằm ở bulk**; batch incremental không có file MQL.
- **Closed deals** chia theo `won_date`.

Kết quả:

| Batch | MQL | closed_deals |
|---|---|---|
| bulk | 8,000 | 665 |
| 2018-06 | không có file | 57 |
| 2018-07 | không có file | 37 |
| 2018-08 | không có file | 33 |
| Ngoài cửa sổ (sau 2018-08-31) | 0 | **50 (bị loại)** |

Tổng: 665 + 57 + 37 + 33 + 50 = 842.

Những điểm cần biết:

1. **50 closed deals có `won_date` từ 2018-09 đến 2018-11-14** nằm ngoài 4 batch. Mặc định script **loại** chúng và in cảnh báo, cho nhất quán với Olist và clickstream (đều dừng ở 2018-08-31). Nếu nhóm muốn giữ, chạy thêm `--include-post-window` để gộp vào batch `2018-08`.
2. **Không orphan giữa MQL và closed deals**: mọi `mql_id` trong closed deals đều có trong MQL, và MQL nằm hết ở bulk, nên deal ở batch incremental vẫn join được về lead.
3. **`seller_id` trong closed deals**: chỉ 380/842 deals có `seller_id` trùng với `olist_sellers_dataset.csv`. Đây là đặc tính dữ liệu (nhiều seller được chốt deal nhưng chưa có đơn bán). Dùng **LEFT JOIN** khi nối với dim_sellers, không dùng INNER JOIN.
4. **1 dòng bất thường**: có 1 deal với `won_date` sớm hơn `first_contact_date` của lead tương ứng. Script không sửa dữ liệu; downstream nên xử lý/gắn cờ nếu cần tính thời gian chốt deal.
5. Cột `origin` của MQL có 60 giá trị null, giữ nguyên, xử lý ở bước làm sạch.

Chạy:

```bash
python split_marketing_funnel_batches.py --mkt-dir marketing_funnel --output-root raw/marketing_funnel --all

# nếu muốn giữ 50 deals sau 2018-08-31 (gộp vào batch 2018-08)
python split_marketing_funnel_batches.py --mkt-dir marketing_funnel --output-root raw/marketing_funnel --all --include-post-window
```

## 6. Cấu trúc thư mục output

```
raw/
├── clickstream/
│   ├── bulk_2016-09_2018-05/   access_log.txt, _SUCCESS
│   ├── 2018-06/                access_log.txt, _SUCCESS
│   ├── 2018-07/                access_log.txt, _SUCCESS
│   └── 2018-08/                access_log.txt, _SUCCESS
├── olist/
│   ├── bulk_2016-09_2018-05/   4 fact + 5 dimension (9 file), _SUCCESS
│   ├── 2018-06/                4 fact, _SUCCESS
│   ├── 2018-07/                4 fact, _SUCCESS
│   └── 2018-08/                4 fact, _SUCCESS
└── marketing_funnel/
    ├── bulk_2016-09_2018-05/   closed_deals + MQL, _SUCCESS
    ├── 2018-06/                closed_deals, _SUCCESS
    ├── 2018-07/                closed_deals, _SUCCESS
    └── 2018-08/                closed_deals, _SUCCESS
```

File `_SUCCESS` (rỗng) đánh dấu batch đã ghi xong; pipeline chỉ đọc batch có file này.

## 7. Quy trình chạy toàn bộ

```bash
pip install pandas

python split_olist_batches.py            --olist-dir olist            --output-root raw/olist            --all
python generate_clickstream.py           --olist-dir olist            --output-root raw/clickstream      --all
python split_marketing_funnel_batches.py --mkt-dir marketing_funnel   --output-root raw/marketing_funnel --all
python check_clickstream.py              --olist-dir olist            --clickstream-dir raw/clickstream
```

`generate_clickstream.py` dùng seed cố định (`--seed`, mặc định `clickstream`), nên mọi thành viên chạy đều ra cùng kết quả. Không đổi seed và `--bounce-ratio` nếu không thống nhất với cả nhóm.

## 8. Điều cần nhớ khi join ở downstream

- Clickstream ↔ Olist: join qua `order_id` (và `customer_id` = `USER_ID`). Session bounce có `ORDER_ID = N/A`, không join được, đúng thiết kế.
- Fact incremental ↔ dimension: join với dimension đã nạp ở bulk. Không cần dimension trong batch incremental.
- Closed deals ↔ MQL: qua `mql_id`. Closed deals ↔ sellers: qua `seller_id`, dùng LEFT JOIN.
- Mọi batch phải được nạp theo thứ tự: bulk trước, sau đó 2018-06 → 2018-07 → 2018-08.
