import pyodbc

# 데이터베이스 연결 설정
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=admin.ch2qqkwuwon8.ap-northeast-2.rds.amazonaws.com,1433;'  # 서버 정보
    'DATABASE=dlfgh;'      # 데이터베이스 이름
    'UID=admin;'           # 데이터베이스 사용자 이름
    'PWD=dnflrkwhr72;'     # 데이터베이스 암호
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 각 테이블에서 데이터를 가져오는 함수
def get_products_from_table(table_name):
    if table_name == 'dbo.marketkurly' or table_name == 'dbo.coupang':
        # marketkurly와 coupang에는 deliver 컬럼이 없으므로 그 값을 제외하고 가져옴
        cursor.execute(f"SELECT title, price, category FROM {table_name}")
        rows = cursor.fetchall()
        products = []
        for row in rows:
            products.append({
                'title': row[0],
                'price': row[1],
                'deliver_price': 0,  # deliver 정보가 없으므로 0으로 처리
                'category': row[2]
            })
        return products
    else:
        # deliver가 있는 테이블 (eleven, auction, gmarket 등)
        cursor.execute(f"SELECT title, price, deliver, category FROM {table_name}")
        rows = cursor.fetchall()
        products = []
        for row in rows:
            deliver_price = 0  # 기본값 설정
            if '배송비' in row[2]:  # 배송비가 존재하는 경우
                try:
                    # 배송비가 있는 경우 숫자만 추출
                    deliver_price = int(''.join(filter(str.isdigit, row[2])))
                except ValueError:
                    deliver_price = 0  # 값이 없거나 오류가 발생한 경우 0으로 처리
            products.append({
                'title': row[0],
                'price': row[1],
                'deliver_price': deliver_price,
                'category': row[3]
            })
        return products

# 테이블 목록
tables = ['auction', 'coupang', 'eleven', 'gmarket', 'marketkurly']

# 각 테이블의 데이터를 순회하면서 처리
for table in tables:
    print(f"Processing table: {table}")
    products = get_products_from_table(f'dbo.{table}')
    
    # 배치 처리를 위해 한번에 100개의 데이터를 묶어서 처리
    batch_size = 100
    batches = [products[i:i + batch_size] for i in range(0, len(products), batch_size)]

    for batch in batches:
        # 쿼리 실행 전에 데이터를 묶어서 처리
        insert_data = []
        for product in batch:
            product_id = hash(product['title']) % 9223372036854775807  # BIGINT 범위 내로 맞춤
            insert_data.append((product_id, product['title'], product['price'], product['deliver_price'], product['category']))

        # 한 번에 배치로 데이터를 삽입 (중복 방지 및 삽입)
        for data in insert_data:
            cursor.execute('''
                IF NOT EXISTS (SELECT 1 FROM dbo.product_prices WHERE product_id = ? AND recorded_date = CAST(GETDATE() AS DATE))
                BEGIN
                    INSERT INTO dbo.product_prices (product_id, title, price, deliver_price, category)
                    VALUES (?, ?, ?, ?, ?)
                END
            ''', data[0], data[0], data[1], data[2], data[3], data[4])

# 7일 이상 된 데이터 삭제
cursor.execute('''
    DELETE FROM dbo.product_prices
    WHERE recorded_date < DATEADD(DAY, -7, GETDATE());
''')

# 변경 사항 커밋 및 종료
conn.commit()
cursor.close()
conn.close()

print("Data updated and old records removed.")
