import pyodbc
from datetime import datetime

def update_product_price_from_tables():
    try:
        conn_str = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=admin.ch2qqkwuwon8.ap-northeast-2.rds.amazonaws.com,1433;'
            'DATABASE=dlfgh;'
            'UID=admin;'
            'PWD=dnflrkwhr72;'
        )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 테이블 리스트 설정 (auction1부터 auction7까지)
        tables = [f'coupang{day}' for day in range(1, 8)]
        platform = 'coupang'

        for table_name in tables:
            print(f"Fetching data from table: {table_name}")

            # 테이블이 존재하는지 확인
            try:
                cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
            except pyodbc.Error as e:
                print(f"Table {table_name} does not exist or cannot be accessed. Skipping. Error: {e}")
                continue

            # 쿼리 구성
            query = f"SELECT title, price"
            try:
                cursor.execute(f"SELECT TOP 1 deliver FROM {table_name}")
                query += ", deliver"
            except pyodbc.Error:
                query += ", '0' AS deliver"  # 기본값 설정

            try:
                cursor.execute(f"SELECT TOP 1 url FROM {table_name}")
                query += ", url"
            except pyodbc.Error:
                query += ", '' AS url"  # 기본값 설정

            try:
                cursor.execute(f"SELECT TOP 1 image_url FROM {table_name}")
                query += ", image_url"
            except pyodbc.Error:
                query += ", '' AS image_url"  # 기본값 설정

            query += f" FROM {table_name}"

            # 데이터 가져오기
            try:
                cursor.execute(query)
            except pyodbc.Error as e:
                print(f"Error executing query on {table_name}: {e}")
                continue

            for row in cursor.fetchall():
                title = row[0]
                try:
                    price = int(row[1].replace(',', '').replace('원', '').strip())
                except ValueError:
                    print(f"Invalid price format for product '{title}': {row[1]}")
                    continue

                # 배송비 처리
                deliver_raw = row[2].strip() if len(row) > 2 else '0'
                if '무료배송' in deliver_raw:
                    deliver = 0
                else:
                    try:
                        deliver = int(deliver_raw.replace(',', '').replace('원', '').strip())
                    except ValueError:
                        deliver = 0

                url = row[3] if len(row) > 3 else ''
                image_url = row[4] if len(row) > 4 else ''

                # 기존 상품 확인
                cursor.execute(
                    "SELECT * FROM dbo.coupangprices WHERE title = ? AND platform = ?",
                    (title, platform)
                )
                existing_product = cursor.fetchone()

                # 데이터 밀림 및 업데이트 처리
                if existing_product:
                    cursor.execute('''  
                        UPDATE dbo.coupangprices
                        SET day7_price = day6_price,
                            day6_price = day5_price,
                            day5_price = day4_price,
                            day4_price = day3_price,
                            day3_price = day2_price,
                            day2_price = day1_price,
                            day1_price = ?,
                            deliver = ?,
                            url = ?,
                            image_url = ?,
                            platform = ?,
                            last_updated = GETDATE()
                        WHERE title = ? AND platform = ?
                    ''', price, deliver, url, image_url, platform, title, platform)
                else:
                    # 새로운 상품이면 day1_price에 최신 가격 삽입
                    cursor.execute('''
                        INSERT INTO dbo.coupangprices 
                        (title, day1_price, deliver, url, image_url, platform, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, GETDATE())
                    ''', title, price, deliver, url, image_url, platform)

        conn.commit()
    except pyodbc.Error as e:
        print(f"Database error: {e}")
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception as e:
            print(f"Error closing cursor: {e}")

        try:
            if conn:
                conn.close()
        except Exception as e:
            print(f"Error closing connection: {e}")

# 스크립트 실행
update_product_price_from_tables()
