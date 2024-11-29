import pyodbc
from datetime import datetime

def update_product_price_from_tables(start_day=1):
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

        # 플랫폼 리스트 설정
        platforms = ['auction', 'coupang', 'eleven', 'gmarket', 'marketkurly']

        # 1일차 ~ 7일차에 따른 테이블 번호를 설정
        for day_index in range(start_day, start_day + 7):  # 1일부터 7일까지 반복
            for platform in platforms:
                table_name = f"{platform}{day_index}"  # 테이블 이름 설정
                print(f"Fetching data from table: {table_name}")  # 디버깅을 위한 출력

                # 각 테이블의 컬럼 확인: deliver 컬럼이 있는지 확인
                cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'deliver'")
                has_deliver_column = cursor.fetchone() is not None

                # deliver 컬럼이 있는지에 따라 쿼리 구성
                if has_deliver_column:
                    query = f"SELECT title, price, deliver FROM dbo.{table_name}"
                else:
                    query = f"SELECT title, price, '0' AS deliver FROM dbo.{table_name}"  # 기본값으로 '0' 설정

                cursor.execute(query)
                table_data = cursor.fetchall()

                for row in table_data:
                    title = row[0]  # 상품명
                    try:
                        # 가격을 숫자로 변환
                        price = int(row[1].replace(',', '').replace('원', '').strip())
                    except ValueError:
                        print(f"Invalid price format for product '{title}': {row[1]}")
                        continue

                    # 배송비 처리
                    deliver_raw = row[2].strip()
                    if '무료배송' in deliver_raw:
                        deliver = 0
                    else:
                        try:
                            deliver = int(deliver_raw.replace(',', '').replace('원', '').strip())
                        except ValueError:
                            deliver = 0  # 숫자가 아닌 경우 기본값 0으로 처리

                    # 기존 상품 확인
                    cursor.execute(
                        "SELECT * FROM dbo.product_prices WHERE title = ? AND source = ?",
                        (title, platform)
                    )
                    existing_product = cursor.fetchone()

                    # 데이터 밀림 및 업데이트 처리
                    if existing_product:
                        cursor.execute('''  
                            UPDATE dbo.product_prices
                            SET day7_price = day6_price,
                                day6_price = day5_price,
                                day5_price = day4_price,
                                day4_price = day3_price,
                                day3_price = day2_price,
                                day2_price = day1_price,
                                day1_price = ?,
                                deliver = ?,
                                source = ?,
                                last_updated = GETDATE()
                            WHERE title = ? AND source = ?
                        ''', price, deliver, platform, title, platform)
                    else:
                        # 새로운 상품이면 day1_price에 최신 가격 삽입
                        cursor.execute('''
                            INSERT INTO dbo.product_prices 
                            (title, day1_price, deliver, source, last_updated)
                            VALUES (?, ?, ?, ?, GETDATE())
                        ''', title, price, deliver, platform)

        conn.commit()
    except pyodbc.Error as e:
        print(f"Database error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# 스크립트 실행, 첫 시작일을 1로 고정하여 실행
update_product_price_from_tables(start_day=1)
