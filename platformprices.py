import pyodbc

def update_product_price(platform_table, platform):
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

        print(f"Fetching data from table: {platform_table}")

        # 테이블이 존재하는지 확인
        cursor.execute(f"SELECT TOP 1 * FROM {platform_table}")

        # 쿼리 구성
        query = f"SELECT title, price, deliver, url, image_url FROM {platform_table}"

        # 데이터 가져오기
        cursor.execute(query)

        for row in cursor.fetchall():
            title = row[0]
            price = int(row[1].replace(',', '').replace('원', '').strip()) if row[1] else 0

            # 배송비 처리
            deliver_raw = row[2].strip() if len(row) > 2 else '0'
            if '무료배송' in deliver_raw:
                deliver = 0
            else:
                deliver_value = ''.join(filter(str.isdigit, deliver_raw))
                deliver = int(deliver_value) if deliver_value else 0

            url = row[3] if len(row) > 3 else ''
            image_url = row[4] if len(row) > 4 else ''

            # 언더바 없는 플랫폼 가격 테이블 이름 구성
            platform_prices_table = f"{platform}prices"  # 'gmarketprices' 형태로 생성

            # 기존 상품 확인
            cursor.execute(f"SELECT * FROM dbo.{platform_prices_table} WHERE title = ?", (title,))
            existing_product = cursor.fetchone()

            # 데이터 밀림 및 업데이트 처리
            if existing_product:
                cursor.execute(f'''  
                    UPDATE dbo.{platform_prices_table}
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
                        last_updated = GETDATE()
                    WHERE title = ?
                ''', price, deliver, url, image_url, title)
            else:
                # 새로운 상품이면 day1_price에 최신 가격 삽입
                cursor.execute(f'''
                    INSERT INTO dbo.{platform_prices_table} 
                    (title, day1_price, deliver, url, image_url, platform, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, GETDATE())
                ''', title, price, deliver, url, image_url, platform)  # platform 값 추가

        conn.commit()
    except pyodbc.Error as e:
        print(f"Database error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# 스크립트 실행
# 아래 두 개의 인자를 변경하여 특정 테이블과 플랫폼을 설정할 수 있습니다.
update_product_price('eleven2', 'eleven')  # 'gmarket1'을 원하는 테이블로 변경
