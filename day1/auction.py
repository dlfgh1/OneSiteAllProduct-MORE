import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import torch
import pyodbc
import time
import random
import re  # 할인 후 가격 추출을 위한 정규식 추가
import gc

# Undetected ChromeDriver 설정 (특정 버전 129 사용)
driver = uc.Chrome(version_main=129)

# 웹사이트 접속
base_url = "https://www.auction.co.kr/n/list?category=64241600"
driver.get(base_url)

# 페이지 로드 대기
time.sleep(5)

# 데이터를 저장할 리스트 및 세트
products = []
seen_titles = set()

# 밀키트 관련 키워드 리스트
meal_kit_keywords = ['밀키트', '인분', '팩', '쿠킹박스', '인용']

# 2페이지까지 네비게이션
for page in range(1, 41):  # 페이지 범위 설정 (1부터 40까지)
    print(f"Scraping page {page}...")

    # 페이지 소스 가져오기
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')

    # 상품명, 가격, 배송비, URL, 이미지 URL 추출
    for item in soup.select('.section--itemcard'):
        title_tag = item.select_one('.area--itemcard_title')
        price_tag = item.select_one('.area--itemcard_price')
        link_tag = item.select_one('.section--itemcard a')  # 상품 링크 추출
        img_tag = item.select_one('.section--itemcard_img img')  # 이미지 태그 추출

        # 할인 후 가격 추출 (정규식 사용)
        if price_tag:
            price_text = price_tag.text.strip()
            final_price_match = re.search(r'(\d{1,3}(,\d{3})*)(?=원)', price_text)
            if final_price_match:
                final_price = final_price_match.group(0).replace(',', '')  # 할인 후 가격 추출
            else:
                final_price = "0"  # 가격이 없으면 0 처리
        else:
            final_price = "0"

        # 배송비 정보 초기화
        delivery_info = 0  # 기본값으로 설정

        # 배송비 정보 추출
        delivery_tags = item.select('li.item')  # 배송비 정보가 있는지 확인
        for delivery_tag in delivery_tags:
            if "배송비" in delivery_tag.text:
                delivery_info = delivery_tag.text.strip()  # 배송비 정보 추출
                break  # 배송비 정보가 확인되면 루프 종료

        if title_tag and final_price and link_tag and img_tag:
            title = title_tag.text.strip()
            url = link_tag['href']  # 상품 URL 추출
            img_url = img_tag['src']  # 이미지 URL 추출

            # 밀키트 관련 키워드 필터링
            if any(keyword.lower() in title.lower() for keyword in meal_kit_keywords):
                # 중복된 상품 필터링
                if title not in seen_titles:
                    seen_titles.add(title)
                    products.append({
                        'title': title,
                        'price': final_price,
                        'deliver': delivery_info,  # 배송 정보 저장
                        'category': '미분류',  # 기본값으로 설정
                        'url': url,  # 상품 URL 추가
                        'img_url': img_url  # 이미지 URL 추가
                    })
                    print(f"Found product: {title} - Price: {final_price} - Delivery: {delivery_info} - URL: {url} - Image: {img_url}")

    # 다음 페이지로 이동
    if page < 40:  # 40페이지까지만 진행
        try:
            # '다음' 버튼 클릭 (link--next_page 클래스 사용)
            next_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'link--next_page'))
            )

            if next_button.is_enabled():
                print(f"Clicking 'Next' button for page {page}")
                driver.execute_script("arguments[0].click();", next_button)
                # 페이지 로딩 대기 시간을 랜덤하게 설정
                time.sleep(random.uniform(4, 7))
            else:
                print("No more pages to scrape.")
                break  # 다음 페이지가 없으면 스크래핑 종료

        except Exception as e:
            print(f"An exception occurred on page {page}: {e}")
            break

# 드라이버 종료 및 가비지 컬렉션 처리
try:
    driver.quit()
except Exception as e:
    print(f"An error occurred while quitting the driver: {e}")
finally:
    del driver
    gc.collect()

# 데이터베이스에 저장 (상품 URL 및 이미지 URL 포함)
conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=admin.ch2qqkwuwon8.ap-northeast-2.rds.amazonaws.com,1433;'
        'DATABASE=dlfgh;'  # 데이터베이스 이름 수정
        'UID=admin;'  # SQL Server 사용자 아이디
        'PWD=dnflrkwhr72;'  # SQL Server 비밀번호
        )

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# 테이블이 없을 경우 생성 (URL 및 이미지 URL 필드 추가)
table_check_query = '''
    IF OBJECT_ID('dbo.auction1', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.auction1 (
            id INT IDENTITY(1,1) PRIMARY KEY,
            title NVARCHAR(255) UNIQUE,  
            price NVARCHAR(50),
            deliver NVARCHAR(50),
            category NVARCHAR(50),
            url NVARCHAR(500),  -- 상품 URL 필드
            img_url NVARCHAR(500)  -- 이미지 URL 필드 추가
        )
    END
'''

cursor.execute(table_check_query)

for product in products:
    print(f"Attempting to insert product: {product['title']} - URL: {product['url']}")
    try:
        cursor.execute('''
            IF NOT EXISTS (SELECT 1 FROM dbo.auction1 WHERE title = ?)
            BEGIN
                INSERT INTO dbo.auction1 (title, price, deliver, category, url, img_url)
                VALUES (?, ?, ?, ?, ?, ?)
            END
        ''', product['title'], product['title'], product['price'], product['deliver'], product['category'], product['url'], product['img_url'])
    except pyodbc.IntegrityError as e:
        print(f"Duplicate entry found for product: {product['title']} - Skipping insertion. Error: {e}")
    except Exception as e:
        print(f"An error occurred while inserting product: {product['title']} - Error: {e}")

conn.commit()
cursor.close()
conn.close()

print("Data saved to the database successfully.")
