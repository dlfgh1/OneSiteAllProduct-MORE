from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from bs4 import BeautifulSoup
import pandas as pd
import pyodbc
import time
import random
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# 학습된 모델과 토크나이저 로드 (로컬 저장 경로)
model_dir = 'food_classification_model'  # 학습한 모델 경로
tokenizer = AutoTokenizer.from_pretrained(model_dir)
model = AutoModelForSequenceClassification.from_pretrained(model_dir)

# 분류할 카테고리 목록
categories = ['한식', '중식', '일식', '양식', '기타']

# 상품명 분류 함수
def classify_product(title):
    try:
        inputs = tokenizer(title, return_tensors="pt", padding=True, truncation=True, max_length=128)
        with torch.no_grad():
            outputs = model(**inputs)
        logits = outputs.logits
        predicted_class = torch.argmax(logits, dim=1).item()
        return categories[predicted_class]
    except Exception as e:
        print(f"Error classifying product: {e}")
        return '기타'

# ChromeDriver 경로 설정
driver_path = 'C:/chromedriver.exe'

# ChromeDriver 서비스 객체 생성
service = ChromeService(executable_path=driver_path)

# Chrome 옵션 객체 생성
chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument('--disable-infobars')
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

# Selenium으로 웹페이지 열기
driver = webdriver.Chrome(service=service, options=chrome_options)

# Selenium Stealth 적용
stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
        )

# 웹사이트 접속
url = "https://www.coupang.com/np/categories/486687"
driver.get(url)

# 데이터 저장 리스트 및 세트
products = []
seen_titles = set()

# 페이지 단위로 네비게이션
page = 1
max_pages = 20  # 최대 페이지 수를 20으로 설정
while page <= max_pages:
    print(f"Scraping page {page}...")

    try:
        # 페이지 로드 대기
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.baby-product-wrap'))
        )

        # 페이지 소스 가져오기
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        # 상품명, 가격, 이미지 URL 추출
        for item in soup.select('.baby-product-wrap'):
            title = item.select_one('.name').text.strip()
            price = item.select_one('.price-value').text.strip()

            # URL 추출
            product_link = item.find_parent('a', class_='baby-product-link')['href']
            full_url = f"https://www.coupang.com{product_link}"

            # 이미지 URL 추출
            image_tag = item.select_one('dt.image img')
            image_url = image_tag['src'] if image_tag else ''

            if title not in seen_titles:
                seen_titles.add(title)
                category = classify_product(title)
                products.append({
                    'title': title,
                    'price': price,
                    'category': category,
                    'url': full_url,  # URL 추가
                    'image_url': image_url  # 이미지 URL 추가
                })

        # 다음 페이지로 이동
        try:
            next_button = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a.icon.next-page'))
            )
            
            if next_button.is_enabled():  # 버튼이 활성화 되어있는지 확인
                driver.execute_script("arguments[0].click();", next_button)
                # 페이지 이동 대기
                time.sleep(random.uniform(5, 10))  # 페이지 로딩 대기
                page += 1
            else:
                print("No more pages to scrape.")
                break  # 다음 페이지가 없으면 스크래핑 종료

        except Exception as e:
            print(f"No next button found or other error: {e}")
            break  # 버튼을 찾지 못하면 종료

    except Exception as e:
        print(f"An exception occurred on page {page}: {e}")
        break

# 드라이버 종료
try:
    driver.quit()
except Exception as e:
    print(f"An error occurred while quitting the driver: {e}")

# 데이터베이스에 저장 (상품 URL 및 이미지 URL 포함)
conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=admin.ch2qqkwuwon8.ap-northeast-2.rds.amazonaws.com,1433;'
        'DATABASE=dlfgh;'  # 데이터베이스 이름 수정
        'UID=admin;'  # SQL Server 사용자 아이디
        'PWD=dnflrkwhr72;'  # SQL Server 비밀번호
        )

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # 테이블이 없을 경우 생성
    table_check_query = '''
        IF OBJECT_ID('dbo.coupang7', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.coupang7 (
                id INT IDENTITY(1,1) PRIMARY KEY,
                title NVARCHAR(255) UNIQUE,  
                price NVARCHAR(50),
                category NVARCHAR(50),
                url NVARCHAR(500),  -- 상품 URL 필드 추가
                image_url NVARCHAR(500)  -- 이미지 URL 필드 추가
            )
        END
    '''

    cursor.execute(table_check_query)

    for product in products:
        print(f"Attempting to insert product: {product['title']} - URL: {product['url']} - Image URL: {product['image_url']}")
        try:
            cursor.execute('''
                IF NOT EXISTS (SELECT 1 FROM dbo.coupang7 WHERE title = ?)
                BEGIN
                    INSERT INTO dbo.coupang7 (title, price, category, url, image_url)
                    VALUES (?, ?, ?, ?, ?)
                END
            ''', product['title'], product['title'], product['price'], product['category'], product['url'], product['image_url'])
        except pyodbc.IntegrityError as e:
            print(f"Duplicate entry found for product: {product['title']} - Skipping insertion. Error: {e}")
        except Exception as e:
            print(f"An error occurred while inserting product: {product['title']} - Error: {e}")

    conn.commit()
except Exception as e:
    print(f"Database operation failed: {e}")
finally:
    cursor.close()
    conn.close()

print("Data saved to the database successfully.")