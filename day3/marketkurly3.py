from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import pyodbc
import time

# ChromeDriver 경로 설정
driver_path = "C:\\chromedriver.exe"
service = ChromeService(executable_path=driver_path)
chrome_options = Options()

# Selenium으로 웹페이지 열기
driver = webdriver.Chrome(service=service, options=chrome_options)
base_url = "https://www.kurly.com/categories/912011"
driver.get(base_url)

# 페이지 로드 대기
time.sleep(5)

# 학습된 모델과 토크나이저 로드 (로컬 저장 경로)
model_dir = 'food_classification_model'  # 학습한 모델 경로
tokenizer = AutoTokenizer.from_pretrained(model_dir)
model = AutoModelForSequenceClassification.from_pretrained(model_dir)

# 분류할 카테고리 목록 (학습 시 사용한 것과 동일해야 함)
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
        return '기타'  # 기본값으로 설정

# 데이터를 저장할 리스트 및 세트
products = []
seen_titles = set()

# 페이지 스크롤 함수 (30초 동안 조금씩 연속적으로 스크롤)
def scroll_to_bottom(duration=30, steps=100):
    total_scroll_time = duration  # 총 스크롤 시간 (30초)
    scroll_pause_time = total_scroll_time / steps  # 각 스크롤 사이의 대기 시간
    current_height = 0  # 현재 스크롤 위치
    last_height = driver.execute_script("return document.body.scrollHeight")  # 전체 페이지 높이
    
    for step in range(steps):
        # 조금씩 스크롤 (총 steps번 나눠서 페이지 끝까지 이동)
        driver.execute_script(f"window.scrollTo(0, {current_height});")
        
        # 스크롤 후 페이지가 로드될 시간을 대기
        time.sleep(scroll_pause_time)
        
        # 스크롤 위치 갱신 (페이지 전체 높이 / steps)
        current_height += last_height / steps
        
        # 페이지가 동적으로 로드되면 새 높이를 가져옴
        new_height = driver.execute_script("return document.body.scrollHeight")
        
        # 페이지 높이가 변경되면 갱신된 높이로 스크롤 진행
        if new_height > last_height:
            last_height = new_height
            
        # 이미 끝까지 스크롤한 경우 중단
        if current_height >= last_height:
            break

# 페이지 네비게이션 범위 설정 (1페이지부터 3페이지까지)
for page in range(1, 4):
    print(f"Scraping page {page}...")

    # 페이지를 20초 동안 천천히 스크롤하여 맨 끝까지 이동
    scroll_to_bottom(duration=30)

    # 페이지 소스 가져오기
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')

    # 상품명, 가격, URL, 이미지 URL 추출
    for item in soup.select('.css-8bebpy.e1c07x488'):  # 상품을 포함하는 a 태그
        title_tag = item.select_one('.css-1dry2r1.e1c07x485')
        price_tag = item.select_one('.price-number')
        relative_url = item['href']  # 상대 URL 추출
        image_tag = item.select_one('img')  # 이미지 URL 추출

        if title_tag and price_tag and image_tag:
            title = title_tag.text.strip()
            price = price_tag.text.strip()
            image_url = image_tag['src']  # 이미지 URL 추출

            # 절대 URL로 변환
            url = f'https://www.kurly.com{relative_url}' if not relative_url.startswith('http') else relative_url

            # 중복된 상품 필터링
            if title not in seen_titles:
                seen_titles.add(title)

                # 카테고리 분류
                category = classify_product(title)

                products.append({
                    'title': title,
                    'price': price,
                    'category': category,
                    'url': url,  # URL 저장
                    'image_url': image_url  # 이미지 URL 저장
                })

    # 다음 페이지로 이동
    try:
        next_button_xpath = '//*[@id="container"]/div/div[2]/div[3]/a[6]'  # '다음' 버튼의 XPATH 수정 필요할 수 있음
        next_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, next_button_xpath))
        )
        print(f"Clicking 'Next' button for page {page}")
        driver.execute_script("arguments[0].click();", next_button)

        # 페이지 로딩 대기 시간
        time.sleep(5)

    except Exception as e:
        print(f"An exception occurred on page {page}: {e}")
        break

# 드라이버 종료
driver.quit()

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

# 테이블이 없을 경우 URL 및 이미지 URL 필드를 포함해서 생성
table_check_query = '''
    IF OBJECT_ID('dbo.marketkurly3', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.marketkurly3 (
            id INT IDENTITY(1,1) PRIMARY KEY,
            title NVARCHAR(255) UNIQUE,  
            price NVARCHAR(50),
            category NVARCHAR(50),
            url NVARCHAR(255),  -- URL 필드 추가
            image_url NVARCHAR(500)  -- 이미지 URL 필드 추가
        )
    END
'''

cursor.execute(table_check_query)

# 데이터 삽입
for product in products:
    print(f"Attempting to insert product: {product['title']} - Category: {product['category']} - Image URL: {product['image_url']} - URL: {product['url']}")
    try:
        cursor.execute('''
            IF NOT EXISTS (SELECT 1 FROM dbo.marketkurly3 WHERE title = ?)
            BEGIN
                INSERT INTO dbo.marketkurly3 (title, price, category, url, image_url)
                VALUES (?, ?, ?, ?, ?)
            END
        ''', product['title'], product['title'], product['price'], product['category'], product['url'], product['image_url'])
    except pyodbc.IntegrityError as e:
        print(f"Duplicate entry found for product: {product['title']} - Skipping insertion. Error: {e}")
    except Exception as e:
        print(f"An error occurred while inserting product: {product['title']} - Error: {e}")

conn.commit()
cursor.close()
conn.close()

print("Data saved to the database successfully.")