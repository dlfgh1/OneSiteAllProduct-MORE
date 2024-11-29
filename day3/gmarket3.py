import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import pyodbc
import time
import random
import gc

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

# Undetected ChromeDriver 설정
driver = uc.Chrome(version_main=129)

# 웹사이트 접속
base_url = "https://www.gmarket.co.kr/n/list?category=300028697&gate_id=5C6A3D0C-0670-40E6-B2C8-984A5471A3CE"
driver.get(base_url)

# 페이지 로드 대기
time.sleep(5)

# 데이터를 저장할 리스트 및 세트
products = []
seen_titles = set()

# 밀키트 관련 키워드 리스트
meal_kit_keywords = ['밀키트', '인분', '팩', '쿠킹박스', '인용']

# 페이지 네비게이션 로직
for page in range(1, 52):  # 1페이지부터 원하는 페이지 수로 조정 가능
    print(f"Scraping page {page}...")

    # 페이지 소스 가져오기
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')

    # 상품명, 가격, 배송비, 이미지 URL 추출
    for item in soup.select('.box__item-container'):
        title_tag = item.select_one('.box__item-title')
        price_tag = item.select_one('.text__value')
        deliver_tag = item.select_one('span.text__tag[style*="color:#424242"]')  # 유료 배송
        free_deliver_tag = item.select_one('img[alt="무료배송"]')  # 무료배송 아이콘
        image_tag = item.select_one('.box__image img')  # 이미지 URL 추출

        if title_tag and price_tag and image_tag:
            title = title_tag.text.strip()
            price = price_tag.text.strip()
            image_url = image_tag['src']  # 이미지 URL 추출
       
        # 배송비 처리
            if free_deliver_tag:  # 무료배송 아이콘이 있을 경우
                deliver = "0원"  # 무료배송이면 0원으로 설정
            elif deliver_tag:  # 유료 배송이 있을 경우
                deliver = deliver_tag.text.strip()  # 배송비 텍스트 가져오기
            else:  # 배송비 정보가 전혀 없을 경우
                deliver = "0원"  # 기본적으로 무료배송으로 설정

            # URL 추출, href 속성이 존재하는 경우에만
            url = item.select_one('a')['href'] if item.select_one('a') else ''
            
            # 밀키트 관련 키워드 필터링
            if any(keyword.lower() in title.lower() for keyword in meal_kit_keywords):
                if title and price and deliver:
                    # 중복된 상품 필터링
                    if title not in seen_titles:
                        seen_titles.add(title)
                        # 카테고리 분류 추가
                        category = classify_product(title)
                        products.append({
                            'title': title,
                            'price': price,
                            'deliver': deliver,
                            'url': url,  # URL 추가
                            'image_url': image_url,  # 이미지 URL 추가
                            'category': category  # 카테고리 추가
                        })

    # 페이지 네비게이션 처리
    try:
        # 페이지 네비게이션을 위한 버튼 클릭 로직
        if page % 10 == 0:
            # 10의 배수 페이지에서는 '다음' 버튼 클릭
            next_button_xpath = '//a[contains(@class, "next") and not(contains(@class, "disabled"))]'
            next_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, next_button_xpath))
            )
            print(f"Clicking 'Next' button for page {page}")
            driver.execute_script("arguments[0].click();", next_button)
        else:
            # 그 외의 경우는 페이지 번호 버튼 클릭
            next_page = page + 1
            page_button_xpath = f'//a[text()="{next_page}"]'
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, page_button_xpath))
            )
            page_buttons = driver.find_elements(By.XPATH, page_button_xpath)
            if page_buttons:
                print(f"Clicking page button for page {next_page}")
                driver.execute_script("arguments[0].click();", page_buttons[0])
            else:
                print(f"Page button for page {next_page} not found.")

        # 페이지 로딩 대기 시간을 랜덤하게 설정
        time.sleep(random.uniform(4, 7))
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

# 테이블이 없을 경우 생성 (URL 및 이미지 URL 필드 포함)
table_check_query = '''
    IF OBJECT_ID('dbo.gmarket3', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.gmarket3 (
            id INT IDENTITY(1,1) PRIMARY KEY,
            title NVARCHAR(255) UNIQUE,  
            price NVARCHAR(50),
            deliver NVARCHAR(50),
            category NVARCHAR(50), -- 카테고리 필드 추가
            url NVARCHAR(MAX),  -- URL 필드 추가
            image_url NVARCHAR(MAX)  -- 이미지 URL 필드 추가
        )
    END
'''

cursor.execute(table_check_query)

# 데이터 삽입 (URL 및 이미지 URL 필드 포함)
for product in products:
    print(f"Attempting to insert product: {product['title']} - Category: {product['category']} - URL: {product['url']} - Image URL: {product['image_url']}")
    try:
        cursor.execute('''
            IF NOT EXISTS (SELECT 1 FROM dbo.gmarket3 WHERE title = ?)
            BEGIN
                INSERT INTO dbo.gmarket3 (title, price, deliver, category, url, image_url)
                VALUES (?, ?, ?, ?, ?, ?)
            END
        ''', product['title'], product['title'], product['price'], product['deliver'], product['category'], product['url'], product['image_url'])
    except pyodbc.IntegrityError as e:
        print(f"Duplicate entry found for product: {product['title']} - Skipping insertion. Error: {e}")
    except Exception as e:
        print(f"An error occurred while inserting product: {product['title']} - Error: {e}")

conn.commit()
cursor.close()
conn.close()

print("Data saved to the database successfully.")