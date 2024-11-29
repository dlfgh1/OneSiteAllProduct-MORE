import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import pyodbc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

# ChromeDriver 경로 설정
driver_path = "C:\\chromedriver.exe"
service = ChromeService(executable_path=driver_path)
chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(service=service, options=chrome_options)

# 학습된 모델과 토크나이저 로드 (로컬 저장 경로)
model_dir = 'food_classification_model'  # 학습한 모델 경로
tokenizer = AutoTokenizer.from_pretrained(model_dir)
model = AutoModelForSequenceClassification.from_pretrained(model_dir)

# 분류할 카테고리 목록
categories = ['한식', '중식', '일식', '양식', '기타']

# 미리 설정된 키워드 리스트
keywords = ['밀키트', '인분', '팩', '쿠킹박스', '인용']

# 크롤링할 카테고리별 URL 목록
category_urls = {
    '한식': 'https://www.11st.co.kr/category/DisplayCategory.tmall?method=getDisplayCategory2Depth&dispCtgrNo=1129418',
    '중식': 'https://www.11st.co.kr/category/DisplayCategory.tmall?method=getDisplayCategory2Depth&dispCtgrNo=1129421',
    '일식': 'https://www.11st.co.kr/category/DisplayCategory.tmall?method=getDisplayCategory2Depth&dispCtgrNo=1129427',
    '양식': 'https://www.11st.co.kr/category/DisplayCategory.tmall?method=getDisplayCategory2Depth&dispCtgrNo=1129419',
}

# 이미지 URL 스크래핑 함수
def scrape_image_urls(soup):
    image_urls = []
    items = soup.select('.total_listitem')
    for item in items:
        image_tag = item.select_one('div.photo_wrap img')
        if image_tag and 'src' in image_tag.attrs:
            image_urls.append(image_tag['src'])
    return image_urls

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

# 상품 정보 스크래핑 함수
def scrape_category(category_name, category_url):
    driver.get(category_url)
    time.sleep(10)
    
    products = []
    seen_titles = set()

    for page in range(1, 52):
        print(f"Scraping {category_name} - Page {page}...")

        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')

        image_urls = scrape_image_urls(soup)

        items = soup.select('.total_listitem')
        if not items:
            print(f"No items found on {category_name} - Page {page}")
            break
        
        for index, item in enumerate(items):
            title_tag = item.select_one('.info_tit')
            price_tag = item.select_one('.price_box strong.sale_price')  # 수정된 부분
            deliver_tag = item.select_one('.deliver')
            link_tag = title_tag.find('a')

            if title_tag and price_tag and deliver_tag and link_tag:
                title = title_tag.text.strip()
                price_str = price_tag.text.strip()  # 가격 문자열 가져오기
                price_int = int(price_str.replace(',', ''))  # 쉼표 제거 후 정수형으로 변환
                deliver = deliver_tag.text.strip()
                url = link_tag['href']

                if not url.startswith('http'):
                    url = 'https://www.11st.co.kr' + url

                image_url = image_urls[index] if index < len(image_urls) else None

                if any(keyword in title for keyword in keywords) and title not in seen_titles:
                    seen_titles.add(title)
                    category = classify_product(title)
                    products.append({
                        'title': title,
                        'price': price_int,  # 가격을 정수형으로 저장
                        'deliver': deliver,
                        'category': category,
                        'url': url,
                        'image_url': image_url
                    })
                    print(f"Found product: {title} - Category: {category} - URL: {url} - Image URL: {image_url} - Price: {price_str}")

        try:
            if page % 10 == 0:
                next_button_xpath = '//*[@id="list_paging"]/a[@class="next"]'
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, next_button_xpath))
                )
                driver.execute_script("arguments[0].click();", next_button)
            else:
                next_page = page + 1
                page_button_xpath = f'//*[@id="list_paging"]/span/a[text()="{next_page}"]'
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, page_button_xpath))
                )
                driver.execute_script("arguments[0].click();", next_button)

            time.sleep(5)
        except Exception as e:
            print(f"An error occurred on {category_name} - Page {page}: {e}")
            break

    return products


# 데이터베이스에 저장
def save_to_database(products, table_name):
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=admin.ch2qqkwuwon8.ap-northeast-2.rds.amazonaws.com,1433;'
        'DATABASE=dlfgh;'  # 데이터베이스 이름 수정
        'UID=admin;'  # SQL Server 사용자 아이디
        'PWD=dnflrkwhr72;'  # SQL Server 비밀번호
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # 테이블 이름을 대괄호로 감싸서 '11bunga' 테이블 생성
    table_check_query = f'''
        IF OBJECT_ID('dbo.eleven6', 'U') IS NULL
        BEGIN
            CREATE TABLE [dbo].eleven6 (
                id INT IDENTITY(1,1) PRIMARY KEY,
                title NVARCHAR(255) UNIQUE,  
                price NVARCHAR(max),
                deliver NVARCHAR(max),
                category NVARCHAR(max),
                url NVARCHAR(MAX),
                image_url NVARCHAR(max)
            )
        END
    '''
    
    cursor.execute(table_check_query)

    for product in products:
        try:
            cursor.execute(f'''
                IF NOT EXISTS (SELECT 1 FROM [dbo].eleven6 WHERE title = ?)
                BEGIN
                    INSERT INTO [dbo].eleven6 (title, price, deliver, category, url, image_url)
                    VALUES (?, ?, ?, ?, ?, ?)
                END
            ''', product['title'], product['title'], product['price'], product['deliver'], product['category'], product['url'], product['image_url'])
        except pyodbc.IntegrityError as e:
            print(f"Duplicate entry found for product: {product['title']} - Skipping insertion.")
        except Exception as e:
            print(f"An error occurred while inserting product: {product['title']} - Error: {e}")

    conn.commit()
    cursor.close()
    conn.close()

# 전체 카테고리 스크래핑 및 저장
for category_name, category_url in category_urls.items():
    products = scrape_category(category_name, category_url)
    save_to_database(products, '11bunga')  # '11bunga' 테이블 이름 전달

driver.quit()
print("Data saved to the database successfully.")