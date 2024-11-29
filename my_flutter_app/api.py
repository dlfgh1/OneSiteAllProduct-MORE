from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import pyodbc
import requests
from PIL import Image
from io import BytesIO

app = Flask(__name__)
CORS(app)

# RDS 연결 설정 함수
def get_db_connection():
    try:
        conn_str = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=admin.ch2qqkwuwon8.ap-northeast-2.rds.amazonaws.com,1433;'  # RDS 엔드포인트
            'DATABASE=dlfgh;'    # 데이터베이스 이름
            'UID=admin;'          # 사용자 이름
            'PWD=dnflrkwhr72;'    # 비밀번호
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# 이미지 URL 처리 함수 (플랫폼별로 다르게 처리)
def process_image_url(platform, image_url):
    if platform == 'coupang':
        return f"https:{image_url}" if not image_url.startswith('http') else image_url
    elif platform == 'eleven':
        return image_url.split('?')[0]
    elif platform == 'marketkurly':
        return f"{image_url}.jpg" if not image_url.endswith('.jpg') else image_url
    else:
        return image_url  # auction, gmarket은 그대로 유지

# 상품 데이터를 조회하는 API 엔드포인트
@app.route('/products', methods=['GET'])
def get_products():
    keyword = request.args.get('keyword')
    platforms = request.args.getlist('platforms')

    platform_map = {
        "11st": "eleven",
        "kurly": "marketkurly"
    }
    platforms = [platform_map.get(p, p) for p in platforms]

    conn = get_db_connection()
    if conn is None:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    try:
        cursor = conn.cursor()
        if not platforms:
            platforms = ['auction', 'eleven', 'marketkurly', 'gmarket', 'coupang']

        products = []

        for platform in platforms:
            query = f"SELECT title, price, category, url, image_url, deliver FROM {platform} WHERE title LIKE ?"
            params = [f'%{keyword}%']

            try:
                cursor.execute(query, params)
                for row in cursor.fetchall():
                    image_url = process_image_url(platform, row[4])
                    delivery_info = '무료배송' if row[5] in ['0', '0원'] else row[5] or '배송비 정보 없음'
                    products.append({
                        'title': row[0],
                        'price': row[1],
                        'category': row[2],
                        'url': row[3],
                        'image_url': image_url,
                        'platform': platform,
                        'delivery': delivery_info
                    })

            except Exception as e:
                print(f"Error executing query on platform {platform}: {e}")

        cursor.close()
        conn.close()
        return jsonify(products)

    except Exception as e:
        print(f"Error during query execution: {e}")
        return jsonify({'error': 'Query execution failed', 'details': str(e)}), 500

# 가격대별 추천 상품 API 엔드포인트
@app.route('/recommended_products', methods=['GET'])
def get_recommended_products():
    price_range = request.args.get('price_range')

    if not price_range:
        return jsonify({'error': 'price_range is required'}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    try:
        cursor = conn.cursor()
        platforms = ['auction', 'eleven', 'marketkurly', 'gmarket', 'coupang']
        products = []

        # 가격대별 SQL 필터 설정
        price_map = {
            "under_1": "price < 10000",
            "1_to_2": "price >= 10000 AND price < 20000",
            "2_to_3": "price >= 20000 AND price < 30000",
            "3_to_4": "price >= 30000 AND price < 40000",
            "4_to_7": "price >= 40000 AND price < 70000",
            "over_7": "price >= 70000"
        }
        price_filter = price_map.get(price_range)

        if not price_filter:
            return jsonify({'error': 'Invalid price range'}), 400

        for platform in platforms:
            query = f"""
                SELECT TOP 10 title, price, category, url, image_url, deliver 
                FROM {platform}
                WHERE {price_filter}
            """
            try:
                cursor.execute(query)
                for row in cursor.fetchall():
                    image_url = process_image_url(platform, row[4])
                    delivery_info = '무료배송' if row[5] in ['0', '0원'] else row[5] or '배송비 정보 없음'
                    products.append({
                        'title': row[0],
                        'price': row[1],
                        'category': row[2],
                        'url': row[3],
                        'image_url': image_url,
                        'platform': platform,
                        'delivery': delivery_info
                    })

            except Exception as e:
                print(f"Error executing query on platform {platform}: {e}")

        cursor.close()
        conn.close()
        return jsonify(products)

    except Exception as e:
        print(f"Error fetching recommended products: {e}")
        return jsonify({'error': 'Query execution failed', 'details': str(e)}), 500

# 비슷한 상품을 추천하는 API 엔드포인트
@app.route('/similar_products', methods=['GET'])
def get_similar_products():
    category = request.args.get('category')
    title_keyword = request.args.get('title_keyword')

    if not category and not title_keyword:
        return jsonify({'error': 'category or title_keyword is required'}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    try:
        cursor = conn.cursor()
        platforms = ['auction', 'eleven', 'marketkurly', 'gmarket', 'coupang']
        similar_products = []
        unique_products = set()

        for platform in platforms:
            query = f"""
                SELECT TOP 5 title, price, category, url, image_url, deliver
                FROM {platform}
                WHERE (category = ? OR title LIKE ?) AND title <> ?
            """
            params = [category, f'%{title_keyword}%', title_keyword]

            try:
                cursor.execute(query, params)
                for row in cursor.fetchall():
                    unique_key = (row[0], platform)
                    if unique_key not in unique_products:
                        unique_products.add(unique_key)
                        image_url = process_image_url(platform, row[4])
                        delivery_info = '무료배송' if row[5] in ['0', '0원'] else row[5] or '배송비 정보 없음'
                        similar_products.append({
                            'title': row[0],
                            'price': row[1],
                            'category': row[2],
                            'url': row[3],
                            'image_url': image_url,
                            'platform': platform,
                            'delivery': delivery_info
                        })

            except Exception as e:
                print(f"Error executing query on platform {platform}: {e}")

        cursor.close()
        conn.close()
        return jsonify(similar_products[:10])

    except Exception as e:
        print(f"Error fetching similar products: {e}")
        return jsonify({'error': 'Query execution failed', 'details': str(e)}), 500

# 이미지 변환 API
@app.route('/convert_image', methods=['GET'])
def convert_image():
    image_url = request.args.get('image_url')

    if not image_url:
        return jsonify({'error': 'image_url is required'}), 400

    try:
        response = requests.get(image_url)
        img = Image.open(BytesIO(response.content))

        img_io = BytesIO()
        img.save(img_io, 'PNG')
        img_io.seek(0)

        return send_file(img_io, mimetype='image/png')

    except Exception as e:
        print(f"Error converting image: {e}")
        return jsonify({'error': 'Failed to convert image', 'details': str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
