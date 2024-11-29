import os
from flask import Flask, jsonify, request
from flask_cors import CORS
import openai
import pyodbc
import numpy as np
from numpy import dot
from numpy.linalg import norm
import json

app = Flask(__name__)
CORS(app)

# OpenAI API Key 설정
openai.api_key = os.getenv("OPENAI_API_KEY")  # 환경 변수에서 API 키 가져오기

# 플랫폼별 테이블 매핑
platform_table_map = {
    '11st': 'elevenprices',
    'gmarket': 'gmarketprices',
    'coupang': 'coupangprices',
    'auction': 'auctionprices',
    'marketkurly': 'marketkurlyprices'
}

# RDS 연결 설정 함수
def get_db_connection():
    try:
        conn_str = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=admin.ch2qqkwuwon8.ap-northeast-2.rds.amazonaws.com,1433;'
            'DATABASE=dlfgh;'
            'UID=admin;'
            'PWD=dnflrkwhr72;'
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# 이미지 URL 처리 함수
def process_image_url(platform, image_url):
    # 쿠팡, 마켓컬리: http/https 처리
    if platform in ['coupang', 'marketkurly']:
        return f"https:{image_url}" if not image_url.startswith('http') else image_url
    # 지마켓, 옥션: http/https 처리
    elif platform in ['gmarket', 'auction']:
        return f"http:{image_url}" if not image_url.startswith('http') else image_url
    # 11번가: 쿼리스트링 제거
    elif platform == 'eleven':
        return image_url.split('?')[0] if '?' in image_url else image_url
    # 기본 처리
    return image_url


# 상품 클릭 시 count 증가 API
@app.route('/increment_count', methods=['POST'])
def increment_count():
    data = request.json
    product_id = data.get('id')
    increment_value = data.get('value', 1)

    print(f"API 호출: /increment_count")  # API 호출 로그
    print(f"수신 데이터: id={product_id}, increment={increment_value}")  # 데이터 확인

    # 입력값 유효성 검사
    if not product_id or not isinstance(product_id, str):
        print("잘못된 요청: product_id가 유효하지 않음")
        return jsonify({'error': 'Invalid or missing product_id'}), 400

    try:
        conn = get_db_connection()
        if conn is None:
            print("데이터베이스 연결 실패")
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor()
        # count 증가 SQL 실행
        cursor.execute(
            f"""
            UPDATE your_table_name_here
            SET count = COALESCE(count, 0) + ?
            WHERE id = ?
            """,
            (increment_value, product_id)
        )
        conn.commit()

        if cursor.rowcount == 0:
            print(f"상품 {product_id}을 찾을 수 없음")
            return jsonify({'error': 'Product not found'}), 404

        print(f"상품 {product_id}의 count가 {increment_value}만큼 증가")
        return jsonify({'message': 'Count updated successfully'}), 200
    except Exception as e:
        print(f"서버 오류: {e}")
        return jsonify({'error': 'Internal server error', 'details': str(e)}), 500
    finally:
        conn.close()


# 상품 데이터를 조회하는 API
@app.route('/products', methods=['GET'])
def get_products():
    household_type = request.args.get('household_type')
    keyword = request.args.get('keyword', '')
    platform = request.args.get('platform')
    min_price = request.args.get('min_price')
    max_price = request.args.get('max_price')
    sort_order = request.args.get('sort_order', 'asc')

    conn = get_db_connection()
    if conn is None:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    try:
        cursor = conn.cursor()
        table_name = platform_table_map.get(platform, 'product_prices')
        query = f"SELECT title, day1_price, deliver, url, image_url, platform FROM {table_name} WHERE title LIKE ?"
        params = [f'%{keyword}%']

        # 1인가구 및 다인가구 필터링
        if household_type == "1인":
            query += " AND (title LIKE ? OR title LIKE ? OR title LIKE ? OR title LIKE ?)"
            params.extend(['%1인%', '%1팩%', '%1인분%', '%2인분%'])
        elif household_type == "다인":
            query += """
        AND (title LIKE ? OR title LIKE ? OR title LIKE ? OR title LIKE ? OR title LIKE ?)
        AND day1_price >= ?
            """
            params.extend(['%3인%', '%4인%', '%대가족%', '%3팩%', '%5팩%', 30000])


        # 가격 필터 추가
        if min_price:
            query += " AND day1_price >= ?"
            params.append(min_price)
        if max_price:
            query += " AND day1_price <= ?"
            params.append(max_price)

        # 정렬 추가
        query += f" ORDER BY day1_price {'ASC' if sort_order == 'asc' else 'DESC'}"

        cursor.execute(query, params)
        products = []
        for row in cursor.fetchall():
            delivery_info = '무료배송' if row[2] in ['0', '0원'] else row[2]
            product_data = {
                'title': row[0],
                'price': f"{row[1]}원",
                'delivery': delivery_info,
                'url': row[3],
                'image_url': row[4],
                'platform': row[5],
            }
            products.append(product_data)

        return jsonify(products)

    except Exception as e:
        print(f"Error during product query: {e}")
        return jsonify({'error': 'Failed to fetch products', 'details': str(e)}), 500
    finally:
        conn.close()

# 가격 추이 데이터 조회 API
@app.route('/price_history', methods=['GET'])
def get_price_history():
    title = request.args.get('title')
    platform = request.args.get('platform')

    if not title or not platform:
        return jsonify({'error': 'Both title and platform are required'}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT day7_price, day6_price, day5_price, day4_price, day3_price, day2_price, day1_price
            FROM product_prices
            WHERE title = ? AND platform = ?
        """, (title, platform))

        row = cursor.fetchone()
        if row:
            price_history = [float(price) if price is not None else 0.0 for price in row]
            return jsonify({'price_history': price_history})
        else:
            return jsonify({'error': 'Price history not found'}), 404

    except Exception as e:
        print(f"Error fetching price history: {e}")
        return jsonify({'error': 'Failed to fetch price history', 'details': str(e)}), 500
    finally:
        conn.close()


@app.route('/popular_products', methods=['GET'])
def get_popular_products():
    limit = int(request.args.get('limit', 10))  # 기본값 10개 반환

    conn = get_db_connection()
    if conn is None:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT title, day1_price, deliver, url, image_url, platform, count
            FROM product_prices
            ORDER BY count DESC  -- 클릭 횟수로 정렬
            OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """, (limit,))
        products = []
        for row in cursor.fetchall():
            delivery_info = '무료배송' if row[2] in ['0', '0원'] else (row[2] or '배송 정보 없음')
            product_data = {
                'title': row[0],
                'price': f"{row[1]}원" if row[1] is not None else '',
                'delivery': delivery_info,
                'url': row[3],
                'image_url': process_image_url(row[5], row[4]) if row[4] else '',
                'platform': row[5],
                'count': row[6],
            }
            products.append(product_data)

        return jsonify(products)

    except Exception as e:
        return jsonify({'error': 'Failed to fetch popular products', 'details': str(e)}), 500
    finally:
        conn.close()

from difflib import SequenceMatcher

@app.route('/recommend_similar', methods=['GET'])
def recommend_similar():
    title = request.args.get('title')

    if not title:
        return jsonify([])  # 제목이 없으면 빈 리스트 반환

    conn = get_db_connection()
    if conn is None:
        return jsonify([])

    try:
        cursor = conn.cursor()
        recommendations = []

        # 플랫폼별로 관련 상품 추천
        for platform_key, table_name in platform_table_map.items():
            query = f"""
                SELECT title, day1_price, image_url, url, platform
                FROM {table_name}
            """
            cursor.execute(query)
            products = cursor.fetchall()

            # 유사도 계산
            similar_products = []
            for row in products:
                product_title = row[0]
                similarity = SequenceMatcher(None, title, product_title).ratio()

                # 유사도 임계값을 설정하여 필터링
                if similarity > 0.5:  # 유사도가 50% 이상인 경우만
                    similar_products.append({
                        "title": product_title,
                        "price": row[1],
                        "image_url": row[2],
                        "url": row[3],
                        "platform": row[4],
                        "similarity": similarity
                    })

            # 유사도 순으로 정렬 후 상위 2개 선택
            similar_products = sorted(similar_products, key=lambda x: x["similarity"], reverse=True)
            recommendations.extend(similar_products[:2])

        # 총 10개만 반환
        return jsonify(recommendations[:10])

    except Exception as e:
        print(f"Error fetching recommendations: {e}")
        return jsonify([])
    finally:
        conn.close()

# 코사인 유사도 계산
def cosine_similarity(vec1, vec2):
    norm_vec1 = norm(vec1)
    norm_vec2 = norm(vec2)
    if norm_vec1 == 0 or norm_vec2 == 0:
        return 0.0
    return dot(vec1, vec2) / (norm_vec1 * norm_vec2)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
