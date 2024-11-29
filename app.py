from flask import Flask, jsonify
from flask_cors import CORS
import pyodbc

app = Flask(__name__)
CORS(app)

# 데이터베이스 연결 설정
def get_db_connection():
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=DESKTOP-UCBAAAM;'
        'DATABASE=dlfgh;'
        'Trusted_Connection=yes;'
    )
    return pyodbc.connect(conn_str)

@app.route('/api/products/<category>', methods=['GET'])
def get_products_by_category(category):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT title, price, category, url, image_url FROM dbo.coupang WHERE category = ?", category)
    products = cursor.fetchall()
    conn.close()

    return jsonify([{
        'title': row[0],
        'price': row[1],
        'category': row[2],
        'url': row[3],
        'imageUrl': row[4]  # 이미지 URL 추가
    } for row in products])

if __name__ == '__main__':
    app.run(debug=True)
