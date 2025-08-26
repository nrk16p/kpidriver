import os
from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv


# Load env vars (local dev only; Vercel uses dashboard env)
load_dotenv()


DB_CONFIG = {
"host": os.getenv("DB_HOST", "127.0.0.1"),
"user": os.getenv("DB_USER", "root"),
"password": os.getenv("DB_PASSWORD", ""),
"database": os.getenv("DB_NAME", "be_database"),
}


# Create a small pool at import-time so serverless can reuse warm instances
cnxpool = None
try:
    cnxpool = pooling.MySQLConnectionPool(pool_name="kpi_pool", pool_size=3, **DB_CONFIG)
except Exception as e:
    # Pool creation can fail on cold boot without DB access; we'll fall back to per-request connect
    cnxpool = None


app = Flask(__name__)


KPI_COLS = [f"kpi_{i}" for i in range(1, 10)]


@app.get("/kpi")
def get_kpi():
    driver_id = request.args.get("driver_id", type=str)
    year      = request.args.get("year", type=int)

    if not driver_id or not year:
        return jsonify({
            "error": "Missing required query params: driver_id, year"
        }), 400

    sql = f"""
        SELECT month, {', '.join(KPI_COLS)}
        FROM driverkpi
        WHERE driver_id = %s AND year = %s
        ORDER BY month
    """

    try:
        conn = cnxpool.get_connection()
        cur = conn.cursor()
        cur.execute(sql, (driver_id, year))
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except mysql.connector.Error as e:
        return jsonify({"error": f"MySQL error: {e}"}), 500

    if not rows:
        return jsonify({
            "driver": driver_id,
            "data": {},
            "message": "No KPI rows found for given driver_id/year"
        }), 404

    # Build: { "11": [k1..k9], "12": [...], "1": [...] }
    data = {}
    for row in rows:
        month = str(row[0])
        kpis  = [int(x) if x is not None else 0 for x in row[1:1+len(KPI_COLS)]]
        data[month] = kpis

    return jsonify({
        "driver": driver_id,
        "data": data
    }), 200

if __name__ == "__main__":
    # For local dev: python app.py
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)), debug=True)