# summary-service/main.py
import os
import asyncpg
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Summary Service")

DB_USER = os.getenv("POSTGRES_USER", "myuser")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "mypassword")
DB_NAME = os.getenv("POSTGRES_DB", "mydb")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

@app.get("/summary")
async def get_summary():
    conn = await asyncpg.connect(
        user=DB_USER, password=DB_PASS, database=DB_NAME, host=DB_HOST, port=DB_PORT
    )

    # Query: get total per category
    rows = await conn.fetch("""
        SELECT c.name AS category, COALESCE(SUM(t.amount), 0) AS total
        FROM transactions t
        JOIN categories c ON t.category_id = c.id
        WHERE date_trunc('month', t.txn_date) = date_trunc('month', now())
        GROUP BY c.name
        ORDER BY c.name;
    """)

    await conn.close()

    # Build summary
    by_category = {r["category"]: float(r["total"]) for r in rows}
    total_spend = sum(by_category.values())

    return JSONResponse({
        "total_spend": round(total_spend, 2),
        "by_category": by_category
    })

@app.get("/")
def root():
    return {"message": "Summary service is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
