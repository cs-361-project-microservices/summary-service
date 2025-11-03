# summary-service/main.py
import os
from datetime import date, datetime, timedelta
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import asyncpg

app = FastAPI()

DB_USER = os.getenv("POSTGRES_USER", "myuser")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "mypassword")
DB_NAME = os.getenv("POSTGRES_DB", "mydb")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

async def get_conn():
    return await asyncpg.connect(
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )

def month_bounds(d: date):
    start = d.replace(day=1)
    # next month start
    if start.month == 12:
        next_start = start.replace(year=start.year + 1, month=1, day=1)
    else:
        next_start = start.replace(month=start.month + 1, day=1)
    return start, next_start

async def totals_between(conn, start: date, end: date):
    row = await conn.fetchrow(
        """
        SELECT
          COALESCE(SUM(CASE WHEN c.kind = 'expense' THEN t.amount ELSE 0 END), 0) AS expenses,
          COALESCE(SUM(CASE WHEN c.kind = 'income'  THEN t.amount ELSE 0 END), 0) AS income,
          COALESCE(SUM(CASE WHEN c.kind = 'savings' THEN t.amount ELSE 0 END), 0) AS savings
        FROM transactions t
        JOIN categories c ON t.category_id = c.id
        WHERE t.txn_date >= $1 AND t.txn_date < $2
        """,
        start, end
    )
    expenses = float(row["expenses"])
    income  = float(row["income"])
    savings = float(row["savings"])
    net = income - expenses - savings
    return {
        "income": income,
        "expenses": expenses,
        "savings": savings,
        "net": net
    }

async def by_category_between(conn, start: date, end: date):
    rows = await conn.fetch(
        """
        SELECT c.name AS category, c.kind AS kind, COALESCE(SUM(t.amount),0) AS total
        FROM transactions t
        JOIN categories c ON t.category_id = c.id
        WHERE t.txn_date >= $1 AND t.txn_date < $2
        GROUP BY c.name, c.kind
        ORDER BY total DESC, c.name
        """,
        start, end
    )
    return [
        {"category": r["category"], "kind": r["kind"], "total": float(r["total"])}
        for r in rows
    ]

@app.get("/summary")
async def summary_this_month():
    today = date.today()
    start, end = month_bounds(today)
    conn = await get_conn()
    try:
        totals = await totals_between(conn, start, end)
        breakdown = await by_category_between(conn, start, end)
        return JSONResponse({
            "period": {"start": str(start), "end_exclusive": str(end)},
            "totals": totals,
            "by_category": breakdown
        })
    finally:
        await conn.close()

@app.get("/summary/range")
async def summary_range(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD (exclusive)")
):
    # parse dates safely
    try:
        start_d = datetime.strptime(start, "%Y-%m-%d").date()
        end_d   = datetime.strptime(end,   "%Y-%m-%d").date()
    except ValueError:
        return JSONResponse({"error": "Invalid date format. Use YYYY-MM-DD."}, status_code=400)

    if end_d <= start_d:
        return JSONResponse({"error": "end must be after start"}, status_code=400)

    conn = await get_conn()
    try:
        totals = await totals_between(conn, start_d, end_d)
        breakdown = await by_category_between(conn, start_d, end_d)
        return JSONResponse({
            "period": {"start": str(start_d), "end_exclusive": str(end_d)},
            "totals": totals,
            "by_category": breakdown
        })
    finally:
        await conn.close()
