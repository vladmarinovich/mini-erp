import duckdb

# Abre tu archivo .duckdb
con = duckdb.connect("warehouse.duckdb")

# Ver las tablas
print(con.execute("SHOW TABLES").fetchdf())

# Leer los primeros registros de campañas
df = con.execute("SELECT * FROM camp LIMIT 5").fetchdf()
print(df)

# === 1) Gasto por campaña (ordenado) ===
q_budget = """
SELECT campaign_id, SUM(total_budget) AS total_budget
FROM camp
GROUP BY campaign_id
ORDER BY total_budget DESC;
"""
print("\n[Gasto por campaña]")
print(con.execute(q_budget).fetchdf())

# === 2) CPA y ROAS por campaña (ordenado por ROAS) ===
q_roas_cpa = """
SELECT campaign_id,
       total_budget,
       total_revenue,
       ROUND(total_budget/NULLIF(total_conversions,0), 2) AS cpa,
       ROUND(total_revenue/NULLIF(total_budget,0), 2)      AS roas,
       ROUND(total_conversions/NULLIF(total_clicks,0), 4)  AS conv_rate
FROM camp
ORDER BY roas DESC;
"""
print("\n[CPA y ROAS por campaña]")
print(con.execute(q_roas_cpa).fetchdf())

# === 3) Top 10 adsets por ROAS ===
q_top_adsets_roas = """
SELECT campaign_id, adset_name,
       ROUND(total_revenue/NULLIF(total_budget,0), 2) AS roas,
       total_budget, total_revenue, total_conversions
FROM adset
ORDER BY roas DESC
LIMIT 10;
"""
print("\n[Top 10 adsets por ROAS]")
print(con.execute(q_top_adsets_roas).fetchdf())

# === 4) Peores 10 adsets por CPA (solo si hubo conversiones) ===
q_worst_adsets_cpa = """
SELECT campaign_id, adset_name,
       ROUND(total_budget/NULLIF(total_conversions,0), 2) AS cpa,
       total_conversions, total_budget
FROM adset
WHERE total_conversions > 0
ORDER BY cpa DESC
LIMIT 10;
"""
print("\n[Peores 10 adsets por CPA]")
print(con.execute(q_worst_adsets_cpa).fetchdf())

# === 5) Top 10 productos por margen total ===
q_top_products_margin = """
SELECT sku,
       ROUND(total_margin, 2) AS total_margin,
       total_revenue, total_conversions
FROM prod
ORDER BY total_margin DESC
LIMIT 10;
"""
print("\n[Top 10 productos por margen]")
print(con.execute(q_top_products_margin).fetchdf())

# === (opcional) % de presupuesto por campaña ===
q_pct_budget = """
WITH tot AS (SELECT SUM(total_budget) AS t FROM camp)
SELECT c.campaign_id,
       c.total_budget,
       ROUND(100.0 * c.total_budget / t.t, 2) AS pct_budget
FROM camp c, tot t
ORDER BY pct_budget DESC;
"""
print("\n[% de presupuesto por campaña]")
print(con.execute(q_pct_budget).fetchdf())
