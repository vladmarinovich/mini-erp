import numpy as np
import math as math
import pandas as pd


CPC = np.array([1500, 1200, 2000], dtype=float)
CVR = np.array([0.035, 0.028, 0.05], dtype=float)
AOV = np.array([120000, 130000, 110000], dtype=float)
target_order = 120

CPA = CPC / CVR
print("CPA por campaña: ", CPA)

# Presupuesto para alcanzar el target

CPA_prom = CPA.mean()
gasto_tot = CPA_prom * target_order
pesos = 1 / CPA
gasto = gasto_tot * (pesos / pesos.sum())
print(f"CPA promedio: {CPA_prom:.2f}")

print(f"Presupuesto total para alcanzar el target:  {gasto_tot:.2f}")


# PASO 2 — Proyección: Spend -> Clicks -> Conversions -> Revenue

# Clicks estimados por campaña
clicks_hat = gasto / CPC
# Conversiones estimados por campaña
conversions_hat = clicks_hat * CVR
# ingresos estimados por campaña
revenue_hat = conversions_hat * AOV

# conversiones estimadas
print("Conversiones estimadas por campaña: ", np.round(conversions_hat, 2))
print("Total conversiones estimadas: ", np.round(conversions_hat.sum(), 2))

# Pedidos por campaña

matriz_sku = np.array([[0.6, 0.2, 0.2],
                       [0.2, 0.5, 0.3],
                       [0.1, 0.2, 0.7]], dtype=float)

demand_sku = conversions_hat * matriz_sku
print("Pedidos estimados por SKU y campaña:\n", np.round(demand_sku, 2))
print("Total pedidos estimados por SKU:\n",
      np.round(demand_sku.sum(axis=0), 2))
demand_sku_total = demand_sku.sum(axis=0)

print("Total pedidos estimados SKU", demand_sku_total)
print("Total pedidos estimados en general", np.round(demand_sku.sum(), 2))


# Pedidos estimados por SKU
# Demanda diaria, desviación estándar y stock de seguridad
# Demanda diaria
demanda_diaria = demand_sku_total / 7
print("Demanda diaria estimada",
      np.round(demanda_diaria, 2))
# Desviación estándar (20% de la demanda diaria)
desviacion_estandar = 0.20 * demanda_diaria
print("Desviación estándar",
      np.round(desviacion_estandar, 2))
# Nivel de servicio 95% -> Z = 1.65
z = 1.65
L = 5  # lead time
# stock de seguridad
stock_seguridad = z * desviacion_estandar * math.sqrt(5)
print("Stock de seguridad", np.round(stock_seguridad, 2))

# Punto de  reorden (ROP)
ROP = demanda_diaria * L + stock_seguridad
print("Punto de reorden (ROP)", np.round(ROP, 2))

print("Demanda diaria por SKU", np.round(demanda_diaria, 2))
print("Desviación estándar por SKU", np.round(desviacion_estandar, 2))
print("Stock de seguridad por SKU", np.round(stock_seguridad, 2))
print("Punto de reorden (ROP) por SKU", np.round(ROP, 2))

# Simulacion de inventario

on_hand = np.array([40, 28, 26])
on_order = np.array([0, 10, 12])
backorder = np.array([0, 2, 5])
inv_pos = on_hand + on_order - backorder
print("Inventario disponible (on hand)", on_hand)
print("Inventario en orden (on order)", on_order)
print("Backorder", backorder)
print("Inventario pos (on hand + on order - backorder)", inv_pos)

# Toma de desición
desicion = np.where(inv_pos < ROP, "Ordenar", "No ordenar")
print("Decisión de orden (Ordenar/No ordenar)", desicion)
