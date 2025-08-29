-- =========================
-- 02_seed_demo.sql (Demo)
-- =========================

-- Productos extra (si quieres ampliar catálogo)
insert into public.products (sku, product_name, sale_price, unit_cost) values
  ('SKU021', 'Shampoo Perro Hipoalergénico 500ml', 26, 15),
  ('SKU022', 'Toallitas Húmedas Mascotas x80',     18, 10)
on conflict (sku) do update
  set product_name = excluded.product_name,
      sale_price   = excluded.sale_price,
      unit_cost    = excluded.unit_cost;