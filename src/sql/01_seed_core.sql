-- =========================
-- 01_seed_core.sql (Seeds)
-- =========================

-- Campañas base (total 700)
insert into public.campaigns (campaign_name, budget) values
  ('PMAX', 300),
  ('Video', 200),
  ('Busqueda', 200)
on conflict (campaign_name) do update
  set budget = excluded.budget;

-- Top 10 productos (precios en unidades “reales” reducidas)
insert into public.products (sku, product_name, sale_price, unit_cost) values
  ('SKU001','Dog Food Seco Adulto 10kg',        110,  80),
  ('SKU002','Dog Food Seco Cachorro 8kg',       105,  70),
  ('SKU005','Cat Food Seco Adulto 5kg',          98,  65),
  ('SKU006','Cat Food Seco Esterilizado 5kg',   120,  80),
  ('SKU015','Ropa Perro Hoodie Talla M',         55,  32),
  ('SKU016','Ropa Perro Impermeable Talla L',    65,  38),
  ('SKU017','Cama Perro Ortopédica L',          250, 150),
  ('SKU018','Cama Gato Donut M',                200, 120),
  ('SKU019','Arena Gato Aglomerante 10kg',       75,  40),
  ('SKU020','Arena Gato Sílica 7.6L',            60,  30)
on conflict (sku) do update
  set product_name = excluded.product_name,
      sale_price   = excluded.sale_price,
      unit_cost    = excluded.unit_cost;