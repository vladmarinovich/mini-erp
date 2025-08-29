-- =========================
-- 00_schema.sql  (Esquema)
-- =========================

-- Extensiones útiles (idempotentes)
create extension if not exists "uuid-ossp";

-- =========================
-- Tablas núcleo
-- =========================

-- Campañas
create table if not exists public.campaigns (
  campaign_id   bigserial primary key,
  campaign_name text not null unique,
  budget        numeric not null default 0,
  created_at    timestamptz not null default now()
);

-- Productos
create table if not exists public.products (
  product_id   bigserial primary key,
  sku          text not null unique,
  product_name text not null,
  sale_price   numeric not null check (sale_price >= 0),
  unit_cost    numeric not null check (unit_cost >= 0),
  created_at   timestamptz not null default now()
);

-- Resultados de simulación por adset (una corrida = un run_ts)
create table if not exists public.adset_simulation (
  run_ts       text not null,
  campaign_id  bigint not null references public.campaigns(campaign_id) on delete cascade,
  adset_name   text  not null,
  product_id   bigint not null references public.products(product_id) on delete cascade,
  sku          text  not null,
  budget       numeric not null default 0,
  cpc          numeric not null default 0,
  cvr          numeric not null default 0,
  aov          numeric not null default 0,
  clicks       numeric not null default 0,
  conversions  numeric not null default 0,
  revenue      numeric not null default 0,
  margin       numeric not null default 0,
  created_at   timestamptz not null default now()
);

-- Índices recomendados
create index if not exists idx_adset_sim_run_ts       on public.adset_simulation(run_ts);
create index if not exists idx_adset_sim_campaign     on public.adset_simulation(campaign_id);
create index if not exists idx_adset_sim_product      on public.adset_simulation(product_id);
create index if not exists idx_adset_sim_campaign_prod on public.adset_simulation(campaign_id, product_id);

-- Vista simple (opcional) del último run
create or replace view public.v_adset_sim_last as
select *
from public.adset_simulation
where run_ts = (select max(run_ts) from public.adset_simulation);
