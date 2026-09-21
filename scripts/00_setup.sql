-- Rodar uma unica vez (celula SQL ou SQL Editor), antes do 02.
-- Nao precisa mais criar Volume: os CSVs ja estao na pasta bronze/ do
-- workspace, e os scripts leem de la direto.

CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;
