-- =============================================================
-- Migration: unique constraints para deduplicação de leads
-- Deve rodar APÓS 001_captacao_schema.sql
-- =============================================================

-- Deduplicação por linkedin_url dentro de uma campanha
CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_campanha_linkedin
    ON captacao.leads (campanha_id, linkedin_url)
    WHERE linkedin_url IS NOT NULL;

-- Deduplicação por nome+empresa para leads sem linkedin_url
CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_campanha_nome_empresa
    ON captacao.leads (campanha_id, nome, empresa)
    WHERE linkedin_url IS NULL;
