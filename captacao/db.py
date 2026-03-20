"""
Acesso ao banco PostgreSQL (sdr_agents) via asyncpg.

Gerencia prompts configuráveis e log de leads do pipeline de captação.
Hostname interno no Swarm: postgres_postgres
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

import asyncpg

from captacao.models import LeadEnriquecido

logger = logging.getLogger(__name__)


async def get_pool(dsn: Optional[str] = None) -> asyncpg.Pool:
    """
    Cria e retorna um pool de conexões asyncpg.

    Args:
        dsn: PostgreSQL DSN (fallback: var DATABASE_URL)

    Returns:
        asyncpg.Pool com min_size=1, max_size=5 (adequado para container batch)
    """
    url = dsn or os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(
        url,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )
    logger.info("Pool PostgreSQL criado: %s", url.split("@")[-1])
    return pool


async def get_prompt(pool: asyncpg.Pool, nicho_slug: str, etapa: str) -> str:
    """
    Busca o system prompt ativo para o nicho+etapa.

    Args:
        pool:        Pool asyncpg
        nicho_slug:  Slug do nicho (ex: 'moveis-planejados')
        etapa:       'enricher' ou 'message_generator'

    Returns:
        Texto do system prompt

    Raises:
        ValueError se nicho não existir ou não houver prompt ativo
    """
    row = await pool.fetchrow(
        """
        SELECT p.system
        FROM captacao.prompts p
        JOIN captacao.nichos n ON n.id = p.nicho_id
        WHERE n.slug = $1
          AND p.etapa = $2
          AND p.ativo = true
        """,
        nicho_slug,
        etapa,
    )
    if row is None:
        raise ValueError(
            f"Nenhum prompt ativo encontrado para nicho='{nicho_slug}' etapa='{etapa}'"
        )
    return row["system"]


async def get_campanha(pool: asyncpg.Pool, campanha_slug: str) -> dict:
    """
    Busca a configuração de uma campanha pelo slug.

    Args:
        pool:           Pool asyncpg
        campanha_slug:  Slug da campanha (ex: 'moveis-sul-2025-06')

    Returns:
        Dict com id, nicho_id, slug, nome, estados, keywords, max_por_combo, ativa

    Raises:
        ValueError se a campanha não existir
    """
    row = await pool.fetchrow(
        """
        SELECT c.id, c.nicho_id, c.slug, c.nome,
               c.estados, c.keywords, c.max_por_combo, c.ativa,
               n.slug AS nicho_slug
        FROM captacao.campanhas c
        JOIN captacao.nichos n ON n.id = c.nicho_id
        WHERE c.slug = $1
        """,
        campanha_slug,
    )
    if row is None:
        raise ValueError(f"Campanha não encontrada: '{campanha_slug}'")
    return dict(row)


async def salvar_lead(
    pool: asyncpg.Pool,
    campanha_id: int,
    lead: LeadEnriquecido,
    status: str,
    erro_msg: Optional[str] = None,
) -> int:
    """
    Faz upsert do lead na tabela captacao.leads.

    Chave de deduplicação:
      - Se tem linkedin_url: (campanha_id, linkedin_url)
      - Fallback: (campanha_id, nome, empresa)

    Args:
        pool:        Pool asyncpg
        campanha_id: ID da campanha
        lead:        LeadEnriquecido com contexto e mensagem
        status:      Status atual no pipeline
        erro_msg:    Mensagem de erro opcional

    Returns:
        ID do registro inserido/atualizado
    """
    raw = lead.raw
    ctx = lead.contexto

    contexto_json = json.dumps(
        ctx.model_dump(exclude={"markdown_site"}),
        ensure_ascii=False,
    )

    # Upsert por linkedin_url se disponível, senão por nome+empresa
    if raw.linkedin_url:
        row = await pool.fetchrow(
            """
            INSERT INTO captacao.leads (
                campanha_id, nome, empresa, cargo, cidade, estado,
                telefone, linkedin_url, site_url, fonte,
                contexto_resumo, contexto_segmento, contexto_json, site_scrapeado,
                mensagem_cold, status, erro_msg, processado_em
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10,
                $11, $12, $13::jsonb, $14,
                $15, $16, $17, NOW()
            )
            ON CONFLICT (campanha_id, linkedin_url)
            DO UPDATE SET
                contexto_resumo   = EXCLUDED.contexto_resumo,
                contexto_segmento = EXCLUDED.contexto_segmento,
                contexto_json     = EXCLUDED.contexto_json,
                site_scrapeado    = EXCLUDED.site_scrapeado,
                mensagem_cold     = EXCLUDED.mensagem_cold,
                status            = EXCLUDED.status,
                erro_msg          = EXCLUDED.erro_msg,
                processado_em     = NOW()
            RETURNING id
            """,
            campanha_id, raw.nome, raw.empresa, raw.cargo, raw.cidade, raw.estado,
            raw.telefone, raw.linkedin_url, raw.site_url, raw.fonte.value,
            ctx.resumo, ctx.segmento_exato, contexto_json, ctx.site_scrapeado,
            lead.mensagem_cold, status, erro_msg,
        )
    else:
        row = await pool.fetchrow(
            """
            INSERT INTO captacao.leads (
                campanha_id, nome, empresa, cargo, cidade, estado,
                telefone, site_url, fonte,
                contexto_resumo, contexto_segmento, contexto_json, site_scrapeado,
                mensagem_cold, status, erro_msg, processado_em
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9,
                $10, $11, $12::jsonb, $13,
                $14, $15, $16, NOW()
            )
            ON CONFLICT (campanha_id, nome, empresa)
            DO UPDATE SET
                contexto_resumo   = EXCLUDED.contexto_resumo,
                contexto_segmento = EXCLUDED.contexto_segmento,
                contexto_json     = EXCLUDED.contexto_json,
                site_scrapeado    = EXCLUDED.site_scrapeado,
                mensagem_cold     = EXCLUDED.mensagem_cold,
                status            = EXCLUDED.status,
                erro_msg          = EXCLUDED.erro_msg,
                processado_em     = NOW()
            RETURNING id
            """,
            campanha_id, raw.nome, raw.empresa, raw.cargo, raw.cidade, raw.estado,
            raw.telefone, raw.site_url, raw.fonte.value,
            ctx.resumo, ctx.segmento_exato, contexto_json, ctx.site_scrapeado,
            lead.mensagem_cold, status, erro_msg,
        )

    lead_id = row["id"]
    logger.debug("Lead salvo id=%d status=%s empresa=%s", lead_id, status, raw.empresa)
    return lead_id


async def marcar_publicado(pool: asyncpg.Pool, lead_id: int) -> None:
    """Atualiza o status do lead para 'publicado' e registra o timestamp."""
    await pool.execute(
        """
        UPDATE captacao.leads
        SET status = 'publicado', publicado_em = NOW()
        WHERE id = $1
        """,
        lead_id,
    )
    logger.debug("Lead id=%d marcado como publicado", lead_id)
