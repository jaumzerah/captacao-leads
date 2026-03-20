"""
Orquestrador do batch diário de captação.

Uso:
  python scheduler.py                        # usa variáveis de ambiente
  python scheduler.py --campanha moveis-sul  # usa configuração do banco
  python scheduler.py --dry-run              # não publica na fila
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime

import structlog
from dotenv import load_dotenv

load_dotenv()

from captacao.enricher import enriquecer_batch
from captacao.linkedin_scraper import capturar_batch
from captacao.message_generator import gerar_mensagens_batch
from captacao.publisher import publicar_leads

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.BoundLogger,
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger()

# Fallbacks via env (usados quando não há --campanha)
ESTADOS_ENV = os.getenv("CAPTACAO_ESTADOS", "São Paulo,Paraná,Santa Catarina").split(",")
KEYWORDS_ENV = os.getenv("CAPTACAO_KEYWORDS", "").split(",") if os.getenv("CAPTACAO_KEYWORDS") else None
MAX_ENV = int(os.getenv("CAPTACAO_MAX_POR_COMBO", "50"))
CAMPANHA_ENV = os.getenv("CAPTACAO_CAMPANHA", f"batch-{datetime.utcnow().strftime('%Y%m%d')}")
NICHO_ENV = os.getenv("CAPTACAO_NICHO_SLUG", "moveis-planejados")


async def _carregar_config_banco(campanha_slug: str) -> tuple[dict, str, str]:
    """
    Carrega configuração da campanha e prompts do banco.
    Retorna (config_campanha, prompt_enricher, prompt_message_generator).
    """
    from captacao.db import get_pool, get_campanha, get_prompt

    pool = await get_pool()
    try:
        campanha = await get_campanha(pool, campanha_slug)
        nicho_slug = campanha["nicho_slug"]

        prompt_enricher = await get_prompt(pool, nicho_slug, "enricher")
        prompt_generator = await get_prompt(pool, nicho_slug, "message_generator")

        log.info(
            "config.carregada",
            campanha=campanha_slug,
            nicho=nicho_slug,
            estados=campanha["estados"],
            max_por_combo=campanha["max_por_combo"],
        )
        return campanha, prompt_enricher, prompt_generator
    finally:
        await pool.close()


async def _salvar_leads_banco(leads_com_mensagem, campanha_id: int, ids_publicados: set[int]) -> None:
    """Persiste leads no banco com status correto após o batch."""
    from captacao.db import get_pool, salvar_lead, marcar_publicado

    pool = await get_pool()
    try:
        for i, lead in enumerate(leads_com_mensagem):
            status = "publicado" if i in ids_publicados else "mensagem_gerada"
            lead_id = await salvar_lead(pool, campanha_id, lead, status)
            if status == "publicado":
                await marcar_publicado(pool, lead_id)
    finally:
        await pool.close()


def rodar_batch(
    campanha_slug: str | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Executa o pipeline completo de captação.

    Se campanha_slug fornecido: busca config do banco.
    Caso contrário: usa variáveis de ambiente como fallback.
    """
    inicio = datetime.utcnow()
    usa_banco = campanha_slug is not None

    # ── Configuração ──────────────────────────────────────────────
    if usa_banco:
        campanha_cfg, prompt_enricher, prompt_generator = asyncio.run(
            _carregar_config_banco(campanha_slug)
        )
        estados = campanha_cfg["estados"]
        keywords = campanha_cfg["keywords"] or None
        max_por_combo = campanha_cfg["max_por_combo"]
        campanha_id = campanha_cfg["id"]
        campanha_tag = campanha_slug
    else:
        prompt_enricher = None   # será passado explicitamente nos testes
        prompt_generator = None  # idem
        estados = ESTADOS_ENV
        keywords = KEYWORDS_ENV
        max_por_combo = MAX_ENV
        campanha_id = None
        campanha_tag = CAMPANHA_ENV

        # Tenta carregar prompts do banco mesmo sem --campanha
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            try:
                from captacao.db import get_pool, get_prompt

                async def _prompts():
                    pool = await get_pool()
                    try:
                        pe = await get_prompt(pool, NICHO_ENV, "enricher")
                        pg = await get_prompt(pool, NICHO_ENV, "message_generator")
                        return pe, pg
                    finally:
                        await pool.close()

                prompt_enricher, prompt_generator = asyncio.run(_prompts())
                log.info("prompts.carregados_do_banco", nicho=NICHO_ENV)
            except Exception as e:
                log.warning("prompts.fallback_hardcoded", erro=str(e))

        # Fallback hardcoded mínimo se banco indisponível
        if not prompt_enricher:
            prompt_enricher = (
                "Extraia contexto da empresa em JSON com campos: "
                "resumo, segmento_exato, diferenciais, produtos_destaque, tom_da_marca. "
                "Responda APENAS JSON válido."
            )
        if not prompt_generator:
            prompt_generator = (
                "Escreva mensagem WhatsApp de cold outreach B2B para dono de loja de "
                "móveis planejados. Máx 3 parágrafos, tom consultivo, mencione algo "
                "específico da empresa, termine com pergunta aberta. Só o texto."
            )

    log.info("batch.inicio", estados=estados, campanha=campanha_tag, dry_run=dry_run)

    # ── 1. Captação via Apify ──────────────────────────────────────
    log.info("batch.etapa", etapa="captacao_linkedin")
    leads_raw = capturar_batch(
        estados=estados,
        keywords=keywords,
        max_por_combinacao=max_por_combo,
    )

    if not leads_raw:
        log.warning("batch.sem_leads")
        return {"status": "vazio", "leads_captados": 0}

    log.info("batch.captados", total=len(leads_raw))

    # ── 2. Enriquecimento ─────────────────────────────────────────
    log.info("batch.etapa", etapa="enriquecimento")
    leads_enriquecidos = enriquecer_batch(leads_raw, system_prompt=prompt_enricher)

    # ── 3. Geração de mensagens ───────────────────────────────────
    log.info("batch.etapa", etapa="geracao_mensagens")
    leads_com_mensagem = gerar_mensagens_batch(leads_enriquecidos, system_prompt=prompt_generator)

    # ── 4. Publicação ─────────────────────────────────────────────
    publicados = 0
    falhas = 0
    ids_publicados: set[int] = set()

    if dry_run:
        log.info("batch.dry_run", total=len(leads_com_mensagem))
        for lead in leads_com_mensagem[:3]:
            log.info(
                "batch.preview",
                nome=lead.raw.nome,
                empresa=lead.raw.empresa,
                mensagem=lead.mensagem_cold[:100] + "...",
            )
    else:
        log.info("batch.etapa", etapa="publicacao_rabbitmq")
        publicados, falhas = publicar_leads(leads_com_mensagem, campanha=campanha_tag)
        ids_publicados = set(range(publicados))  # índices dos publicados com sucesso

    # ── 5. Persistência no banco ──────────────────────────────────
    if usa_banco and not dry_run and campanha_id:
        try:
            asyncio.run(_salvar_leads_banco(leads_com_mensagem, campanha_id, ids_publicados))
            log.info("batch.leads_salvos_no_banco", total=len(leads_com_mensagem))
        except Exception:
            log.exception("batch.erro_ao_salvar_no_banco")

    duracao = (datetime.utcnow() - inicio).total_seconds()
    metricas = {
        "status": "ok",
        "campanha": campanha_tag,
        "leads_captados": len(leads_raw),
        "leads_enriquecidos": len(leads_enriquecidos),
        "leads_publicados": publicados,
        "falhas": falhas,
        "duracao_segundos": round(duracao, 1),
    }
    log.info("batch.concluido", **metricas)
    return metricas


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch de captação de leads")
    parser.add_argument("--campanha", type=str, default=None, help="Slug da campanha no banco")
    parser.add_argument("--dry-run", action="store_true", help="Não publica na fila")
    args = parser.parse_args()

    resultado = rodar_batch(campanha_slug=args.campanha, dry_run=args.dry_run)
    sys.exit(0 if resultado["status"] in ("ok", "vazio") else 1)
