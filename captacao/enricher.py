"""
Enriquecimento de leads via Firecrawl (self-hosted) + Anthropic LLM.

Fluxo por lead:
  1. Se tem site_url → Firecrawl scrapa → markdown limpo
  2. LLM recebe markdown + perfil do lead → extrai ContextoEmpresa
  3. Se não tem site_url → LLM usa web search tool como fallback
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

import anthropic
from firecrawl import FirecrawlApp
from tenacity import retry, stop_after_attempt, wait_exponential

from captacao.models import ContextoEmpresa, LeadEnriquecido, LeadRaw

logger = logging.getLogger(__name__)

# Limite de tokens do markdown para não explodir o context window
MAX_MARKDOWN_TOKENS = 4000

SYSTEM_ENRICHER = """\
Você é um analista especializado em pequenas e médias empresas do setor de \
móveis planejados e marcenarias no Brasil.

Sua tarefa: analisar o conteúdo do site de uma empresa e extrair um contexto \
estruturado para uso em prospecção B2B.

Responda APENAS com JSON válido, sem markdown, sem explicações.
Schema obrigatório:
{
  "resumo": "2-3 frases descrevendo o negócio",
  "segmento_exato": "ex: marcenaria sob medida premium, loja de móveis planejados populares",
  "diferenciais": ["diferencial 1", "diferencial 2"],
  "produtos_destaque": ["produto 1", "produto 2"],
  "tom_da_marca": "ex: sofisticado, familiar, técnico, moderno"
}

Se algum campo não for identificável, use null.
"""

SYSTEM_ENRICHER_WEBSEARCH = """\
Você é um analista especializado em pequenas e médias empresas do setor de \
móveis planejados e marcenarias no Brasil.

Use a ferramenta de busca web para pesquisar a empresa e extrair contexto \
para prospecção B2B.

Responda APENAS com JSON válido, sem markdown, sem explicações.
Schema obrigatório:
{
  "resumo": "2-3 frases descrevendo o negócio",
  "segmento_exato": "ex: marcenaria sob medida premium",
  "diferenciais": ["diferencial 1"],
  "produtos_destaque": ["produto 1"],
  "tom_da_marca": "ex: sofisticado"
}

Se algum campo não for identificável, use null.
"""


def _truncar_markdown(markdown: str, max_chars: int = MAX_MARKDOWN_TOKENS * 4) -> str:
    """Trunca o markdown para evitar context overflow no LLM."""
    if len(markdown) <= max_chars:
        return markdown
    logger.debug("Markdown truncado de %d para %d chars", len(markdown), max_chars)
    return markdown[:max_chars] + "\n\n[conteúdo truncado]"


def _parse_contexto_json(raw_text: str, lead: LeadRaw) -> ContextoEmpresa:
    """Parseia o JSON do LLM com fallback robusto."""
    text = raw_text.strip()
    # Remove possível markdown code block
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    try:
        data = json.loads(text)
        return ContextoEmpresa(
            resumo=data.get("resumo") or f"Empresa {lead.empresa} do setor de móveis.",
            segmento_exato=data.get("segmento_exato"),
            diferenciais=data.get("diferenciais") or [],
            produtos_destaque=data.get("produtos_destaque") or [],
            tom_da_marca=data.get("tom_da_marca"),
        )
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning("Falha ao parsear JSON do enricher: %s — usando fallback", e)
        return ContextoEmpresa(
            resumo=f"{lead.empresa} atua no setor de móveis planejados.",
        )


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=5, max=30), reraise=False)
def _scrape_site(firecrawl: FirecrawlApp, url: str) -> Optional[str]:
    """Scrapa o site via Firecrawl self-hosted. Retorna markdown ou None."""
    try:
        result = firecrawl.scrape_url(url, params={"formats": ["markdown"]})
        markdown = (
            result.get("markdown")
            if isinstance(result, dict)
            else getattr(result, "markdown", None)
        )
        if not markdown:
            logger.warning("Firecrawl retornou sem markdown para %s", url)
            return None
        return _truncar_markdown(markdown)
    except Exception as e:
        logger.warning("Firecrawl falhou para %s: %s", url, e)
        return None


def _enriquecer_com_site(
    cliente_llm: anthropic.Anthropic,
    lead: LeadRaw,
    markdown_site: str,
) -> ContextoEmpresa:
    """Chama o LLM com o conteúdo do site para extrair contexto."""
    prompt = (
        f"Empresa: {lead.empresa}\n"
        f"Cargo do contato: {lead.cargo or 'não informado'}\n"
        f"Cidade: {lead.cidade or 'não informada'}\n\n"
        f"Conteúdo do site (markdown):\n{markdown_site}"
    )
    response = cliente_llm.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=SYSTEM_ENRICHER,
        messages=[{"role": "user", "content": prompt}],
    )
    raw_text = response.content[0].text
    ctx = _parse_contexto_json(raw_text, lead)
    ctx.site_scrapeado = True
    ctx.markdown_site = markdown_site
    return ctx


def _enriquecer_com_websearch(
    cliente_llm: anthropic.Anthropic,
    lead: LeadRaw,
) -> ContextoEmpresa:
    """Fallback: LLM usa web search tool para pesquisar a empresa."""
    prompt = (
        f"Pesquise a empresa '{lead.empresa}' localizada em "
        f"'{lead.cidade or ''} {lead.estado or 'Brasil'}' "
        f"do setor de móveis planejados/marcenaria. "
        f"Extraia o contexto no formato JSON solicitado."
    )
    response = cliente_llm.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=SYSTEM_ENRICHER_WEBSEARCH,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": prompt}],
    )
    # Extrai bloco de texto (pode vir após tool_use)
    raw_text = ""
    for block in response.content:
        if block.type == "text":
            raw_text = block.text
            break
    if not raw_text:
        logger.warning("LLM não retornou texto para web search de '%s'", lead.empresa)
        return ContextoEmpresa(
            resumo=f"{lead.empresa} atua no setor de móveis planejados.",
        )
    return _parse_contexto_json(raw_text, lead)


def enriquecer_lead(
    lead: LeadRaw,
    firecrawl_url: Optional[str] = None,
    firecrawl_token: Optional[str] = None,
    anthropic_key: Optional[str] = None,
) -> LeadEnriquecido:
    """
    Enriquece um LeadRaw com contexto da empresa e retorna LeadEnriquecido.

    Estratégia:
      1. Se lead.site_url → Firecrawl scrape → LLM extrai contexto
      2. Fallback → LLM com web search tool

    Args:
        lead:             LeadRaw a enriquecer
        firecrawl_url:    URL base do Firecrawl self-hosted
                          (fallback: var FIRECRAWL_URL)
        firecrawl_token:  Token da API Firecrawl
                          (fallback: var FIRECRAWL_API_KEY)
        anthropic_key:    Chave Anthropic
                          (fallback: var ANTHROPIC_API_KEY)

    Returns:
        LeadEnriquecido com contexto e mensagem vazia
        (mensagem é preenchida pelo message_generator)
    """
    fc_url = firecrawl_url or os.environ.get("FIRECRAWL_URL", "http://firecrawl_firecrawl:3002")
    fc_token = firecrawl_token or os.environ.get("FIRECRAWL_API_KEY", "self-hosted")
    ant_key = anthropic_key or os.environ["ANTHROPIC_API_KEY"]

    firecrawl = FirecrawlApp(api_key=fc_token, api_url=fc_url)
    cliente_llm = anthropic.Anthropic(api_key=ant_key)

    contexto: Optional[ContextoEmpresa] = None

    if lead.site_url:
        logger.info("Scraping site de '%s': %s", lead.empresa, lead.site_url)
        markdown = _scrape_site(firecrawl, lead.site_url)
        if markdown:
            contexto = _enriquecer_com_site(cliente_llm, lead, markdown)

    if contexto is None:
        logger.info(
            "Sem site ou Firecrawl falhou para '%s' — usando web search",
            lead.empresa,
        )
        contexto = _enriquecer_com_websearch(cliente_llm, lead)

    return LeadEnriquecido(
        raw=lead,
        contexto=contexto,
        mensagem_cold="",  # será preenchida pelo message_generator
    )


def enriquecer_batch(
    leads: list[LeadRaw],
    **kwargs,
) -> list[LeadEnriquecido]:
    """Enriquece uma lista de leads, pulando os que falharem."""
    enriquecidos = []
    for i, lead in enumerate(leads, 1):
        logger.info("[%d/%d] Enriquecendo: %s — %s", i, len(leads), lead.nome, lead.empresa)
        try:
            enriquecido = enriquecer_lead(lead, **kwargs)
            enriquecidos.append(enriquecido)
        except Exception:
            logger.exception("Falha ao enriquecer lead '%s' — pulando", lead.empresa)
    logger.info("Enriquecimento concluído: %d/%d leads", len(enriquecidos), len(leads))
    return enriquecidos
