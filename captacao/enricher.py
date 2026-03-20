"""
Enriquecimento de leads via scraping HTTP direto + Anthropic LLM.

Substitui o Firecrawl por httpx + BeautifulSoup — sem dependência externa de serviço.

Fluxo por lead:
  1. Se tem site_url → httpx scrapa → BeautifulSoup extrai texto limpo → LLM extrai contexto
  2. Se não tem site_url ou scraping falhou → LLM usa web search tool como fallback

Os system prompts são lidos do banco (captacao.prompts) pelo scheduler
e passados explicitamente — não há prompts hardcoded aqui.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Optional

import anthropic
import httpx
from bs4 import BeautifulSoup
import json
from tenacity import retry, stop_after_attempt, wait_exponential

from captacao.models import ContextoEmpresa, LeadEnriquecido, LeadRaw

logger = logging.getLogger(__name__)

MAX_CHARS = 16000  # ~4000 tokens — suficiente para a maioria dos sites


def _truncar_markdown(markdown: str, max_chars: int = MAX_CHARS) -> str:
    """Mantém compatibilidade de truncamento para conteúdo textual longo."""
    if len(markdown) <= max_chars:
        return markdown
    logger.debug("Texto truncado de %d para %d chars", len(markdown), max_chars)
    return markdown[:max_chars] + "\n\n[conteúdo truncado]"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


def _html_para_texto(html: str) -> str:
    """Extrai texto limpo do HTML removendo scripts, estilos e tags desnecessárias."""
    soup = BeautifulSoup(html, "html.parser")

    # Remove elementos que não têm conteúdo útil
    for tag in soup(["script", "style", "nav", "footer", "header", "aside", "iframe", "noscript"]):
        tag.decompose()

    # Extrai texto com separadores de linha
    texto = soup.get_text(separator="\n", strip=True)

    # Remove linhas em branco excessivas
    linhas = [l for l in texto.splitlines() if l.strip()]
    texto_limpo = "\n".join(linhas)

    return _truncar_markdown(texto_limpo)


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=3, max=15), reraise=False)
def _scrape_site(url: str) -> Optional[str]:
    """Faz scraping HTTP do site e retorna texto limpo. Retorna None se falhar."""
    try:
        with httpx.Client(
            headers=HEADERS,
            timeout=15.0,
            follow_redirects=True,
            verify=False,  # alguns sites de móveis têm SSL mal configurado
        ) as client:
            response = client.get(url)
            response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        if "text/html" not in content_type and "text/plain" not in content_type:
            logger.warning("Content-type não suportado para %s: %s", url, content_type)
            return None

        texto = _html_para_texto(response.text)
        if not texto or len(texto) < 100:
            logger.warning("Texto extraído muito curto para %s (%d chars)", url, len(texto))
            return None

        logger.info("Site scrapeado: %s (%d chars)", url, len(texto))
        return texto

    except httpx.HTTPStatusError as e:
        logger.warning("HTTP %d para %s", e.response.status_code, url)
        return None
    except Exception as e:
        logger.warning("Scraping falhou para %s: %s", url, e)
        return None


def _parse_contexto_json(raw_text: str, lead: LeadRaw) -> ContextoEmpresa:
    """Parseia o JSON do LLM com fallback robusto."""
    text = raw_text.strip()
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
        return ContextoEmpresa(resumo=f"{lead.empresa} atua no setor de móveis planejados.")


def _enriquecer_com_texto(
    cliente_llm: anthropic.Anthropic,
    lead: LeadRaw,
    texto_site: str,
    system_prompt: str,
) -> ContextoEmpresa:
    """Chama o LLM com o texto do site para extrair contexto estruturado."""
    prompt = (
        f"Empresa: {lead.empresa}\n"
        f"Cargo do contato: {lead.cargo or 'não informado'}\n"
        f"Cidade: {lead.cidade or 'não informada'}\n\n"
        f"Conteúdo do site:\n{texto_site}"
    )
    response = cliente_llm.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=system_prompt,
        messages=[{"role": "user", "content": prompt}],
    )
    raw_text = response.content[0].text
    ctx = _parse_contexto_json(raw_text, lead)
    ctx.site_scrapeado = True
    return ctx


def _enriquecer_com_websearch(
    cliente_llm: anthropic.Anthropic,
    lead: LeadRaw,
    system_prompt: str,
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
        system=system_prompt,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": prompt}],
    )
    raw_text = ""
    for block in response.content:
        if block.type == "text":
            raw_text = block.text
            break
    if not raw_text:
        logger.warning("LLM não retornou texto para web search de '%s'", lead.empresa)
        return ContextoEmpresa(resumo=f"{lead.empresa} atua no setor de móveis planejados.")
    return _parse_contexto_json(raw_text, lead)


def enriquecer_lead(
    lead: LeadRaw,
    system_prompt: str,
    anthropic_key: Optional[str] = None,
) -> LeadEnriquecido:
    """
    Enriquece um LeadRaw com contexto da empresa.

    Estratégia:
      1. Se lead.site_url → httpx + BeautifulSoup → LLM extrai contexto
      2. Fallback → LLM com web search tool

    Args:
        lead:          LeadRaw a enriquecer
        system_prompt: Prompt do banco (etapa='enricher')
        anthropic_key: Chave Anthropic (fallback: ANTHROPIC_API_KEY)
    """
    ant_key = anthropic_key or os.environ["ANTHROPIC_API_KEY"]
    cliente_llm = anthropic.Anthropic(api_key=ant_key)
    contexto: Optional[ContextoEmpresa] = None

    if lead.site_url:
        logger.info("Scraping site de '%s': %s", lead.empresa, lead.site_url)
        texto = _scrape_site(lead.site_url)
        if texto:
            contexto = _enriquecer_com_texto(cliente_llm, lead, texto, system_prompt)

    if contexto is None:
        logger.info("Sem site ou scraping falhou para '%s' — usando web search", lead.empresa)
        contexto = _enriquecer_com_websearch(cliente_llm, lead, system_prompt)

    return LeadEnriquecido(raw=lead, contexto=contexto, mensagem_cold="")


def enriquecer_batch(
    leads: list[LeadRaw],
    system_prompt: str,
    **kwargs,
) -> list[LeadEnriquecido]:
    """Enriquece uma lista de leads, pulando os que falharem."""
    enriquecidos = []
    for i, lead in enumerate(leads, 1):
        logger.info("[%d/%d] Enriquecendo: %s — %s", i, len(leads), lead.nome, lead.empresa)
        try:
            enriquecido = enriquecer_lead(lead, system_prompt=system_prompt, **kwargs)
            enriquecidos.append(enriquecido)
        except Exception:
            logger.exception("Falha ao enriquecer lead '%s' — pulando", lead.empresa)
    logger.info("Enriquecimento concluído: %d/%d leads", len(enriquecidos), len(leads))
    return enriquecidos
