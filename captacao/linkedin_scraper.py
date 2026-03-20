"""
Captação de leads B2B via Apify — LinkedIn People/Company Search.

Actor usado: "apify/linkedin-profile-scraper" ou equivalente gratuito.
Filtra por cargo (dono/sócio/diretor) e setor (móveis planejados / marcenaria).

Referência: https://docs.apify.com/api/client/python
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from apify_client import ApifyClient
from tenacity import retry, stop_after_attempt, wait_exponential

from captacao.models import FonteLead, LeadRaw

logger = logging.getLogger(__name__)

# Actor público gratuito para busca de pessoas no LinkedIn
# Alternativa: "anchor/linkedin-profile-scraper" (free tier)
ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "apify/linkedin-profile-scraper")

# Cargos-alvo: donos e decisores de lojas e marcenarias
CARGOS_ALVO = [
    "dono",
    "proprietário",
    "sócio",
    "diretor",
    "CEO",
    "fundador",
    "gerente geral",
]

# Keywords de busca para o nicho
KEYWORDS_NICHO = [
    "loja móveis planejados",
    "móveis planejados",
    "marcenaria",
    "marcenaria sob medida",
    "móveis sob medida",
]


def _normalizar_telefone(raw: Optional[str]) -> Optional[str]:
    """Remove caracteres não numéricos e garante DDI 55 para BR."""
    if not raw:
        return None
    digits = "".join(c for c in raw if c.isdigit())
    if not digits:
        return None
    if not digits.startswith("55"):
        digits = "55" + digits
    return digits


def _extrair_estado(location: Optional[str]) -> Optional[str]:
    """Tenta extrair UF a partir do campo location do LinkedIn."""
    if not location:
        return None
    # Formato comum: "São Paulo, Brasil" ou "Curitiba, Paraná, Brasil"
    partes = [p.strip() for p in location.split(",")]
    if len(partes) >= 2:
        return partes[-2]  # penúltimo = estado geralmente
    return None


def _item_para_lead_raw(item: dict) -> Optional[LeadRaw]:
    """Converte um item do dataset Apify em LeadRaw. Retorna None se inválido."""
    nome = item.get("fullName") or item.get("name") or item.get("firstName", "")
    if not nome:
        logger.debug("Item sem nome, ignorando: %s", item.get("linkedinUrl"))
        return None

    empresa = (
        item.get("companyName")
        or item.get("currentCompany")
        or (item.get("positions") or [{}])[0].get("companyName")
        or ""
    )

    cargo = (
        item.get("headline")
        or item.get("currentPosition")
        or (item.get("positions") or [{}])[0].get("title")
    )

    location = item.get("location") or item.get("city") or ""
    cidade_raw = location.split(",")[0].strip() if location else None

    return LeadRaw(
        nome=nome.strip(),
        empresa=empresa.strip(),
        cargo=cargo.strip() if cargo else None,
        cidade=cidade_raw,
        estado=_extrair_estado(location),
        telefone=_normalizar_telefone(item.get("phone")),
        linkedin_url=item.get("linkedinUrl") or item.get("url"),
        site_url=item.get("companyWebsite") or item.get("website"),
        fonte=FonteLead.LINKEDIN_APIFY,
        apify_raw=item,
    )


def _filtrar_cargo(lead: LeadRaw) -> bool:
    """Retorna True se o cargo do lead é um decisor-alvo."""
    if not lead.cargo:
        return True  # sem cargo, mantém para não perder leads
    cargo_lower = lead.cargo.lower()
    return any(c.lower() in cargo_lower for c in CARGOS_ALVO)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=10, max=60),
    reraise=True,
)
def _run_actor(client: ApifyClient, run_input: dict) -> list[dict]:
    """Executa o actor Apify e retorna os itens do dataset."""
    logger.info("Iniciando actor %s com input: %s", ACTOR_ID, run_input)
    run = client.actor(ACTOR_ID).call(run_input=run_input)

    if run is None or run.get("status") != "SUCCEEDED":
        status = run.get("status") if run else "None"
        raise RuntimeError(f"Actor finalizou com status inesperado: {status}")

    items = client.dataset(run["defaultDatasetId"]).list_items().items
    logger.info("Actor retornou %d itens", len(items))
    return items


def capturar_leads(
    keyword: str,
    estado: Optional[str] = None,
    max_results: int = 100,
    apify_token: Optional[str] = None,
) -> list[LeadRaw]:
    """
    Executa o scraper do LinkedIn via Apify e retorna lista de LeadRaw.

    Args:
        keyword:      Termo de busca (ex: "loja móveis planejados")
        estado:       Filtro geográfico opcional (ex: "São Paulo")
        max_results:  Limite de resultados
        apify_token:  Token da API Apify (fallback: var APIFY_TOKEN)

    Returns:
        Lista de LeadRaw filtrados por cargo-alvo
    """
    token = apify_token or os.environ["APIFY_TOKEN"]
    client = ApifyClient(token)

    search_query = keyword
    if estado:
        search_query = f"{keyword} {estado}"

    run_input = {
        "searchUrl": f"https://www.linkedin.com/search/results/people/?keywords={search_query}",
        "maxResults": max_results,
        "proxyConfiguration": {"useApifyProxy": True},
    }

    raw_items = _run_actor(client, run_input)

    leads = []
    for item in raw_items:
        lead = _item_para_lead_raw(item)
        if lead and _filtrar_cargo(lead):
            leads.append(lead)

    logger.info(
        "Captação concluída: %d/%d leads após filtro de cargo",
        len(leads),
        len(raw_items),
    )
    return leads


def capturar_batch(
    estados: list[str],
    keywords: Optional[list[str]] = None,
    max_por_combinacao: int = 50,
    apify_token: Optional[str] = None,
) -> list[LeadRaw]:
    """
    Executa captação para múltiplas combinações keyword × estado.
    Deduplica por linkedin_url.

    Args:
        estados:               Lista de UFs alvo (ex: ["São Paulo", "Paraná"])
        keywords:              Lista de keywords (default: KEYWORDS_NICHO)
        max_por_combinacao:    Limite por combinação
        apify_token:           Token Apify

    Returns:
        Lista deduplicada de LeadRaw
    """
    kws = keywords or KEYWORDS_NICHO[:2]  # padrão: 2 keywords para economizar cota
    vistos: set[str] = set()
    todos: list[LeadRaw] = []

    for keyword in kws:
        for estado in estados:
            logger.info("Buscando: '%s' em '%s'", keyword, estado)
            try:
                lote = capturar_leads(
                    keyword=keyword,
                    estado=estado,
                    max_results=max_por_combinacao,
                    apify_token=apify_token,
                )
                for lead in lote:
                    chave = lead.linkedin_url or f"{lead.nome}|{lead.empresa}"
                    if chave not in vistos:
                        vistos.add(chave)
                        todos.append(lead)
            except Exception:
                logger.exception(
                    "Falha ao capturar '%s' em '%s' — continuando", keyword, estado
                )

    logger.info("Batch concluído: %d leads únicos", len(todos))
    return todos
