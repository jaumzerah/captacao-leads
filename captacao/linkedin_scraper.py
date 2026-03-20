"""
Captação de leads B2B por múltiplos provedores.

Provedores suportados:
- ddgs (padrão): busca gratuita via DuckDuckGo Search
- apify: actor harvestapi/linkedin-profile-search
"""

from __future__ import annotations

import logging
import os
import re
from urllib.parse import urlparse
from typing import Optional

from apify_client import ApifyClient
from tenacity import retry, stop_after_attempt, wait_exponential

from captacao.models import FonteLead, LeadRaw

logger = logging.getLogger(__name__)

ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "harvestapi/linkedin-profile-search")
PROVIDER = os.getenv("CAPTACAO_PROVIDER", "ddgs").lower()

# Cargos-alvo: decisores de lojas e marcenarias
CARGOS_ALVO = [
    "dono",
    "proprietário",
    "sócio",
    "diretor",
    "CEO",
    "fundador",
    "gerente geral",
    "owner",
    "founder",
]

# Keywords de busca para o nicho
KEYWORDS_NICHO = [
    "loja móveis planejados",
    "marcenaria",
    "móveis sob medida",
    "marcenaria sob medida",
]

# Mapeamento de estados BR para GeoURN do LinkedIn
# IDs obtidos via LinkedIn API — os mais relevantes para BR
GEO_URNS_BR: dict[str, str] = {
    "São Paulo": "106057199",
    "Rio de Janeiro": "104769234",
    "Minas Gerais": "102278238",
    "Paraná": "102081502",
    "Santa Catarina": "102681177",
    "Rio Grande do Sul": "101620748",
    "Bahia": "106765561",
    "Goiás": "106765748",
    "Espírito Santo": "104375742",
    "Pernambuco": "106573891",
}


def _normalizar_url(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return None
        if not parsed.netloc:
            return None
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return None


def _empresa_from_text(texto: str, fallback_url: Optional[str]) -> str:
    if texto:
        limpo = re.sub(r"\s+", " ", texto).strip()
        for sep in ("|", "-", "—"):
            if sep in limpo:
                candidato = limpo.split(sep, 1)[0].strip()
                if len(candidato) >= 3:
                    return candidato
        if len(limpo) >= 3:
            return limpo[:120]
    if fallback_url:
        netloc = urlparse(fallback_url).netloc
        return netloc.replace("www.", "")
    return "Empresa não identificada"


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
    partes = [p.strip() for p in location.split(",")]
    if len(partes) >= 2:
        return partes[-2]
    return None


def _item_para_lead_raw(item: dict) -> Optional[LeadRaw]:
    """Converte um item do dataset Apify em LeadRaw."""
    nome = (
        item.get("fullName")
        or item.get("name")
        or f"{item.get('firstName', '')} {item.get('lastName', '')}".strip()
    )
    if not nome or nome.strip() == "":
        logger.debug("Item sem nome, ignorando: %s", item.get("linkedinUrl"))
        return None

    empresa = (
        item.get("companyName")
        or item.get("currentCompany")
        or (((item.get("positions") or [{}])[0]).get("companyName"))
        or ""
    )

    cargo = (
        item.get("headline")
        or item.get("currentPosition")
        or item.get("title")
        or (((item.get("positions") or [{}])[0]).get("title"))
    )

    # O actor pode retornar location como dict {"city": ..., "state": ...} ou string
    location_raw = item.get("location") or item.get("geoLocationName") or ""
    if isinstance(location_raw, dict):
        cidade_raw = location_raw.get("city") or location_raw.get("name") or ""
        location = ", ".join(filter(None, [
            location_raw.get("city"),
            location_raw.get("state"),
            location_raw.get("country"),
        ]))
    else:
        location = location_raw
        cidade_raw = location.split(",")[0].strip() if location else None

    return LeadRaw(
        nome=nome.strip(),
        empresa=empresa.strip(),
        cargo=cargo.strip() if cargo else None,
        cidade=cidade_raw,
        estado=_extrair_estado(location),
        telefone=_normalizar_telefone(item.get("phone")),
        linkedin_url=item.get("linkedinUrl") or item.get("profileUrl") or item.get("url"),
        site_url=item.get("companyWebsite") or item.get("website"),
        fonte=FonteLead.LINKEDIN_APIFY,
        apify_raw=item,
    )


def _resultado_web_para_lead_raw(item: dict, estado: Optional[str]) -> Optional[LeadRaw]:
    """Converte resultado de web search em LeadRaw para enriquecimento posterior."""
    url = _normalizar_url(item.get("href") or item.get("url") or "")
    if not url:
        return None

    titulo = item.get("title") or ""
    descricao = item.get("body") or item.get("snippet") or ""
    empresa = _empresa_from_text(titulo, url)
    nome = "Contato Comercial"

    telefone_match = re.search(r"(?:\+?55)?\s?\(?\d{2}\)?\s?9?\d{4}-?\d{4}", descricao)
    telefone = _normalizar_telefone(telefone_match.group(0)) if telefone_match else None

    return LeadRaw(
        nome=nome,
        empresa=empresa,
        cargo=None,
        cidade=None,
        estado=estado,
        telefone=telefone,
        linkedin_url=None,
        site_url=url,
        fonte=FonteLead.LINKEDIN_APIFY,
        apify_raw=item,
    )


def _filtrar_cargo(lead: LeadRaw) -> bool:
    """Retorna True se o cargo do lead é um decisor-alvo."""
    if not lead.cargo:
        return True
    cargo_lower = lead.cargo.lower()
    return any(c.lower() in cargo_lower for c in CARGOS_ALVO)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=10, max=60),
    reraise=True,
)
def _run_actor(client: ApifyClient, run_input: dict) -> list[dict]:
    """Executa o actor Apify e retorna os itens do dataset."""
    logger.info("Iniciando actor %s", ACTOR_ID)
    run = client.actor(ACTOR_ID).call(run_input=run_input)

    if run is None or run.get("status") != "SUCCEEDED":
        status = run.get("status") if run else "None"
        raise RuntimeError(f"Actor finalizou com status: {status}")

    items = client.dataset(run["defaultDatasetId"]).list_items().items
    logger.info("Actor retornou %d itens", len(items))
    return items


def _capturar_leads_ddgs(
    keyword: str,
    estado: Optional[str] = None,
    max_results: int = 100,
) -> list[LeadRaw]:
    """Busca leads por pesquisa web gratuita (DuckDuckGo)."""
    try:
        from duckduckgo_search import DDGS
    except Exception as exc:
        raise RuntimeError(
            "Dependência 'duckduckgo-search' não instalada. "
            "Adicione no ambiente para usar CAPTACAO_PROVIDER=ddgs."
        ) from exc

    query_parts = [keyword]
    if estado:
        query_parts.append(f'"{estado}"')
    query_parts.extend(["site:.br", "(móveis OR marcenaria OR planejados)"])
    query = " ".join(query_parts)

    logger.info("Buscando DDGS: %s", query)
    resultados: list[dict] = []
    with DDGS() as ddgs:
        for item in ddgs.text(query, region="br-pt", safesearch="off", max_results=max_results):
            resultados.append(item)

    leads: list[LeadRaw] = []
    for item in resultados:
        lead = _resultado_web_para_lead_raw(item, estado)
        if lead:
            leads.append(lead)

    logger.info("DDGS retornou %d leads brutos", len(leads))
    return leads


def _capturar_leads_apify(
    keyword: str,
    estado: Optional[str] = None,
    max_results: int = 100,
    apify_token: Optional[str] = None,
) -> list[LeadRaw]:
    """Executa o scraper do LinkedIn via Apify e retorna lista de LeadRaw."""
    token = apify_token or os.environ["APIFY_TOKEN"]
    client = ApifyClient(token)

    run_input: dict = {
        "query": keyword,
        "currentJobTitles": CARGOS_ALVO[:5],
        "takePages": max(1, max_results // 25),
        "scrapeType": "short",
    }

    if estado:
        run_input["locations"] = [estado]
        logger.info("Filtro de localização: %s", estado)

    raw_items = _run_actor(client, run_input)

    leads = []
    for item in raw_items:
        lead = _item_para_lead_raw(item)
        if lead and _filtrar_cargo(lead):
            leads.append(lead)

    logger.info(
        "Captação Apify: %d/%d leads após filtro de cargo",
        len(leads),
        len(raw_items),
    )
    return leads


def capturar_leads(
    keyword: str,
    estado: Optional[str] = None,
    max_results: int = 100,
    apify_token: Optional[str] = None,
    provider: Optional[str] = None,
) -> list[LeadRaw]:
    """
    Executa o scraper do LinkedIn via Apify e retorna lista de LeadRaw.

    Args:
        keyword:      Keyword de busca geral (ex: "móveis planejados")
        estado:       Estado BR para filtro de localização
        max_results:  Limite de resultados (1 página = 25 perfis)
        apify_token:  Token Apify (fallback: APIFY_TOKEN)
    """
    prov = (provider or PROVIDER).lower()
    if prov == "ddgs":
        return _capturar_leads_ddgs(keyword=keyword, estado=estado, max_results=max_results)
    if prov == "apify":
        return _capturar_leads_apify(
            keyword=keyword,
            estado=estado,
            max_results=max_results,
            apify_token=apify_token,
        )
    raise ValueError(f"CAPTACAO_PROVIDER inválido: {prov}. Use 'ddgs' ou 'apify'.")


def capturar_batch(
    estados: list[str],
    keywords: Optional[list[str]] = None,
    max_por_combinacao: int = 50,
    apify_token: Optional[str] = None,
    provider: Optional[str] = None,
) -> list[LeadRaw]:
    """
    Executa captação para múltiplas combinações keyword × estado.
    Deduplica por linkedin_url.
    """
    kws = keywords or KEYWORDS_NICHO[:2]
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
                    provider=provider,
                )
                for lead in lote:
                    chave = lead.site_url or lead.linkedin_url or f"{lead.nome}|{lead.empresa}"
                    if chave not in vistos:
                        vistos.add(chave)
                        todos.append(lead)
            except Exception:
                logger.exception(
                    "Falha ao capturar '%s' em '%s' — continuando", keyword, estado
                )

    logger.info("Batch concluído: %d leads únicos", len(todos))
    return todos
