"""
Modelos de dados do pipeline de captação.

LeadRaw        → saída bruta do Apify (LinkedIn scraper)
LeadEnriquecido → após Firecrawl + LLM enricher
PayloadDisparo  → formato publicado na fila leads_disparo
                  (deve ser compatível com o sdr-worker existente)
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FonteLead(str, Enum):
    LINKEDIN_APIFY = "linkedin_apify"


class LeadRaw(BaseModel):
    """Dados brutos retornados pelo actor Apify do LinkedIn."""

    # Identificação
    nome: str
    empresa: str
    cargo: Optional[str] = None
    cidade: Optional[str] = None
    estado: Optional[str] = None

    # Contato
    telefone: Optional[str] = None          # nem sempre disponível no LinkedIn
    linkedin_url: Optional[str] = None
    site_url: Optional[str] = None

    # Metadados
    fonte: FonteLead = FonteLead.LINKEDIN_APIFY
    capturado_em: datetime = Field(default_factory=_utcnow)

    # Raw do Apify para auditoria
    apify_raw: Optional[dict] = None


class ContextoEmpresa(BaseModel):
    """Contexto extraído pelo Firecrawl + LLM sobre a empresa do lead."""

    resumo: str                             # 2-3 frases sobre o negócio
    segmento_exato: Optional[str] = None    # ex: "marcenaria sob medida premium"
    diferenciais: list[str] = Field(default_factory=list)
    produtos_destaque: list[str] = Field(default_factory=list)
    tom_da_marca: Optional[str] = None      # ex: "sofisticado", "familiar", "técnico"
    site_scrapeado: bool = False
    markdown_site: Optional[str] = None     # conteúdo bruto para debug


class LeadEnriquecido(BaseModel):
    """Lead com contexto enriquecido, pronto para geração de mensagem."""

    raw: LeadRaw
    contexto: ContextoEmpresa
    mensagem_cold: str                      # mensagem gerada pelo LLM
    enriquecido_em: datetime = Field(default_factory=_utcnow)


class PayloadDisparo(BaseModel):
    """
    Payload publicado na fila leads_disparo.
    Deve ser compatível com o schema esperado pelo sdr-worker.
    """

    # Identificação do lead
    nome: str
    empresa: str
    telefone: str                           # obrigatório para disparo WhatsApp
    cidade: Optional[str] = None

    # Mensagem de cold outreach
    mensagem: str

    # Contexto para o agente Yhas (SDR Frios) usar na conversa
    contexto_negocio: Optional[str] = None  # resumo da empresa para o agente
    linkedin_url: Optional[str] = None
    site_url: Optional[str] = None

    # Metadados de rastreabilidade
    fonte: str = FonteLead.LINKEDIN_APIFY
    campanha: Optional[str] = None          # ex: "moveis-sul-2025-06"
    capturado_em: Optional[datetime] = None
    disparado_em: datetime = Field(default_factory=_utcnow)
