"""
Geração de mensagem de cold outreach personalizada via LLM.

Recebe um LeadEnriquecido e retorna o LeadEnriquecido com mensagem_cold preenchida.
A mensagem será enviada pelo sdr-worker via instância SimplifiqueMoveis
e recebida pelo agente Yhas (SDR Frios).
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import anthropic

from captacao.models import LeadEnriquecido

logger = logging.getLogger(__name__)

SYSTEM_GERADOR = """\
Você é um especialista em vendas consultivas B2B para o setor de móveis \
planejados no Brasil.

Escreva mensagens de WhatsApp de cold outreach para donos de lojas de móveis \
planejados e marcenarias. O remetente é a Agência Simplifique, que oferece um \
sistema de SDR com IA que agenda reuniões automaticamente via WhatsApp.

Regras obrigatórias:
- Máximo 3 parágrafos curtos
- Tom consultivo, não vendedor agressivo
- Mencionar algo específico e real do negócio do lead (empresa, produto, cidade)
- Não usar emojis em excesso — máximo 1 por mensagem
- Terminar com uma pergunta aberta que convide à resposta
- NÃO mencionar preço, desconto ou promoção
- NÃO usar termos como "revolucionário", "incrível", "oportunidade única"
- Escrever em português brasileiro informal mas profissional
- A mensagem deve parecer escrita por uma pessoa, não por um robô

Responda APENAS com o texto da mensagem, sem aspas, sem explicações.
"""


def gerar_mensagem(
    lead: LeadEnriquecido,
    owner_name: Optional[str] = None,
    brand_name: Optional[str] = None,
    anthropic_key: Optional[str] = None,
) -> str:
    """
    Gera mensagem de cold outreach personalizada para o lead.

    Args:
        lead:          LeadEnriquecido com contexto da empresa
        owner_name:    Nome do dono da agência (fallback: var OWNER_NAME)
        brand_name:    Nome da marca/agência (fallback: var BRAND_NAME)
        anthropic_key: Chave Anthropic (fallback: var ANTHROPIC_API_KEY)

    Returns:
        Texto da mensagem para WhatsApp
    """
    ant_key = anthropic_key or os.environ["ANTHROPIC_API_KEY"]
    dono = owner_name or os.getenv("OWNER_NAME", "João")
    marca = brand_name or os.getenv("BRAND_NAME", "Agência Simplifique")

    cliente = anthropic.Anthropic(api_key=ant_key)
    raw = lead.raw
    ctx = lead.contexto

    # Monta prompt com todo o contexto disponível
    linhas_contexto = [
        f"Nome do contato: {raw.nome}",
        f"Empresa: {raw.empresa}",
        f"Cargo: {raw.cargo or 'não informado'}",
        f"Cidade/Estado: {raw.cidade or ''} {raw.estado or ''}".strip(),
    ]

    if ctx.resumo:
        linhas_contexto.append(f"Sobre a empresa: {ctx.resumo}")
    if ctx.segmento_exato:
        linhas_contexto.append(f"Segmento: {ctx.segmento_exato}")
    if ctx.diferenciais:
        linhas_contexto.append(f"Diferenciais: {', '.join(ctx.diferenciais[:2])}")
    if ctx.produtos_destaque:
        linhas_contexto.append(f"Produtos em destaque: {', '.join(ctx.produtos_destaque[:2])}")
    if ctx.tom_da_marca:
        linhas_contexto.append(f"Tom da marca: {ctx.tom_da_marca}")

    linhas_contexto.append(f"\nRemetente: {dono} da {marca}")
    linhas_contexto.append(
        "Objetivo da mensagem: conseguir uma resposta que abra conversa "
        "sobre como o sistema SDR pode ajudar nas vendas da loja."
    )

    prompt = "\n".join(linhas_contexto)

    response = cliente.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        system=SYSTEM_GERADOR,
        messages=[{"role": "user", "content": prompt}],
    )

    mensagem = response.content[0].text.strip()
    logger.debug("Mensagem gerada para '%s': %s chars", raw.empresa, len(mensagem))
    return mensagem


def gerar_mensagens_batch(
    leads: list[LeadEnriquecido],
    **kwargs,
) -> list[LeadEnriquecido]:
    """
    Gera mensagens para uma lista de leads enriquecidos.
    Preenche o campo mensagem_cold in-place e retorna a lista.
    """
    for i, lead in enumerate(leads, 1):
        logger.info(
            "[%d/%d] Gerando mensagem para: %s — %s",
            i,
            len(leads),
            lead.raw.nome,
            lead.raw.empresa,
        )
        try:
            lead.mensagem_cold = gerar_mensagem(lead, **kwargs)
        except Exception:
            logger.exception(
                "Falha ao gerar mensagem para '%s' — usando fallback genérico",
                lead.raw.empresa,
            )
            lead.mensagem_cold = (
                f"Olá {lead.raw.nome.split()[0]}, tudo bem? "
                f"Vi que você trabalha com móveis planejados na {lead.raw.empresa}. "
                f"Tenho algo que pode otimizar muito o processo de prospecção de vocês. "
                f"Posso compartilhar como funciona?"
            )
    return leads
