"""
Publicação de leads na fila leads_disparo do RabbitMQ.

Usa o mesmo broker/vhost da infraestrutura existente (SimplifiqueNet).
O sdr-worker já está rodando e consumindo essa fila.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

import aio_pika
import asyncio

from captacao.models import LeadEnriquecido, PayloadDisparo

logger = logging.getLogger(__name__)

FILA_DISPARO = "leads_disparo"


def _montar_payload(lead: LeadEnriquecido, campanha: Optional[str] = None) -> PayloadDisparo:
    """Converte LeadEnriquecido no payload esperado pelo sdr-worker."""
    raw = lead.raw

    if not raw.telefone:
        raise ValueError(
            f"Lead '{raw.nome}' ({raw.empresa}) sem telefone — não pode ser disparado"
        )

    return PayloadDisparo(
        nome=raw.nome,
        empresa=raw.empresa,
        telefone=raw.telefone,
        cidade=raw.cidade,
        mensagem=lead.mensagem_cold,
        contexto_negocio=lead.contexto.resumo,
        linkedin_url=raw.linkedin_url,
        site_url=raw.site_url,
        fonte=raw.fonte,
        campanha=campanha,
        capturado_em=raw.capturado_em,
    )


async def _publicar_async(
    payloads: list[PayloadDisparo],
    rabbitmq_url: str,
) -> tuple[int, int]:
    """
    Publica payloads na fila leads_disparo de forma assíncrona.
    Retorna (publicados, falhas).
    """
    connection = await aio_pika.connect_robust(rabbitmq_url)
    publicados = 0
    falhas = 0

    async with connection:
        channel = await connection.channel()
        fila = await channel.declare_queue(
            FILA_DISPARO,
            durable=True,
            passive=True,  # não recria se já existir
        )

        for payload in payloads:
            try:
                body = json.dumps(payload.model_dump(mode="json"), ensure_ascii=False).encode()
                message = aio_pika.Message(
                    body=body,
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                )
                await channel.default_exchange.publish(
                    message,
                    routing_key=fila.name,
                )
                logger.info("Publicado: %s — %s", payload.nome, payload.empresa)
                publicados += 1
            except Exception:
                logger.exception("Falha ao publicar '%s' — pulando", payload.empresa)
                falhas += 1

    return publicados, falhas


def publicar_leads(
    leads: list[LeadEnriquecido],
    campanha: Optional[str] = None,
    rabbitmq_url: Optional[str] = None,
) -> tuple[int, int]:
    """
    Monta payloads e publica na fila leads_disparo.

    Args:
        leads:         Lista de LeadEnriquecido com mensagem_cold preenchida
        campanha:      Tag de rastreabilidade (ex: "moveis-sul-2025-06")
        rabbitmq_url:  URL do RabbitMQ (fallback: var RABBITMQ_URL)

    Returns:
        Tupla (publicados, falhas)
    """
    url = rabbitmq_url or os.environ["RABBITMQ_URL"]

    # Monta payloads — descarta leads sem telefone
    payloads = []
    sem_telefone = 0
    for lead in leads:
        try:
            payload = _montar_payload(lead, campanha=campanha)
            payloads.append(payload)
        except ValueError as e:
            logger.warning("%s", e)
            sem_telefone += 1

    if sem_telefone:
        logger.warning("%d leads descartados por falta de telefone", sem_telefone)

    if not payloads:
        logger.warning("Nenhum payload para publicar")
        return 0, sem_telefone

    publicados, falhas = asyncio.run(_publicar_async(payloads, url))
    logger.info(
        "Publicação finalizada: %d OK, %d falhas, %d sem telefone",
        publicados,
        falhas,
        sem_telefone,
    )
    return publicados, falhas + sem_telefone
