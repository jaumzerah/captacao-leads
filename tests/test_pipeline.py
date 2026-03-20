"""Testes unitários do pipeline de captação. Rodar: pytest tests/ -v"""

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from captacao.models import (
    ContextoEmpresa,
    FonteLead,
    LeadEnriquecido,
    LeadRaw,
    PayloadDisparo,
)

PROMPT_TESTE = "Extraia contexto em JSON. Responda APENAS JSON válido."


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def lead_raw_completo():
    return LeadRaw(
        nome="Carlos Silva",
        empresa="Móveis Silva & Filhos",
        cargo="Sócio-proprietário",
        cidade="Curitiba",
        estado="Paraná",
        telefone="5541999990000",
        linkedin_url="https://linkedin.com/in/carlos-silva",
        site_url="https://moveissilva.com.br",
        fonte=FonteLead.LINKEDIN_APIFY,
    )


@pytest.fixture
def lead_raw_sem_site():
    return LeadRaw(
        nome="Ana Costa",
        empresa="Marcenaria Costa",
        cargo="Proprietária",
        cidade="Florianópolis",
        estado="Santa Catarina",
        telefone="5548988880000",
    )


@pytest.fixture
def lead_enriquecido(lead_raw_completo):
    return LeadEnriquecido(
        raw=lead_raw_completo,
        contexto=ContextoEmpresa(
            resumo="Móveis Silva & Filhos é uma marcenaria familiar em Curitiba.",
            segmento_exato="marcenaria sob medida",
            diferenciais=["entrega em 30 dias", "projeto 3D gratuito"],
            produtos_destaque=["cozinhas planejadas", "closets"],
            tom_da_marca="familiar",
            site_scrapeado=True,
        ),
        mensagem_cold="Olá Carlos, vi que a Móveis Silva está crescendo em Curitiba...",
    )


# ── models.py ────────────────────────────────────────────────────

class TestLeadRaw:
    def test_criacao_minima(self):
        lead = LeadRaw(nome="João", empresa="Loja X")
        assert lead.fonte == FonteLead.LINKEDIN_APIFY
        assert lead.telefone is None

    def test_criacao_completa(self, lead_raw_completo):
        assert lead_raw_completo.nome == "Carlos Silva"
        assert lead_raw_completo.telefone == "5541999990000"

    def test_capturado_em_default(self):
        lead = LeadRaw(nome="X", empresa="Y")
        assert isinstance(lead.capturado_em, datetime)


class TestPayloadDisparo:
    def test_serializacao_json(self, lead_enriquecido):
        payload = PayloadDisparo(
            nome=lead_enriquecido.raw.nome,
            empresa=lead_enriquecido.raw.empresa,
            telefone=lead_enriquecido.raw.telefone,
            mensagem=lead_enriquecido.mensagem_cold,
        )
        data = payload.model_dump(mode="json")
        assert data["nome"] == "Carlos Silva"
        assert json.dumps(data)  # serializável


# ── linkedin_scraper.py ───────────────────────────────────────────

class TestLinkedinScraper:
    def test_normalizar_telefone_sem_ddi(self):
        from captacao.linkedin_scraper import _normalizar_telefone
        assert _normalizar_telefone("41999990000") == "5541999990000"

    def test_normalizar_telefone_com_ddi(self):
        from captacao.linkedin_scraper import _normalizar_telefone
        assert _normalizar_telefone("5541999990000") == "5541999990000"

    def test_normalizar_telefone_none(self):
        from captacao.linkedin_scraper import _normalizar_telefone
        assert _normalizar_telefone(None) is None

    def test_normalizar_telefone_vazio(self):
        from captacao.linkedin_scraper import _normalizar_telefone
        assert _normalizar_telefone("") is None

    def test_extrair_estado_formato_linkedin(self):
        from captacao.linkedin_scraper import _extrair_estado
        assert _extrair_estado("Curitiba, Paraná, Brasil") == "Paraná"

    def test_extrair_estado_cidade_pais(self):
        from captacao.linkedin_scraper import _extrair_estado
        resultado = _extrair_estado("São Paulo, Brasil")
        assert resultado in ("Brasil", "São Paulo")

    def test_extrair_estado_none(self):
        from captacao.linkedin_scraper import _extrair_estado
        assert _extrair_estado(None) is None

    def test_item_para_lead_raw_completo(self):
        from captacao.linkedin_scraper import _item_para_lead_raw
        item = {
            "fullName": "Carlos Silva",
            "companyName": "Móveis Silva",
            "headline": "Sócio | Marcenaria",
            "location": "Curitiba, Paraná, Brasil",
            "linkedinUrl": "https://linkedin.com/in/carlos",
            "companyWebsite": "https://moveissilva.com.br",
            "phone": "41999990000",
        }
        lead = _item_para_lead_raw(item)
        assert lead is not None
        assert lead.nome == "Carlos Silva"
        assert lead.telefone == "5541999990000"
        assert lead.site_url == "https://moveissilva.com.br"

    def test_item_para_lead_raw_sem_nome(self):
        from captacao.linkedin_scraper import _item_para_lead_raw
        assert _item_para_lead_raw({"companyName": "X"}) is None

    def test_filtrar_cargo_dono(self, lead_raw_completo):
        from captacao.linkedin_scraper import _filtrar_cargo
        assert _filtrar_cargo(lead_raw_completo) is True

    def test_filtrar_cargo_irrelevante(self, lead_raw_completo):
        from captacao.linkedin_scraper import _filtrar_cargo
        lead_raw_completo.cargo = "Auxiliar de estoque"
        assert _filtrar_cargo(lead_raw_completo) is False

    def test_filtrar_cargo_sem_cargo(self, lead_raw_completo):
        from captacao.linkedin_scraper import _filtrar_cargo
        lead_raw_completo.cargo = None
        assert _filtrar_cargo(lead_raw_completo) is True


# ── enricher.py ──────────────────────────────────────────────────

class TestEnricher:
    def test_parse_contexto_json_valido(self, lead_raw_completo):
        from captacao.enricher import _parse_contexto_json
        raw = json.dumps({
            "resumo": "Empresa de móveis em Curitiba.",
            "segmento_exato": "marcenaria premium",
            "diferenciais": ["entrega rápida"],
            "produtos_destaque": ["cozinhas"],
            "tom_da_marca": "familiar",
        })
        ctx = _parse_contexto_json(raw, lead_raw_completo)
        assert ctx.resumo == "Empresa de móveis em Curitiba."
        assert ctx.segmento_exato == "marcenaria premium"

    def test_parse_contexto_json_invalido_usa_fallback(self, lead_raw_completo):
        from captacao.enricher import _parse_contexto_json
        ctx = _parse_contexto_json("não é json {{{", lead_raw_completo)
        assert ctx.resumo

    def test_parse_contexto_json_com_markdown_block(self, lead_raw_completo):
        from captacao.enricher import _parse_contexto_json
        raw = '```json\n{"resumo": "Empresa X.", "segmento_exato": null, "diferenciais": [], "produtos_destaque": [], "tom_da_marca": null}\n```'
        ctx = _parse_contexto_json(raw, lead_raw_completo)
        assert ctx.resumo == "Empresa X."

    def test_truncar_markdown(self):
        from captacao.enricher import _truncar_markdown
        longo = "x" * 50000
        result = _truncar_markdown(longo, max_chars=1000)
        assert len(result) <= 1021
        assert "[conteúdo truncado]" in result

    def test_truncar_markdown_curto_nao_trunca(self):
        from captacao.enricher import _truncar_markdown
        curto = "Conteúdo curto do site."
        assert _truncar_markdown(curto) == curto


# ── publisher.py ─────────────────────────────────────────────────

class TestPublisher:
    def test_montar_payload_completo(self, lead_enriquecido):
        from captacao.publisher import _montar_payload
        payload = _montar_payload(lead_enriquecido, campanha="teste")
        assert payload.nome == "Carlos Silva"
        assert payload.telefone == "5541999990000"
        assert payload.campanha == "teste"
        assert payload.mensagem

    def test_montar_payload_sem_telefone_levanta_erro(self, lead_raw_sem_site):
        from captacao.publisher import _montar_payload
        lead = LeadEnriquecido(
            raw=lead_raw_sem_site,
            contexto=ContextoEmpresa(resumo="X"),
            mensagem_cold="mensagem",
        )
        lead.raw.telefone = None
        with pytest.raises(ValueError, match="sem telefone"):
            _montar_payload(lead)


# ── db.py ─────────────────────────────────────────────────────────

class TestDb:
    @pytest.mark.asyncio
    async def test_get_prompt_encontrado(self):
        from captacao.db import get_prompt
        pool = AsyncMock()
        pool.fetchrow = AsyncMock(return_value={"system": "Prompt de teste"})
        result = await get_prompt(pool, "moveis-planejados", "enricher")
        assert result == "Prompt de teste"
        pool.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_prompt_nao_encontrado_levanta_erro(self):
        from captacao.db import get_prompt
        pool = AsyncMock()
        pool.fetchrow = AsyncMock(return_value=None)
        with pytest.raises(ValueError, match="Nenhum prompt ativo"):
            await get_prompt(pool, "nicho-inexistente", "enricher")

    @pytest.mark.asyncio
    async def test_get_campanha_encontrada(self):
        from captacao.db import get_campanha
        pool = AsyncMock()
        pool.fetchrow = AsyncMock(return_value={
            "id": 1, "nicho_id": 1, "slug": "moveis-sul",
            "nome": "Móveis Sul", "estados": ["São Paulo", "Paraná"],
            "keywords": ["loja móveis"], "max_por_combo": 50,
            "ativa": True, "nicho_slug": "moveis-planejados",
        })
        result = await get_campanha(pool, "moveis-sul")
        assert result["slug"] == "moveis-sul"
        assert result["estados"] == ["São Paulo", "Paraná"]

    @pytest.mark.asyncio
    async def test_get_campanha_nao_encontrada_levanta_erro(self):
        from captacao.db import get_campanha
        pool = AsyncMock()
        pool.fetchrow = AsyncMock(return_value=None)
        with pytest.raises(ValueError, match="Campanha não encontrada"):
            await get_campanha(pool, "campanha-inexistente")

    @pytest.mark.asyncio
    async def test_salvar_lead_com_linkedin_url(self, lead_enriquecido):
        from captacao.db import salvar_lead
        pool = AsyncMock()
        pool.fetchrow = AsyncMock(return_value={"id": 42})
        lead_id = await salvar_lead(pool, campanha_id=1, lead=lead_enriquecido, status="publicado")
        assert lead_id == 42
        pool.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_marcar_publicado(self):
        from captacao.db import marcar_publicado
        pool = AsyncMock()
        pool.execute = AsyncMock()
        await marcar_publicado(pool, lead_id=42)
        pool.execute.assert_called_once()
        # Verifica que o SQL menciona 'publicado'
        call_args = pool.execute.call_args[0]
        assert "publicado" in call_args[0]
        assert call_args[1] == 42
