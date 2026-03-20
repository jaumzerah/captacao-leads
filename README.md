# captacao-leads

Sistema de captação e disparo de leads B2B para o setor de móveis planejados.  
Parte da stack **Agência Simplifique** — serviço autônomo no Docker Swarm.

## Visão geral

```
LinkedIn (Apify) → Firecrawl (self-hosted) → LLM (Anthropic) → RabbitMQ → sdr-worker
```

1. **Apify** scrapa perfis LinkedIn filtrando por cargo (dono/sócio/diretor) e setor
2. **Firecrawl** visita o site da empresa e extrai conteúdo em markdown
3. **LLM** enriquece o contexto da empresa e gera mensagem de cold outreach personalizada
4. **RabbitMQ** recebe o payload na fila `leads_disparo`
5. **sdr-worker** (repo `simplifique-moveis`) consome e envia via instância `SimplifiqueMoveis`

## Estrutura

```
captacao-leads/
├── captacao/
│   ├── models.py            # LeadRaw, LeadEnriquecido, PayloadDisparo
│   ├── linkedin_scraper.py  # Captação via Apify actor
│   ├── enricher.py          # Firecrawl + LLM → ContextoEmpresa
│   ├── message_generator.py # LLM → cold outreach personalizado
│   └── publisher.py         # Publica em leads_disparo (RabbitMQ)
├── tests/
│   └── test_pipeline.py     # 23 testes unitários
├── scheduler.py             # Orquestrador do batch diário
├── Dockerfile
├── docker-stack.captacao.yml   # Stack Swarm do captador
├── docker-stack.firecrawl.yml  # Stack Swarm do Firecrawl self-hosted
└── .env.example
```

## Pré-requisitos

- Docker Swarm com rede overlay `simplifiquenet`
- Stack `simplifique-moveis` rodando (RabbitMQ + sdr-worker ativos)
- Conta [Apify](https://apify.com) (free tier suficiente para início)
- Chave Anthropic API

## Setup

```bash
# 1. Clone e configure variáveis
cp .env.example .env
# Preencha ANTHROPIC_API_KEY, APIFY_TOKEN, RABBITMQ_PASS

# 2. Deploy do Firecrawl self-hosted (uma vez)
set -a && source .env && set +a
docker stack deploy -c docker-stack.firecrawl.yml firecrawl

# 3. Build e deploy do captador
docker build -t ghcr.io/jaumzerah/captacao-leads:latest .
docker stack deploy -c docker-stack.captacao.yml captacao
```

## Executar batch manualmente

```bash
# Dry-run (não publica na fila — só loga)
docker run --rm --env-file .env captacao-leads python scheduler.py --dry-run

# Batch real
docker run --rm --env-file .env captacao-leads python scheduler.py
```

## Configuração via variáveis de ambiente

| Variável | Descrição | Padrão |
|---|---|---|
| `APIFY_TOKEN` | Token da API Apify | obrigatório |
| `APIFY_ACTOR_ID` | Actor ID do LinkedIn scraper | `apify/linkedin-profile-scraper` |
| `FIRECRAWL_URL` | URL do Firecrawl self-hosted | `http://firecrawl_firecrawl:3002` |
| `ANTHROPIC_API_KEY` | Chave Anthropic | obrigatório |
| `RABBITMQ_USER` | Usuário RabbitMQ | obrigatório |
| `RABBITMQ_PASS` | Senha RabbitMQ | obrigatório |
| `RABBITMQ_VHOST` | vhost RabbitMQ | `padrao` |
| `CAPTACAO_ESTADOS` | Estados alvo (CSV) | `São Paulo,Paraná,Santa Catarina` |
| `CAPTACAO_MAX_POR_COMBO` | Máx leads por keyword×estado | `50` |
| `CAPTACAO_CAMPANHA` | Tag de rastreabilidade | data de hoje |
| `OWNER_NAME` | Nome do dono da agência | `João Andrade` |
| `BRAND_NAME` | Nome da marca | `Agência Simplifique` |

## Testes

```bash
pip install -e ".[dev]"
pytest tests/ -v
# 23 passed
```

## Regras técnicas

- `set -a / source .env / set +a` antes de todo `docker stack deploy`
- `env_file:` não é suportado pelo Swarm — todas as vars via `environment:` com `${VAR}`
- Consultar Context7 antes de atualizar dependências
- Rodar testes antes de qualquer push

## Payload publicado em `leads_disparo`

```json
{
  "nome": "Carlos Silva",
  "empresa": "Móveis Silva & Filhos",
  "telefone": "5541999990000",
  "cidade": "Curitiba",
  "mensagem": "Olá Carlos...",
  "contexto_negocio": "Marcenaria familiar em Curitiba...",
  "linkedin_url": "https://linkedin.com/in/...",
  "site_url": "https://moveissilva.com.br",
  "fonte": "linkedin_apify",
  "campanha": "batch-20250601",
  "capturado_em": "2025-06-01T08:00:00Z",
  "disparado_em": "2025-06-01T08:05:00Z"
}
```
