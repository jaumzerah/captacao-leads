FROM python:3.11-slim

WORKDIR /app

# Dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instala dependências Python
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Copia código
COPY captacao/ ./captacao/
COPY scheduler.py .

# Usuário não-root
RUN useradd -m -u 1000 captacao
USER captacao

# Cron via supercronic ou execução direta
CMD ["python", "scheduler.py"]
