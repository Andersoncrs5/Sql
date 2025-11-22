# ============================
# STAGE 1: Builder (usando imagem Glibc)
# ============================
FROM golang:1.25.4 AS builder 
WORKDIR /app

# Instala as dependências C necessárias para o CGO/confluent-kafka-go
# Em imagens base Debian (Glibc), os pacotes são diferentes.
# O build-essential fornece o gcc, e o librdkafka-dev fornece os cabeçalhos.
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia go.mod e go.sum
COPY go.mod go.sum ./
RUN go mod download

# Copia o restante do código
COPY . .

# Compila o binário. O CGO está ativo e o ambiente é Glibc-based, resolvendo o erro de linkagem.
RUN go build -o cdc_processor main.go


# ============================
# STAGE 2: Runtime (Melhor usar uma base Glibc para consistência)
# ============================
FROM debian:bookworm-slim
# Configurações de fuso horário, se necessário (menos crítico agora)
RUN apt-get update && apt-get install -y \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Instala a versão runtime da librdkafka. O binário compilado com CGO PRECISA DESSA LIB.
# O nome do pacote runtime para librdkafka no Debian é geralmente librdkafka1.
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Copia o binário gerado no estágio anterior
COPY --from=builder /app/cdc_processor /cdc_processor

# Expõe porta (opcional)
EXPOSE 8080

# Executa o binário
ENTRYPOINT ["/cdc_processor"]