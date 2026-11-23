# Runbook — operação e solução de problemas

Comandos no PowerShell, na pasta `plataforma-iot`.

## Verificar a saúde do ambiente

```powershell
docker compose --profile ingestao ps                 # estado de cada serviço
docker stats --no-stream                             # memória e CPU de cada container
docker compose logs --tail 50 consumidor             # últimos eventos do consumidor
docker compose run --rm ferramentas python -m consultas.verificar_bronze
```

Sinais de que está tudo bem:

- `init` com estado **Exited (0)**
- `consumidor` registrando `"msg": "lote gravado"` a cada minuto, com `"invalidas": 0`
- o número de arquivos na Bronze aumentando

## Problemas comuns

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `init` reinicia ou fica esperando o Cassandra | Cassandra ainda subindo (até 3 min com 8 GB) | Esperar. Conferir com `docker compose logs -f cassandra` |
| Container com estado **Exited (137)** | Faltou memória | Parar o outro perfil, fechar programas e subir de novo |
| `consumidor` com `aguardando servico` | Algum banco fora do ar | `docker compose --profile ingestao ps` e subir o que estiver parado |
| `port is already allocated` | Outro programa usando a porta | Fechar o programa, ou trocar a porta da esquerda em `ports:` no compose |
| `pull access denied` | Imagem inexistente ou sem internet | Conferir a internet e o nome da imagem |
| Console do RustFS não abre | RustFS ainda iniciando | Esperar 30 s e recarregar http://localhost:9001 |
| `carga-catalogo` com erro de autenticação no Neo4j | Senha mudou depois da primeira subida | `docker compose --profile catalogo down -v` e subir de novo |
| Mudança no código não tem efeito | Imagem antiga | `docker compose build` e subir de novo |

## Reprocessar a Bronze a partir do Kafka

O Kafka guarda as mensagens por 24 h. Para regravar tudo o que ainda está no tópico:

```powershell
docker compose stop consumidor
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 `
  --group bronze-writer --topic sensores.leituras --reset-offsets --to-earliest --execute
docker compose start consumidor
```

Como os nomes dos arquivos dependem da partição e dos offsets, os arquivos regravados substituem
os anteriores quando os lotes coincidem. Diferenças de recorte entre lotes podem gerar
sobreposição, resolvida na Silver pela deduplicação por `(particao, offset)`.

## Começar do zero

```powershell
docker compose --profile ingestao --profile catalogo down -v
docker compose build
docker compose --profile ingestao up -d
```

## Acessar os bancos pela linha de comando

```powershell
docker compose exec cassandra cqlsh -e "SELECT * FROM iot.leituras_por_sensor WHERE sensor_id='S-L1-M01-TEMP' AND dia=toDate(now()) LIMIT 5;"
docker compose exec redis redis-cli HGETALL sensor:S-L1-M01-TEMP:ultimo
docker compose exec mongo mongosh -u admin -p admin123 --eval "db.getSiblingDB('fabrica').maquinas.countDocuments()"
```
