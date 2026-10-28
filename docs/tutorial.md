# Збірка, конфігурація та розгортання екосистеми

Цей посібник описує запуск трьох частин разом:

1. **JSON-RPC-2 backend** — прикладний сервер, який реєструє методи та обробляє JSON-RPC.
2. **JSON-RPC Gateway** — ingress, маршрутизація та Control Plane.
3. **JSON-RPC Balancer** — бібліотека всередині gateway, яка вибирає backend, підтримує пули з'єднань і circuit breaker-и.

Balancer не є окремим процесом чи executable: окремо запускається gateway, а він під час старту завантажує backends і перевіряє їх доступність.

## Важливо про сумісність backend-а

Для обробки трафіку від gateway backend має використовувати `json_rpc_2_server::RpcServer`, загорнутий у `gateway_protocol::GatewayBackendProtocol`. Це внутрішній протокол між gateway та backend-ом, який переносить оригінальний payload і метадані з'єднання. У цьому workspace немає окремого прикладного backend executable із бізнесовими методами: його потрібно надати/зібрати як застосунок, використовуючи наведений у README server adapter.

Звичайний `JSON_RPC_2` CLI server — демонстраційний/прямий JSON-RPC сервер. Він **не** загортає протокол у `GatewayBackendProtocol` і тому не є сумісним gateway backend-ом без відповідного адаптера в прикладному сервері. Приклад адаптера і серверного запуску є в кореневому [README](../README.md#backend-contract).

Для окремої перевірки звичайного JSON-RPC-2 CLI server-а (не через gateway) можна запустити один transport:

```sh
cd /path/to/JSON-RPC-2
cargo run --locked -p json-rpc-2-cli --bin JSON_RPC_2 -- server tcp --bind 127.0.0.1 --tcp-port 8081
```

## Топологія і мережеві правила

```text
JSON-RPC client
      |
      v
JSON-RPC Gateway : ingress ports
      |\
      | +--> Control Plane : CONTROL_PORT (для внутрішніх push/register команд)
      +----> Prometheus : METRICS_BIND:METRICS_PORT/metrics
      |
      +----> JSON-RPC-2 backend : один чи кілька internal endpoints
                     |
                     +--> зовнішні MQTT/AMQP brokers, якщо вони використовуються
```

- Gateway приймає зовнішній трафік на налаштованих ingress-портах. Він не розбирає бізнесові JSON-RPC payload-и.
- Balancer підбирає endpoint відповідно до ingress-транспорту, підтримує pool connections, перевірки здоров'я та circuit breaker-и.
- Backend-и слід розміщувати в приватній мережі й дозволяти доступ до них лише з gateway-вузлів.
- Control Plane не має автентифікації в цьому сервісі. Не відкривайте `CONTROL_PORT` у публічну мережу; обмежте його firewall-ом/мережею застосунку.
- Metrics за замовчуванням слухають лише `127.0.0.1`. Для віддаленого Prometheus використовуйте приватну адресу і мережеве обмеження доступу.
- TLS/QUIC/HTTP3 ingress потребують сертифікат і приватний ключ gateway-а. TLS-з'єднання до backend-а перевіряється через довірений CA, налаштований для цього endpoint-а.
- У репозиторії немає Dockerfile або compose-конфігурації. Нижче наведено збірку бінарників і приклад розгортання через systemd; контейнеризація може використовувати ті самі бінарники, конфігурацію та мережеві правила.

## Підготовка та збірка

### Загальні вимоги

- Rust toolchain, що підтримує edition 2024, і Cargo.
- Доступ до Git-залежності [JSON-RPC-2](https://github.com/vsvictor/JSON-RPC-2) під час першої збірки.
- Для TLS/QUIC/HTTP3 — сертифікат і ключ gateway-а, а для TLS backend-ів — CA сертифікат.
- Для MQTT/AMQP — окремо налаштовані зовнішні brokers і мережевий доступ до них.
- Для SRT — збірка gateway з feature `srt`, C/C++ toolchain, CMake, OpenSSL development files, pkg-config і libclang. Див. [SRT build documentation](../README.md#building-with-srt).
- UDP-Lite, DCCP і SCTP залежать від ОС та підтримки ядра. Для Linux/Android додатково перевірте доступність потрібних socket types.

Gateway workspace використовує upstream JSON-RPC-2 через Git dependency, а `Cargo.lock` фіксує конкретний commit. Збирайте з lockfile, щоб середовища отримали однаковий upstream код:

```sh
cd /path/to/JsonRPCLoadBalanser
cargo build --locked --release -p json-rpc-gateway
```

Якщо потрібен SRT:

```sh
scripts/build-with-srt.sh build --release -p json-rpc-gateway --features srt
```

JSON-RPC-2 CLI — корисний для перевірки library/transport-ів або прямого запуску JSON-RPC сервера, але не заміняє gateway-сумісний backend:

```sh
cd /path/to/JSON-RPC-2
cargo build --locked --release -p json-rpc-2-cli --bin JSON_RPC_2
```

Якщо backend застосунок живе в окремому Cargo-проєкті, збирайте його окремо з зафіксованою версією JSON-RPC-2 та `gateway-protocol`. Для production зафіксуйте версію/commit у lockfile самого backend застосунку.

## Backend: реалізація та конфігурація

Backend застосунок має:

1. Створити `RpcServer` і зареєструвати прикладні RPC методи.
2. Обгорнути його в `GatewayBackendProtocol`.
3. Запустити `MultiTransportServer` на внутрішній адресі й портах, що збігаються з `ports` у gateway backend-конфігурації.
4. Не відкривати ці listener-и безпосередньо в публічний інтернет.

Для найпростішого локального прикладу використовуйте TCP. Якщо налаштовані TLS/QUIC/HTTP3/SRT чи broker transports, їхні server-side протоколи та довірчі матеріали мають відповідати конфігурації gateway-а.

### Конфігурація backend pool

`BACKEND_CONFIG_PATH` у gateway вказує на JSON або YAML файл:

```json
{
  "backends": [
    {
      "id": "backend-a",
      "host": "10.20.0.11",
      "ports": { "tcp": 8081 },
      "pool_size": 8,
      "options": {
        "operation_timeout_ms": 30000,
        "health_check_interval_ms": 30000,
        "circuit_breaker_failure_threshold": 5,
        "circuit_breaker_reset_timeout_ms": 10000
      }
    },
    {
      "id": "backend-b",
      "host": "10.20.0.12",
      "ports": { "tcp": 8081 },
      "pool_size": 8
    }
  ]
}
```

Підтримувані поля:

| Поле | Значення |
|---|---|
| `backends` | Непорожній список backend endpoints. |
| `id` | Унікальний непорожній ID endpoint-а; використовується в логах і низькокардинальних metric labels. |
| `host` | IP або DNS hostname backend-а. |
| `ports` | Map транспорт → порт, наприклад `"tcp": 8081`, `"tls": 8443`, `"http1": 8082`. Для endpoint-а потрібен принаймні один порт. Вказуйте тільки транспорти, доступні у збірці та мережі. |
| `pool_size` | Кількість persistent connections на кожний налаштований транспорт, default `4`; має бути більше нуля. |
| `options.tls_ca_path` | Шлях до PEM CA bundle для `tls`, `ws-tls`, `quic`, `ws-quic`, `http3`, `ws-http3`. Обов'язковий для цих захищених backend-транспорту. |
| `options.tls_server_name` | Очікуване ім'я сертифіката; якщо відсутнє, використовується `host`. |
| `options.srt_passphrase_env` | Ім'я environment variable з SRT passphrase. Сам секрет зберігайте в environment/secret manager, не в JSON/YAML. |
| `options.mqtt_url_env` | Ім'я environment variable з URL MQTT broker-а, наприклад `BACKEND_MQTT_URL`. |
| `options.mqtt_request_topic` | Override topic для MQTT backend route. |
| `options.ws_mqtt_request_topic` | Override topic для WebSocket/MQTT backend route. |
| `options.amqp_url_env` | Ім'я environment variable з URL AMQP broker-а, наприклад `BACKEND_AMQP_URL`. |
| `options.amqp_request_queue` | Override queue для AMQP backend route. |
| `options.ws_amqp_request_queue` | Override queue для WebSocket/AMQP backend route. |
| `options.operation_timeout_ms` | Timeout connect/health/exchange операцій, default `30000` мс; має бути більше нуля. |
| `options.health_check_interval_ms` | Інтервал періодичної перевірки pooled connection, default `30000` мс; має бути більше нуля. |
| `options.circuit_breaker_failure_threshold` | Кількість послідовних transport/health-check помилок до відкриття circuit-а, default `5`; має бути більше нуля. |
| `options.circuit_breaker_reset_timeout_ms` | Час до half-open recovery probe, default `10000` мс; має бути більше нуля. |
| `options.broker_heartbeat_interval_ms` | Heartbeat MQTT/AMQP backend session, default `10000` мс. |
| `options.broker_session_lease_ms` | Lease broker session, default `45000` мс; має перевищувати heartbeat interval. |

Backend-и спочатку проходять startup warm-up. Якщо будь-який endpoint/transport, вказаний у конфігурації, не проходить health check, gateway завершує запуск з помилкою, а не тихо стартує з неповним pool-ом. Не додавайте в `ports` transport, який цей backend не слухає.

## Gateway: `.env` та середовище запуску

У gateway checkout зараз немає окремого перевіреного `.env` файлу. Створіть runtime `.env` у робочому каталозі gateway-а або передайте ті самі змінні через environment менеджера сервісів. Gateway завантажує `.env` з поточного каталогу при старті. Для systemd задавайте абсолютні шляхи та явно вказуйте `WorkingDirectory`.

Мінімальний приклад для двох TCP backend-ів із попереднього розділу:

```dotenv
GATEWAY_BIND=10.20.0.5
GATEWAY_PORT=8443
CONTROL_PORT=8500
BACKEND_CONFIG_PATH=/etc/json-rpc-gateway/backends.json
LOG_LEVEL=info
METRICS_BIND=127.0.0.1
METRICS_PORT=9090
```

### Обов'язкові загальні змінні

| Змінна | Обов'язкова | Призначення / значення |
|---|---:|---|
| `GATEWAY_BIND` | так | IP адреса, на якій слухаються gateway ingress і Control Plane. Не встановлюйте `0.0.0.0`, якщо сервіс не повинен слухати всі інтерфейси. |
| `GATEWAY_PORT` | так | Fallback порт для TCP та UDP ingress, якщо не задано transport-specific порт. |
| `CONTROL_PORT` | так | TCP порт Control Plane; захистіть його firewall-ом, він не має автентифікації. |
| `BACKEND_CONFIG_PATH` | так | Шлях до backend JSON/YAML файлу з попереднього розділу. Відносні шляхи залежать від робочого каталогу процесу. |
| `LOG_LEVEL` | так | `tracing` filter, наприклад `info`, `debug`, або детальніший фільтр за target. `debug` виводить повні request/response payload-и, тому використовуйте його лише контрольовано. Gateway використовує `LOG_LEVEL`, не `RUST_LOG`, для фільтрації. |

Gateway записує tracing-події, включно з balancer і бібліотечними подіями, у stdout окремим worker-потоком через bounded чергу на 1024 записи. Форматування залишається в потоці, що створює подію, але повільний stdout/journald не блокує RPC-задачі на I/O. При переповненні нові записи будь-якого рівня відкидаються; Prometheus-лічильник `gateway_log_records_dropped_total` оновлюється раз на секунду. Він вимірює втрати черги, не помилки подальшої доставки stdout/journald. При штатному завершенні (Ctrl+C або Unix SIGTERM) guard робить обмежену в часі спробу дописати чергу й повідомляє кількість втрат у stderr; аварійне завершення може втратити записи. Нових `.env` параметрів немає.

Demo backend використовує такий самий неблокувальний writer із `LOG_LEVEL=info` за замовчуванням і повідомляє втрати у stderr під час завершення; окремого metrics endpoint у нього немає. Upstream CLI також має незалежну bounded чергу на 1024 tracing-записи та stderr-звіт про втрати при завершенні, але використовує `RUST_LOG`, а не `LOG_LEVEL`. Явно некоректний `RUST_LOG` завершує CLI з помилкою; звичайний CLI-вивід `println!` залишається синхронним. Бібліотеки не встановлюють глобальний subscriber: у власному прикладному backend-і його налаштовує застосунок.

### Ingress, concurrency та metrics

| Змінна | Default | Призначення / правила |
|---|---:|---|
| `GATEWAY_<TRANSPORT>_PORT` | не задано | Увімкнути конкретний ingress transport і встановити його порт. Для назв із дефісами дефіс замінюється на `_`, наприклад `GATEWAY_WS_TLS_PORT`, `GATEWAY_WS_HTTP3_PORT`. Transport-и з однаковим IP protocol і портом можуть ділити listener, де це підтримано ingress-ом. |
| `GATEWAY_DISPATCH_MAX_CONCURRENT` | `256` | Верхня межа одночасних backend forward операцій. Позитивне ціле число. |
| `GATEWAY_DISPATCH_QUEUE_CAPACITY` | `1024` | Кількість заявок у bounded dispatcher queue. Позитивне ціле число; перевищення ліміту спричиняє відхилення без replay. |
| `METRICS_BIND` | `127.0.0.1` | IP bind окремого Prometheus HTTP listener-а. Для віддаленого scrape дозволяйте тільки приватні мережі. |
| `METRICS_PORT` | `9090` | Порт окремого exporter-а; `0` заборонений. Scrape path — `/metrics`. |

Для стандартних транспортів із таблиці нижче `GATEWAY_PORT` автоматично використовується як fallback саме для `tcp` та `udp`. Інші transport-и не вмикаються неявно.

### Всі transport-specific порти Gateway

Відповідність «transport → environment variable» повністю визначається формулою `GATEWAY_<TRANSPORT>_PORT`, де назва transport-а uppercase, а `-` замінюється на `_`:

| Transport | Environment variable | Transport | Environment variable |
|---|---|---|---|
| `tcp` | `GATEWAY_TCP_PORT` | `ws-tcp` | `GATEWAY_WS_TCP_PORT` |
| `tls` | `GATEWAY_TLS_PORT` | `ws-tls` | `GATEWAY_WS_TLS_PORT` |
| `udp` | `GATEWAY_UDP_PORT` | `ws-udp` | `GATEWAY_WS_UDP_PORT` |
| `rtp` | `GATEWAY_RTP_PORT` | `ws-rtp` | `GATEWAY_WS_RTP_PORT` |
| `rudp` | `GATEWAY_RUDP_PORT` | `ws-rudp` | `GATEWAY_WS_RUDP_PORT` |
| `srt` | `GATEWAY_SRT_PORT` | `ws-srt` | `GATEWAY_WS_SRT_PORT` |
| `udplite` | `GATEWAY_UDPLITE_PORT` | `ws-udplite` | `GATEWAY_WS_UDPLITE_PORT` |
| `dccp` | `GATEWAY_DCCP_PORT` | `ws-dccp` | `GATEWAY_WS_DCCP_PORT` |
| `sctp` | `GATEWAY_SCTP_PORT` | `ws-sctp` | `GATEWAY_WS_SCTP_PORT` |
| `quic` | `GATEWAY_QUIC_PORT` | `ws-quic` | `GATEWAY_WS_QUIC_PORT` |
| `http1` | `GATEWAY_HTTP1_PORT` | `ws-http1` | `GATEWAY_WS_HTTP1_PORT` |
| `http3` | `GATEWAY_HTTP3_PORT` | `ws-http3` | `GATEWAY_WS_HTTP3_PORT` |
| `mqtt` | `GATEWAY_MQTT_PORT` | `ws-mqtt` | `GATEWAY_WS_MQTT_PORT` |
| `coap` | `GATEWAY_COAP_PORT` | `ws-coap` | `GATEWAY_WS_COAP_PORT` |
| `amqp` | `GATEWAY_AMQP_PORT` | `ws-amqp` | `GATEWAY_WS_AMQP_PORT` |

`DCCP`, `SCTP` та `UDP-Lite` залежать від підтримки ОС/ядра. Для `srt`/`ws-srt` потрібен gateway binary, зібраний із feature `srt`. Для MQTT/AMQP потрібні зовнішні brokers, їхні URL задаються окремо.

### TLS, SRT, UDP-Lite та broker environment variables

| Змінна | Коли потрібна | Призначення / правила |
|---|---|---|
| `TLS_CERT_PATH` | Коли увімкнено `tls`, `quic`, `http3`, `ws-tls`, `ws-quic` або `ws-http3` ingress | Шлях до PEM certificate chain. |
| `TLS_KEY_PATH` | Ті самі TLS ingress | Шлях до PEM private key, що відповідає сертифікату. Зберігайте з обмеженими правами доступу. |
| `GATEWAY_SRT_PASSPHRASE` | Увімкнено `srt`/`ws-srt` і зібрано feature `srt` | SRT shared passphrase, 10–79 bytes. Не плутати з backend secret-ами, заданими через `srt_passphrase_env`. |
| `GATEWAY_UDPLITE_SEND_CHECKSUM_COVERAGE` | Linux/Android UDP-Lite ingress | Outbound checksum coverage у bytes; default `8`, `0` означає повне покриття, часткове значення має бути не менше `8`. |
| `GATEWAY_UDPLITE_RECV_CHECKSUM_COVERAGE` | Linux/Android UDP-Lite ingress | Inbound checksum coverage у bytes; ті самі правила. |
| `MQTT_BROKER_URL` | Увімкнено gateway `mqtt` або `ws-mqtt` ingress | URL зовнішнього MQTT broker-а. Може містити credentials; зберігайте як secret. |
| `AMQP_BROKER_URL` | Увімкнено gateway `amqp` або `ws-amqp` ingress | URL зовнішнього AMQP broker-а. Може містити credentials; зберігайте як secret. |
| `GATEWAY_BROKER_HEARTBEAT_INTERVAL_MS` | Gateway broker transport-и | Heartbeat broker session у ms; default `10000`. |
| `GATEWAY_BROKER_SESSION_LEASE_MS` | Gateway broker transport-и | Lease broker session у ms; default `45000`, має перевищувати heartbeat. Обидва значення перевіряються, коли увімкнено хоча б один broker ingress. |

### Gateway metrics

Prometheus exporter слухає `http://<METRICS_BIND>:<METRICS_PORT>/metrics` окремо від JSON-RPC ingress. Зокрема експортуються:

- `gateway_backend_circuit_open` — gauge `0/1` для backend/transport;
- `gateway_backend_requests_rejected_total` — запити, відхилені circuit breaker-ом;
- `gateway_dispatch_requests_rejected_total` — відхилення через насичений або закритий dispatcher;
- `gateway_backend_circuit_probe_total` — recovery probe за результатом.

У metric labels немає connection ID або request ID.

## JSON-RPC-2 upstream `.env` файли

В upstream checkout зараз є два файли: `.env` для CLI/demo та налаштувань локальних серверів/клієнтів, і `.env.test` для E2E scripts. Це не gateway configuration і gateway їх не читає. JSON-RPC-2 CLI завантажує базовий `.env` з кореня власного checkout-а; `JSON_RPC_ENV` змінює шлях до base-файла, а `JSON_RPC_ENV_OVERLAY` додає override-файл. Пріоритет налаштувань: CLI options → process environment → overlay → base `.env`. Для upstream CLI фільтр логів читається з `RUST_LOG` (за відсутності встановлюється вбудований debug filter для JSON-RPC targets); gateway натомість налаштовується через обов'язковий `LOG_LEVEL`.

Значення, що зараз збережені у приватних/локальних `.env`, тут не дублюються. Для production створюйте окремі service-specific файли та не переносіть секрети з developer/test конфігурації.

### Основні JSON-RPC-2 налаштування

| Змінна | Default / формат | Призначення |
|---|---|---|
| `JSON_RPC_ENV` | шлях; default — `.env` у корені upstream workspace | Вибрати базовий env-файл JSON-RPC-2 CLI. |
| `JSON_RPC_ENV_OVERLAY` | необов'язковий шлях | Файл override-ів, що накладається поверх base `.env`; значення вже наявної process environment змінної має вищий пріоритет. |
| `JSON_RPC_TRANSPORT` | `all` або comma-separated transport IDs | Transport-и для CLI server/client/demo: `tcp`, `tls`, `udp`, `rtp`, `rudp`, `srt`, `udplite`, `dccp`, `sctp`, `quic`, `http1`, `http3`, `mqtt`, `coap`, `amqp` та відповідні `ws-*` варіанти. Для production зазвичай вибирайте явний список; `all` може включати непідтримувані конкретною ОС transports. |
| `JSON_RPC_BIND` | comma-separated IP addresses | Локальні адреси listener-ів; необхідно для server з локальними transports. Приклад формату: `127.0.0.1,::1`. |
| `JSON_RPC_HOST` | hostname або comma-separated hostnames; default `mobilespace.local` | Адреса(и) серверів для CLI client/demo. |
| `JSON_RPC_CERT` | PEM path; default — тимчасовий `json-rpc-2/cert.pem` | Сертифікат server-а, також default trust source для клієнта, якщо `JSON_RPC_CA` не заданий. |
| `JSON_RPC_KEY` | PEM path або unset | Private key server-а. Якщо unset, demo може згенерувати self-signed key у пам'яті; для deploy використовуйте керований ключ. |
| `JSON_RPC_CA` | PEM CA bundle path або unset | CA, якому довіряє JSON-RPC-2 client під час перевірки TLS-сертифіката. |
| `JSON_RPC_SERVER_NAME` | необов'язкове DNS ім'я | Ім'я TLS server-а, яке перевіряє клієнт; якщо unset, береться з host. |
| `JSON_RPC_SERVER_MAX_CONNECTIONS` | default `4096`, positive integer | Верхня межа одночасно прийнятих server connections. |
| `JSON_RPC_SERVER_MAX_CONCURRENT_REQUESTS` | default `1024`, positive integer | Максимум одночасних server request handlers. |
| `JSON_RPC_MAX_MISSED_PONGS` | default `3`, positive integer | Кількість послідовних missed ping/pong ticks перед `restore()` і reconnect клієнтського транспорту. |
| `JSON_RPC_REQUEST_TIMEOUT_MS` | default `30000`, positive ms | Timeout одного client RPC request. |
| `JSON_RPC_RECONNECT_COOLDOWN_MS` | default `0` | Початкове reconnect backoff клієнта. |
| `JSON_RPC_RECONNECT_MAX_BACKOFF_MS` | default `0` | Верхня межа reconnect backoff; якщо обидва значення ненульові, має бути не менше cooldown. |
| `JSON_RPC_RECONNECT_JITTER_MS` | default `0` | Максимальний випадковий додатковий час до reconnect. |
| `JSON_RPC_RECONNECT_MAX_ELAPSED_MS` | unset | Максимальний час однієї reconnect failure streak; unset означає без цього time budget. |

### TCP і UDP налаштування JSON-RPC-2

Налаштування TCP впливають лише на socket-и, якими керують локальні TCP/TLS/HTTP1/WebSocket TCP adapters; вони не змінюють socket tuning зовнішніх MQTT/AMQP бібліотек.

| Змінна | Default | Призначення |
|---|---:|---|
| `JSON_RPC_TCP_NODELAY` | `true` | Увімкнути TCP_NODELAY, вимикаючи Nagle на підтримуваних adapter-owned TCP sockets. Boolean: `true/false`, `1/0`, `yes/no`, `on/off`. |
| `JSON_RPC_TCP_CONGESTION` | системний профіль | Congestion-control profile: `system` або `bbr`; залежить від ОС/прав. |
| `JSON_RPC_TCP_FASTOPEN` | `false` | Увімкнути TCP Fast Open там, де ОС та adapter підтримують його. |
| `JSON_RPC_TCP_FASTOPEN_QUEUE_LEN` | `1024` | Позитивна довжина server Fast Open queue. |
| `JSON_RPC_UDP_RETRANSMIT_MS` | `50` | Час до UDP retransmit. |
| `JSON_RPC_UDP_MAX_RETRANSMITS` | `5` | Кількість retransmit після першої відправки. |
| `JSON_RPC_UDP_MAX_PACKET_SIZE` | `4096` bytes | UDP packet limit, від `64` до `65507` bytes. |
| `JSON_RPC_UDP_DEDUP_CACHE_CAPACITY` | `4096` | Кількість елементів per-peer deduplication cache. |
| `JSON_RPC_UDP_DEDUP_CACHE_TTL_MS` | `30000` | Час життя deduplication entry. |
| `JSON_RPC_UDPLITE_SEND_CSCOV` | `8` bytes | Outbound UDP-Lite checksum coverage: `0` повне покриття, або часткове не менше `8`. |
| `JSON_RPC_UDPLITE_RECV_CSCOV` | `8` bytes | Inbound UDP-Lite checksum coverage, ті самі правила. |
| `JSON_RPC_SRT_PASSPHRASE` | обов'язкова для `srt`/`ws-srt` | Shared SRT passphrase. Секрет; передавайте через secret manager або права доступу на env-файл. |

### MQTT/AMQP параметри JSON-RPC-2 CLI

Ці налаштування призначені для upstream CLI/client/demo, який безпосередньо підключається до брокера. Вони відрізняються від gateway ingress `MQTT_BROKER_URL`/`AMQP_BROKER_URL` і від backend pool `*_url_env`.

| Змінна | Default | Призначення |
|---|---|---|
| `JSON_RPC_MQTT_URL` | `mqtt://127.0.0.1:1883` | Зовнішній MQTT 3.1.1 broker URL (`mqtt://`/`mqtts://`); credentials не зберігати у відкритому конфігу. |
| `JSON_RPC_MQTT_TOPIC` | `json-rpc-2/requests` | Request topic для звичайного MQTT RPC transport-а. |
| `JSON_RPC_WS_MQTT_TOPIC` | `json-rpc-2/ws/requests` | Request topic для WebSocket/MQTT transport-а. |
| `JSON_RPC_MQTT_OPERATION_TIMEOUT_MS` | `10000` | Timeout однієї broker operation. |
| `JSON_RPC_MQTT_RESPONSE_TIMEOUT_MS` | `30000` | Timeout очікування broker response. |
| `JSON_RPC_MQTT_CA` | unset | Додатковий PEM CA bundle для `mqtts://`; не призначений для gateway TLS ingress. |
| `JSON_RPC_AMQP_URL` | loopback AMQP URL | Зовнішній AMQP 0-9-1 broker URL (`amqp://`/`amqps://`); може містити credentials/vhost. |
| `JSON_RPC_AMQP_QUEUE` | `json-rpc-2.requests` | Request queue звичайного AMQP RPC transport-а. |
| `JSON_RPC_WS_AMQP_QUEUE` | `json-rpc-2.ws.requests` | Request queue WebSocket/AMQP transport-а. |
| `JSON_RPC_AMQP_OPERATION_TIMEOUT_MS` | `10000` | Timeout однієї broker operation. |
| `JSON_RPC_AMQP_RESPONSE_TIMEOUT_MS` | `30000` | Timeout очікування broker response. |

У production broker transports підключайте лише за наявності налаштованого broker-а, credentials, TLS trust і правильно створених topic/queue routes. MQTT/AMQP — зовнішні brokers, а не вбудовані server listeners.

### Порти локальних JSON-RPC-2 transports

Порт задається як `JSON_RPC_<TRANSPORT>_PORT`, а WebSocket варіанти мають префікс `JSON_RPC_WS_`. Якщо transport вибрано для CLI server/client, його локальний port має бути заданий. Порт має бути в діапазоні `1..=65535`.

| Transport | Змінна | Transport | Змінна |
|---|---|---|---|
| `tcp` | `JSON_RPC_TCP_PORT` | `ws-tcp` | `JSON_RPC_WS_TCP_PORT` |
| `tls` | `JSON_RPC_TLS_PORT` | `ws-tls` | `JSON_RPC_WS_TLS_PORT` |
| `udp` | `JSON_RPC_UDP_PORT` | `ws-udp` | `JSON_RPC_WS_UDP_PORT` |
| `rtp` | `JSON_RPC_RTP_PORT` | `ws-rtp` | `JSON_RPC_WS_RTP_PORT` |
| `rudp` | `JSON_RPC_RUDP_PORT` | `ws-rudp` | `JSON_RPC_WS_RUDP_PORT` |
| `srt` | `JSON_RPC_SRT_PORT` | `ws-srt` | `JSON_RPC_WS_SRT_PORT` |
| `coap` | `JSON_RPC_COAP_PORT` | `ws-coap` | `JSON_RPC_WS_COAP_PORT` |
| `dccp` | `JSON_RPC_DCCP_PORT` | `ws-dccp` | `JSON_RPC_WS_DCCP_PORT` |
| `sctp` | `JSON_RPC_SCTP_PORT` | `ws-sctp` | `JSON_RPC_WS_SCTP_PORT` |
| `udplite` | `JSON_RPC_UDPLITE_PORT` | `ws-udplite` | `JSON_RPC_WS_UDPLITE_PORT` |
| `quic` | `JSON_RPC_QUIC_PORT` | `ws-quic` | `JSON_RPC_WS_QUIC_PORT` |
| `http1` | `JSON_RPC_HTTP1_PORT` | `ws-http1` | `JSON_RPC_WS_HTTP1_PORT` |
| `http3` | `JSON_RPC_HTTP3_PORT` | `ws-http3` | `JSON_RPC_WS_HTTP3_PORT` |

`JSON_RPC_MQTT_PORT`, `JSON_RPC_WS_MQTT_PORT`, `JSON_RPC_AMQP_PORT` і `JSON_RPC_WS_AMQP_PORT` — deprecated. MQTT/AMQP працюють через зовнішній URL/topic/queue, а не через локальний listen port; старі broker port options відхиляються.

### Профілі upstream scripts і `.env.test`

Ці параметри використовуються helper scripts та E2E tests, а не production gateway:

| Змінна | Файл | Default / призначення |
|---|---|---|
| `JSON_RPC_BIND_IPV4` | `.env` | IPv4 loopback профіль для `tools/server.sh ipv4`; default `127.0.0.1`. |
| `JSON_RPC_BIND_IPV6` | `.env` | IPv6 loopback профіль для `tools/server.sh ipv6`; default `::1`. |
| `JSON_RPC_BIND_LAN` | `.env` | Bind addresses для `tools/server.sh lan`; default `0.0.0.0,::`, тобто всі інтерфейси. |
| `JSON_RPC_HOST_IPV4` | `.env` | IPv4 destination host для helper scripts; default `127.0.0.1`. |
| `JSON_RPC_HOST_IPV6` | `.env` | IPv6 destination host для helper scripts; default `::1`. |
| `JSON_RPC_E2E_BIND` | `.env.test` | Bind IP list для E2E servers; default loopback IPv4 та IPv6. |
| `JSON_RPC_E2E_HOST` | `.env.test` | Host list для E2E clients; default `127.0.0.1,::1`. |
| `JSON_RPC_E2E_HTTP1_HOST` | `.env.test` | Destination host для HTTP/1 E2E tests; default `127.0.0.1`. |
| `JSON_RPC_E2E_PORT_BASE` | `.env.test` | Base port для test-портів; default `41400`. |

Для сервісів, що використовують лише одну адресу, `tools/server.sh` має профілі `ipv4`, `ipv6`, `lan`. `tools/server-all.sh` може підбирати доступні локальні transports, а broker transports вмикаються окремо через `--brokers`. Це upstream testing/demo scripts, не запуск gateway cluster-а.

Інші test-only environment змінні, що можуть бути потрібні окремим upstream тестам, але не належать до `.env`/`.env.test` runtime-параметрів:

- `MQTT_BROKER_URL`, `AMQP_BROKER_URL` — ізольовані broker-и для integration tests;
- `JSON_RPC_REQUIRE_DCCP=1` — зробити відсутність DCCP помилкою на CI-хості, де DCCP очікується;
- `SRT_LIVE_TRANSMIT` — шлях до зовнішнього executable для спеціального native SRT тесту;
- `DOCKER_BIN` — шлях/назва Docker executable для tests, які піднімають broker containers.

## Gateway `.env` приклади для transport-ів

### TCP backend

Додайте до gateway `.env`:

```dotenv
GATEWAY_TCP_PORT=8443
```

`GATEWAY_PORT` також є fallback для TCP і UDP; задайте явний `GATEWAY_TCP_PORT`, якщо вони мають слухати різні порти. У `backends.json` вкажіть `"ports": {"tcp": 8081}` для внутрішнього backend listener-а.

### TLS ingress і TLS backend

Для TLS ingress:

```dotenv
GATEWAY_TLS_PORT=8444
TLS_CERT_PATH=/etc/json-rpc-gateway/tls/fullchain.pem
TLS_KEY_PATH=/etc/json-rpc-gateway/tls/private-key.pem
```

Для gateway → TLS backend у відповідному backend `options` також встановіть `tls_ca_path`, а якщо ім'я сертифіката не збігається з `host` — `tls_server_name`.

### Broker ingress і backend broker routes

Gateway ingress використовує environment variables `MQTT_BROKER_URL` та `AMQP_BROKER_URL` разом із портами `GATEWAY_MQTT_PORT`/`GATEWAY_AMQP_PORT`. Це окремо від backend connector-а. Для backend broker connector-а в JSON-конфігурації вкажіть, наприклад:

```json
{
  "id": "broker-backend",
  "host": "127.0.0.1",
  "ports": { "mqtt": 1883, "amqp": 5672 },
  "options": {
    "mqtt_url_env": "BACKEND_MQTT_URL",
    "amqp_url_env": "BACKEND_AMQP_URL",
    "mqtt_request_topic": "internal/rpc/requests",
    "amqp_request_queue": "internal.rpc.requests"
  }
}
```

У gateway process environment/secret store мають бути `BACKEND_MQTT_URL` і/або `BACKEND_AMQP_URL`. URL не слід записувати у backend JSON. MQTT/AMQP broker transports у balancer мають відповідати узгодженим маршрутам backend-а.

## Порядок запуску і перевірка

1. Підготуйте broker-и, TLS файли та секрети, якщо вони потрібні вибраним transports.
2. Запустіть один чи кілька backend процесів і перевірте доступність їхніх internal ports з gateway вузла.
3. Переконайтеся, що кожен backend endpoint розуміє `GatewayBackendProtocol`, а не тільки стандартний JSON-RPC wire format.
4. Створіть `backends.json` з endpoint-ами, pool sizes та потрібними options.
5. Створіть gateway `.env`; встановіть `GATEWAY_BIND`, `GATEWAY_PORT`, `CONTROL_PORT`, `BACKEND_CONFIG_PATH`, `LOG_LEVEL`; додайте transport ports і секрети, потрібні тільки для активних transports.
6. Перевірте порти й фаєрвол: ingress доступний потрібним клієнтам, backend ports тільки gateway-у, Control Plane тільки внутрішнім службам, metrics — Prometheus.
7. Запустіть gateway. Він спершу підніме metrics recorder, прочитає backend-конфігурацію і перевірить pool-и, після чого запускає ingress.
8. Перевірте `GET http://<METRICS_BIND>:<METRICS_PORT>/metrics` з дозволеного host-а; для default loopback виконайте `curl http://127.0.0.1:9090/metrics` на gateway node.
9. Надішліть JSON-RPC запит через gateway і перевірте backend application logs та gateway logs. `DEBUG` має payload-и, тому вмикайте його короткочасно й обмежте доступ/retention.

Рекомендована ручна перевірка gateway deployment:

```sh
cd /opt/json-rpc-gateway
cargo run --release --locked -p json-rpc-gateway
```

У production запускайте вже зібраний binary через process manager, а не через `cargo run`. Наприклад, systemd service може містити:

```ini
[Unit]
Description=JSON-RPC Gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=jsonrpc
Group=jsonrpc
WorkingDirectory=/opt/json-rpc-gateway
EnvironmentFile=/etc/json-rpc-gateway/gateway.env
ExecStart=/opt/json-rpc-gateway/target/release/json-rpc-gateway
Restart=on-failure
RestartSec=2
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

TLS key, backend config і `.env` мають бути доступні користувачу сервісу з мінімальними правами. Не включайте broker URLs/passphrases у Git, container image, process arguments чи звичайні deployment logs. Для production краще передавати їх із secret manager-у або обмеженого `EnvironmentFile`.

## Автоматичне LAN-розгортання на одному вузлі

Скрипт [`scripts/deploy-lan-ecosystem.sh`](../scripts/deploy-lan-ecosystem.sh) збирає gateway та сумісний демонстраційний backend, копіює бінарники через SSH і створює systemd services для такої топології:

| Вузол | Сервіси | Порти за замовчуванням |
|---|---|---|
| `192.168.1.132` | Gateway із вбудованим balancer та два базові backend-и | Gateway TCP/UDP `8443`, backend-и TCP/UDP `8081` і `8082`; Control Plane `8500` |

Запускайте скрипт із checkout-у на build-машині: якщо цільовий IP належить
поточному комп'ютеру, встановлення локальне; інакше використовується SSH.
Усі три процеси встановлюються лише на цільовий вузол, окремого backend-а
на build-машині немає. Balancer працює всередині процесу gateway.
IP і порти задаються константами на початку `deploy-lan-ecosystem.sh`:
`gateway_host`, `gateway_port=8443`, `control_port=8500`,
`backend_1_port=8081`, `backend_2_port=8082`, не environment overrides.
Для зміни адреси/ingress порту узгодьте також константи на початку
`start-lan-ecosystem.sh`, `stop-lan-ecosystem.sh` та
`enable-lan-gateway-protocols-node.sh`.
Потрібні Cargo на build-машині, systemd на цільовому вузлі, SSH-ключ для
віддаленого встановлення та sudo-доступ. Для іншого SSH-акаунта задайте
`SSH_USER`, для системного акаунта — `SERVICE_USER`.
Для sudo налаштуйте `NOPASSWD` або передайте `SUDO_PASSWORD` із захищеного
середовища; пароль не записується у файли чи аргументи.
Створюються `json-rpc-gateway.service`, `json-rpc-backend-1.service`,
`json-rpc-backend-2.service`, `gateway.env`, `backend-1.env`, `backend-2.env`
та початковий `backends.json`.
Старі `json-rpc-backend-131` / `json-rpc-backend-132-*` services потрібно
явно зупинити й вимкнути перед міграцією на цільовому вузлі.
Збережений реєстр backend-ів не перезаписується: якщо його ID або endpoints
не відповідають новій топології, installer зупиняється до заміни файлів.
Міграцію state-файлу та виведення старих backend-ів на інших вузлах
виконуйте явно, зі збереженням резервних копій.

```sh
./scripts/deploy-lan-ecosystem.sh
```

### LAN: усі доступні протоколи на одному порту

Додатковий скрипт налаштовує gateway на `8443` для TCP/TLS/HTTP1, UDP/RTP/RUDP/SRT,
QUIC/HTTP3/CoAP, SCTP/UDP-Lite та відповідних `ws-*` варіантів.
Це 24 socket profiles; DCCP та `ws-dccp` не ввімкнено, бо ядро вузла
не підтримує DCCP. Сертифікат і ключ встановлено в
`/etc/json-rpc-ecosystem/tls/` із правами `root:jsonrpc 0640`.
Оновлений сертифікат покриває DNS-імена `express-t.local`, `localhost`
та IP `127.0.0.1`, `192.168.1.132`. Для TLS/QUIC використовуйте server name
`express-t.local` або IP `192.168.1.132`; клієнт має довіряти новому
сертифікату/його CA, а не попередньому `*.express-t.ua`.
Сертифікат чинний до **2027-10-26 13:25:40 UTC**.

MQTT/AMQP та їхні `ws-*` варіанти працюють через локальні Mosquitto/RabbitMQ,
а не як socket listeners на `8443`. Broker-и слухають loopback
`127.0.0.1:1883` і `127.0.0.1:5672`; для віддалених клієнтів потрібен
SSH port forwarding або окремо погоджене захищене відкриття broker endpoints.
Використовуються raw/WebSocket routes за замовчуванням, розділені topics/queues.
Control Plane `list_components` показує 24 socket profiles, не 4 broker subscriptions.
Усі 28 профілів включено через transport-specific змінні `gateway.env`.
SRT passphrase генерується при першому ввімкненні й зберігається лише
в захищеному `gateway.env`; передавайте її клієнтам захищеним каналом.

Для відтворення після встановлення `mosquitto` і `rabbitmq-server`:

```sh
./scripts/build-with-srt.sh build --locked --release \
  -p json-rpc-gateway --bin json-rpc-gateway --features srt
stage="$(ssh dev@192.168.1.132 'mktemp -d /tmp/jrpc-protocols.XXXXXX')"
scp target/release/json-rpc-gateway scripts/enable-lan-gateway-protocols-node.sh \
  /home/dev/Certs/server-fullchain.pem /home/dev/Certs/server-key.pem \
  "dev@192.168.1.132:$stage/"
ssh -t dev@192.168.1.132 \
  "sudo bash '$stage/enable-lan-gateway-protocols-node.sh' '$stage'"
```

Node installer зберігає решту gateway env, налаштовує 28 профілів, додає
постійні UFW before-rules для SCTP (132) та UDP-Lite (136) на порт `8443`
лише з `192.168.1.0/24`, перевіряє готовність і відновлює попередній
бінарник/env у разі timeout. Стандартні TCP/UDP LAN rules повинні вже існувати.
Після першого встановлення RabbitMQ в `/etc/rabbitmq/rabbitmq.conf` задано
`listeners.tcp.1 = 127.0.0.1:5672` та
`distribution.listener.interface = 127.0.0.1`, потім сервіс перезапущено.
Скрипт не встановлює пакети й не змінює конфігурацію broker-ів.

### Запуск і зупинка встановлених сервісів

Після першого розгортання запускайте локально або з build-машини через SSH:

```sh
./scripts/start-lan-ecosystem.sh
./scripts/stop-lan-ecosystem.sh
```

Deployment встановлює базовий `gateway-demo-backend` без прикладних
розширень і сховищ даних. Він надає `echo`, `identify` та вбудований
`rpc.list_methods`. Node installer відмовляється замінювати backend із
власними systemd drop-in `.conf`: спочатку явно перегляньте та приберіть
їх, якщо хочете перейти саме на базовий deployment.

[`start-lan-ecosystem.sh`](../scripts/start-lan-ecosystem.sh) запускає два
backend-и й gateway на одному цільовому вузлі та очікує ingress `8443`.
Він не збирає й не копіює файли, не змінює конфіги чи firewall і не
перезапускає вже активні сервіси.
[`stop-lan-ecosystem.sh`](../scripts/stop-lan-ecosystem.sh) зупиняє ці три
сервіси на тому самому вузлі, зберігаючи автозапуск після reboot.
Обидва скрипти використовують `SSH_USER=dev` та підтримують `SUDO_PASSWORD`;
без нього потрібен `sudo -n`. Пароль не додавайте у скрипти чи репозиторій.
Якщо запуск завершиться помилкою, вже запущені сервіси залишаться працювати
для діагностики.

Базові backend-и не мають бізнесових методів: лише `echo`, `identify` та
`rpc.list_methods`. `identify` повертає `backend-1` або `backend-2`, що
дозволяє перевірити round-robin. Backend-и приймають TCP і UDP на однакових
числових портах. Для production застосунку замініть demo binary і systemd
launch configuration на backend із `GatewayBackendProtocol`.
Основний deployment не змінює firewall. Для LAN доступу дозвольте TCP/UDP
ingress `8443` із `192.168.1.0/24` (наприклад,
`sudo ufw allow from 192.168.1.0/24 to any port 8443 proto tcp` та аналогічно
`proto udp`); не відкривайте Control Plane `8500` публічно.
Backend-порти `8081`/`8082` обмежте до gateway-вузла.
Повторний `deploy-lan-ecosystem.sh` оновлює бінарники/початкові конфіги та
перезапускає services; сумісний збережений реєстр і transport state залишає.

## Circuit breaker та поведінка при відмовах

### Перелік методів через Control Plane

Запит `{"action":"list_methods"}\n` повертає єдиний відсортований список
імен JSON-RPC методів без дублікатів:

```json
{"success":true,"methods":["echo","identify","rpc.list_methods"],"complete":true,"errors":[]}
```

Gateway паралельно запитує **кожен** налаштований backend через
`rpc.list_methods` із `params:{"describe":true}`, незалежно від round-robin. Використовується TCP, якщо він
налаштований, інакше перший транспорт у відсортованій конфігурації. Запит
не викликає бізнесові методи, не займає слоти основного пулу й не змінює
QUIC affinity або circuit breaker. Для кожного backend-а діє timeout:
менше з `operation_timeout_ms` і 5 секунд. Результат не кешується.

Якщо хоча б один backend недоступний, не підтримує discovery чи повернув
некоректну відповідь, повертаються `success:false`,
`error:"MethodDiscoveryIncomplete"`, `complete:false`, доступні `methods`
і `errors:[{"backend_id":"...","error":"DiscoveryFailed"}]`.
Для timeout код — `DiscoveryTimeout`. Порожній список при відмові всіх
backend-ів не є успішною відповіддю.

Це **об'єднання**, не перетин методів: наявність методу в списку означає,
що його оголосив щонайменше один backend, а не що він доступний на кожному.
Discovery не додає маршрутизацію за методом; для неоднакових backend-ів
звичайний round-robin і надалі може вибрати backend без потрібного методу.
Форма `list_components` не змінюється.

`RpcServer::method_names()` у JSON-RPC-2 читає реєстр handlers;
вбудований `rpc.list_methods` повертає цей список та власне ім'я.
`RpcServer::register_alias()` додає реальний alias у той самий реєстр.

Зміни introspection опубліковано в JSON-RPC-2, гілка `start`, commit
`8b8ca2d`. `Cargo.lock` фіксує Git revision; локальний checkout
`.upstream-work/JSON-RPC-2` для збірки не потрібен.

Відповідь також містить `method_details`: схеми параметрів і готові приклади
JSON-RPC запитів. Поле `methods` лишається масивом імен для сумісності.
Приклад одного елемента `method_details`:

```json
{
  "name": "echo",
  "params_schema": {
    "type": ["object", "array"],
    "description": "Returns the supplied params unchanged."
  },
  "example_request": {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "echo",
    "params": {"message": "example"}
  }
}
```

`params_schema` використовує JSON Schema: `required` перелічує обов'язкові
поля об'єкта; інші поля `properties` необов'язкові. Вкладені об'єкти мають
власні `required`.
Приклади містять лише вигадані значення, не конфігурацію, паролі чи токени
користувачів. Вони показують форму запиту, але не гарантують успішної
авторизації чи наявності сутностей; не надсилайте мокові дані в методи,
що записують їх у production сховище.

Метадані реєструються backend-ом через `RpcServer::document_method` після
реєстрації handlers та перед створенням aliases. Rust handlers приймають
динамічний JSON, тому бібліотека не може автоматично вивести їхні параметри.
Для всіх методів поточного backend-а схеми й приклади задано явно.
Незадокументований сторонній handler повертає `params_schema:null` та
`example_request:null`, а не вигаданий порожній набір параметрів.
Backend-и старої версії без підтримки `describe` позначаються як
`DiscoveryFailed`; повноту discovery не підміняємо статичними даними.
Однакові описи об'єднуються; якщо один метод має різні схеми/приклади на
різних backend-ах, `method_details` зберігає окремі варіанти з тим самим `name`.
`complete` означає успішне опитування всіх увімкнених backend-ів, а не наявність
документації для кожного handler-а.

### Гаряче керування backend-ами

Control Plane може додавати вже запущені сервери до балансування,
вимикати їх і повторно вмикати без рестарту gateway чи backend-процесів:

```jsonl
{"action":"list_backends"}
{"action":"add_backend","backend":{"id":"backend-133","host":"192.168.1.133","ports":{"tcp":8081,"udp":8081},"pool_size":4}}
{"action":"set_backend_enabled","backend_id":"backend-133","enabled":false}
{"action":"set_backend_enabled","backend_id":"backend-133","enabled":true}
```

Формат `backend` такий самий, як у початковому `backends.json`, включно з
`options`. Новий backend одразу отримує `enabled:true`; додавання перевіряє
конфігурацію, а не гарантує мережеву доступність сервера. З'єднання
створюються й перевіряються звичайним health-check механізмом. Сервер має
використовувати `RpcServer` із `GatewayBackendProtocol`, не звичайний
JSON-RPC listener без gateway framing. Для TLS потрібні довірений CA та
налаштування server name; broker URLs і SRT passphrase задаються посиланнями
на environment variables gateway, не секретами в командах.

Успішна відповідь містить `success:true` і масив `backends` зі знімками:
`id`, `host`, `enabled`, `active_requests`, `transports`. `active_requests`
враховує поточні backend exchanges та health/discovery операції.
`list_components` показує ті самі поля й загальну кількість зареєстрованих
backend-ів, включно з вимкненими. `list_methods` опитує лише ввімкнені.
Якщо всі вимкнені, RPC відхиляються без replay, а discovery повертає
`complete:false`, порожні методи та `MethodDiscoveryIncomplete`.

Вимкнення одразу прибирає backend із кандидатів для нових запитів та
закриває idle pool connections. Вже прийняті до обробки операції
завершуються в межах своїх timeout-ів; `success:true` для команди означає
застосовану заборону, а не очікування завершення всіх операцій.
QUIC/WS-QUIC сесії, прив'язані до цього backend-а, закриваються після
завершення операцій їхнього покоління. Нові запити таких сесій
відхиляються; gateway не переносить сесію й не повторює RPC на іншому
backend-і. Звичайні TCP/HTTP та інші неприв'язані клієнтські сесії
продовжують працювати з рештою ввімкнених backend-ів.
Повторне ввімкнення допускає нові запити й нові прив'язані сесії, але
ніколи не відновлює старі, навіть якщо попередні операції ще завершуються.

Перший успішний запис зберігає **весь** поточний реєстр, включно зі станом
кожного backend-а, у `/var/lib/json-rpc-gateway/backends.json`.
Шлях можна змінити через `GATEWAY_BACKEND_STATE_PATH`.
Запис атомарний і відбувається до зміни runtime; помилка
`BackendPersistenceFailed` залишає runtime незміненим.
Після створення цього файлу він є авторитетним джерелом реєстру:
початковий `BACKEND_CONFIG_PATH` залишається потрібним для конфігурації
запуску, але його backend entries більше не замінюють збережений реєстр.
За відсутності state-файлу використовується початковий конфіг.
Пошкоджений state-файл, невідома версія, дублікати ID чи недоступний у
поточній збірці transport зупиняють запуск з помилкою, без скидання стану.
Вимкнені backend-и пропускаються під час startup warm-up та maintenance;
увімкнені зберігають стандартну startup перевірку доступності.

Помилки команд: `BackendAlreadyExists`, `BackendNotFound`,
`InvalidBackendConfig`, `BackendPersistenceNotConfigured` і
`BackendPersistenceFailed`. Змінити адресу/порти існуючого ID командою
`add_backend` не можна: зареєструйте новий ID і вимкніть попередній.
Ці команди не запускають і не зупиняють systemd-процеси на вузлах.
Для LAN deployment достатньо вже налаштованого
`StateDirectory=json-rpc-gateway`; для ручного запуску каталог state-файлу
потрібно створити й зробити writable заздалегідь. Control Plane має
залишатися доступним лише довіреним адміністраторам.

### Гаряче керування транспортами

Control Plane дозволяє незалежно вимикати й знову вмикати налаштовані
профілі без рестарту gateway:

```jsonl
{"action":"list_transports"}
{"action":"set_transport_enabled","transport":"quic","enabled":false}
{"action":"set_transport_enabled","transport":"quic","enabled":true}
```

Кожна команда займає окремий рядок TCP Control Plane, як і `list_methods`.
Успішна відповідь містить `success:true` та `transports` — відсортований
масив об'єктів `{"transport":"quic","enabled":false}`. Ідентифікатори
збігаються з ingress (`tcp`, `tls`, `udp`, `http1`, `http3`, `ws-quic`
тощо); raw та `ws-*` перемикаються окремо, включно з MQTT/AMQP.
Можна ввімкнути лише профілі, вже налаштовані через `GATEWAY_*_PORT`
і доступні у збірці. Невідомий або неналаштований профіль повертає
`success:false,error:"TransportNotConfigured"`, а не створює новий listener.

Заборона скасовує активні сесії вибраного профілю, зокрема idle-з'єднання,
прибирає їхні client/group registrations та QUIC backend affinity і
відхиляє нові запити. Повторне ввімкнення допускає нові сесії, але не
відновлює старі. Спільні сокети залишаються прив'язаними: TCP/TLS/QUIC
handshake на спільному порту сам по собі не означає дозволений RPC.
HTTP-профілі відмовляють HTTP 503 або закривають з'єднання; datagram-профілі
відкидають пакети та очищають власні reliability caches. Інші протоколи на
тому самому порту не перезапускаються. На спільному HTTP/3 connection
скасовуються лише відповідні request/WebSocket streams: сам QUIC connection
може залишатися відкритим для іншого дозволеного HTTP/3 профілю.
MQTT/AMQP надсилають клієнтам session `close`, зберігаючи broker subscription
для повторного ввімкнення. Вже передані backend-у бізнес-операції можуть
завершитися; заборона транспорту не є rollback або дозволом повторити RPC.

Стан зберігається атомарною заміною JSON-файлу **до** застосування команди.
Типовий шлях — `/var/lib/json-rpc-gateway/transports.json`; його можна змінити
через `GATEWAY_TRANSPORT_STATE_PATH`. Помилка запису повертає
`TransportPolicyPersistenceFailed` і не змінює поточну політику.
Пошкоджений файл, невідома версія або профіль, недоступний у поточній збірці,
зупиняють запуск з помилкою; немає непомітного скидання заборон.
Тимчасово прибрані з ingress конфігурації заборони зберігаються у файлі.
`list_components` і далі показує прив'язані socket profiles; фактичний дозвіл
потрібно читати через `list_transports`.

LAN deployment створює writable systemd `StateDirectory=json-rpc-gateway`
із mode `0750`, сумісний із `ProtectSystem=strict`. Для ручного запуску
підготуйте writable каталог і задайте, наприклад,
`GATEWAY_TRANSPORT_STATE_PATH="$PWD/var/transports.json"`; каталог `var`
потрібно створити заздалегідь. Для наявного systemd unit потрібен такий самий
`StateDirectory` drop-in і `systemctl daemon-reload`; скрипт увімкнення
LAN-протоколів встановлює його автоматично.

### Перелік компонентів через Control Plane

Надішліть у TCP `CONTROL_PORT` один рядок `{"action":"list_components"}\n`.
Для поточного LAN deployment запит можна виконати на `.132` (Control Plane
не потрібно відкривати firewall-ом):

```sh
printf '%s\n' '{"action":"list_components"}' |
  ssh dev@192.168.1.132 'nc -N 192.168.1.132 8500'
```

Відповідь має форму `{"success":true,"components":{"gateway":{...},"balancer":{...},"backends":[...]}}`.
`gateway` містить `version`, `pid`, `uptime_seconds`, фактичні `listeners`
(`transport`, `address`), `control_address` і `metrics_address`.
`balancer` містить `version`, `execution: "in_process"` та `backend_count`.
Кожен backend має `id`, `host`, `enabled`, `active_requests`, список `transports`; кожен транспорт —
`transport`, `port`, `pool_size`, `connected_idle_slots`, `empty_idle_slots`,
`busy_slots` та `circuit`. Circuit: `closed` з `consecutive_failures`,
`open` з `retry_after_ms` або `half_open` з `probe_in_flight`.
Це локальний неатомарний знімок без активного health check: зайняті слоти
не очікуються, а наявність connection чи закритого circuit не є доказом
запущеного backend-процесу. Список включає всі налаштовані backend-и, навіть
недоступні. PID/версія/uptime віддалених процесів і секретні options не повертаються.

Кожен backend/transport має незалежний circuit breaker. За замовчуванням він відкривається після 5 послідовних transport/health-check помилок на 10 секунд; після cooldown дозволяється одна half-open recovery probe. У звичайному round-robin запиті відкритий endpoint пропускається на користь іншого доступного backend-а. QUIC/WS-QUIC сесії лишаються pinned до обраного backend-а. Запити автоматично не повторюються: gateway не може знати, чи backend уже встиг виконати попередній запит.

JSON-RPC business error від доступного backend-а не вважається transport failure для circuit breaker-а. Якщо всі звичайні endpoints недоступні, запит відхиляється; для pinned session помилка віддається без failover.

## Короткий список перевірки перед production

- [ ] Backend binary використовує `GatewayBackendProtocol` і порт, вказаний у backend config.
- [ ] У `backends` є щонайменше один endpoint; `id` унікальні, `ports` непорожні, `pool_size > 0`.
- [ ] TLS CA/hostname перевіряються; private key і broker/SRT secrets захищені.
- [ ] Gateway ingress і backend transports обмежені до потрібних мереж; Control Plane не публічний.
- [ ] `METRICS_BIND` доступний тільки Prometheus/операційній мережі.
- [ ] `LOG_LEVEL=info` у звичайній роботі; DEBUG включається лише на час діагностики з урахуванням чутливих даних.
- [ ] MQTT/AMQP broker integration та OS-specific transports перевірені саме у deployment environment.
- [ ] Gateway і backend-и запускаються окремими supervised services з restart policy та контрольованим shutdown.
