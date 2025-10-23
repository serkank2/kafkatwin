# KafkaTwin - Enterprise Kafka Multi-Cluster Proxy

KafkaTwin, birden fazla Apache Kafka cluster'ını tek bir endpoint üzerinden yönetmeyi ve veri senkronizasyonunu otomatik olarak sağlamayı hedefleyen enterprise-grade bir proxy servisidir.

## 🎯 Özellikler

### Core Features
- ✅ **Tek Endpoint**: Tüm producer ve consumer'lar tek bir proxy endpoint'e bağlanır
- ✅ **Multi-Cluster Yazma**: Producer'dan gelen her mesaj otomatik olarak tüm cluster'lara yazılır
- ✅ **Multi-Cluster Okuma**: Consumer group'ları tüm cluster'lardan koordineli şekilde veri okur
- ✅ **Otomatik Hata Yönetimi**: Cluster failure'larında otomatik retry ve fallback
- ✅ **Circuit Breaker**: Sürekli fail olan cluster'ları geçici olarak devre dışı bırakma
- ✅ **Health Monitoring**: Sürekli cluster health kontrolü ve otomatik recovery

### Advanced Features
- ✅ **Schema Registry Integration**: Avro, JSON, Protobuf schema yönetimi
- ✅ **Message Transformation**: Runtime message dönüşüm engine'i
- ✅ **Rate Limiting & Quotas**: Client bazlı rate limiting ve quota management
- ✅ **Admin REST API**: Comprehensive management API
- ✅ **Web Dashboard**: Real-time monitoring ve yönetim UI'ı
- ✅ **Multi-DC Support**: Geo-distributed cluster'lar için optimizasyonlar
- ✅ **Prometheus Metrics**: Detaylı metrik ve monitoring
- ✅ **Kubernetes Ready**: Native Kubernetes deployment support
- ✅ **Flexible Configuration**: YAML config, environment variable override

## 🏗️ Mimari

```
┌─────────────────────────────────────────────────────────────────────┐
│                        KafkaTwin Proxy                               │
│                                                                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────────┐  │
│  │ Protocol │  │ Producer │  │ Consumer │  │  Transformation    │  │
│  │ Handler  │  │ Handler  │  │ Handler  │  │      Engine        │  │
│  └─────┬────┘  └────┬─────┘  └────┬─────┘  └─────────┬──────────┘  │
│        │            │             │                   │             │
│  ┌─────┴────────────┴─────────────┴───────────────────┴──────────┐  │
│  │              Cluster Manager & Metadata Cache                 │  │
│  │  (Health Monitoring, Circuit Breaker, Connection Pool)        │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌──────────────────┐  ┌───────────────┐  ┌────────────────────┐  │
│  │ Schema Registry  │  │  Rate Limiter │  │   Multi-DC Manager │  │
│  └──────────────────┘  └───────────────┘  └────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
         │                  │                  │
         ▼                  ▼                  ▼
   ┌──────────┐       ┌──────────┐       ┌──────────┐
   │ Kafka    │       │ Kafka    │       │ Kafka    │
   │ DC-1     │       │ DC-2     │       │ DC-3     │
   └──────────┘       └──────────┘       └──────────┘
```

## 📋 Gereksinimler

- Go 1.21 veya üzeri
- Apache Kafka 2.8.0 veya üzeri (backend clusters)
- (Opsiyonel) Schema Registry
- (Opsiyonel) Redis/etcd (offset storage için)
- Linux, macOS veya Windows

## 🚀 Hızlı Başlangıç

### Makefile ile Hızlı Kurulum (Önerilen)

```bash
# Projeyi clone edin
git clone https://github.com/serkank2/kafkatwin.git
cd kafkatwin

# Tüm servisleri başlatın (Kafka, Zookeeper, KafkaTwin, Prometheus, Grafana)
make docker-up

# Logları izleyin
make docker-logs-proxy
```

Bu komut otomatik olarak:
- 2 Kafka cluster'ı (cluster-1, cluster-2)
- Schema Registry
- Prometheus
- Grafana
- KafkaTwin Proxy

hizmetlerini başlatır.

**Erişim Noktaları:**
- KafkaTwin Proxy: `localhost:9092`
- Admin Web UI: `http://localhost:8090`
- Health Check: `http://localhost:8080/health`
- Prometheus Metrics: `http://localhost:9090/metrics`
- Prometheus UI: `http://localhost:9091`
- Grafana: `http://localhost:3000` (admin/admin)

### Manuel Binary Kurulum

```bash
# Bağımlılıkları indirin
go mod download

# Build edin
make build

# Çalıştırın
./bin/kafkatwin -config config.yaml
```

### Docker ile Kurulum

```bash
# Docker image build edin
make docker-build

# Çalıştırın
docker run -p 9092:9092 -p 8080:8080 -p 8090:8090 -p 9090:9090 \
  -v $(pwd)/config.yaml:/app/config.yaml \
  kafkatwin:latest
```

### Kubernetes Deployment

```bash
# ConfigMap oluşturun
kubectl apply -f deployments/kubernetes/configmap.yaml

# Deployment oluşturun
kubectl apply -f deployments/kubernetes/deployment.yaml

# Service oluşturun
kubectl apply -f deployments/kubernetes/service.yaml

# HPA (opsiyonel)
kubectl apply -f deployments/kubernetes/hpa.yaml

# PDB (opsiyonel)
kubectl apply -f deployments/kubernetes/pdb.yaml

# ServiceMonitor (Prometheus Operator ile kullanım için)
kubectl apply -f deployments/kubernetes/pdb.yaml
```

## 🎮 Kullanım Örnekleri

### Producer Örneği

```go
package main

import (
    "github.com/IBM/sarama"
)

func main() {
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true

    // KafkaTwin proxy'ye bağlan
    producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    // Mesaj gönder - otomatik olarak tüm cluster'lara yazılacak
    msg := &sarama.ProducerMessage{
        Topic: "my-topic",
        Value: sarama.StringEncoder("Hello KafkaTwin!"),
    }

    partition, offset, err := producer.SendMessage(msg)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
```

### Consumer Örneği

```go
package main

import (
    "github.com/IBM/sarama"
)

func main() {
    config := sarama.NewConfig()
    config.Consumer.Return.Errors = true

    // KafkaTwin proxy'ye bağlan
    consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    // Topic'ten oku - otomatik olarak tüm cluster'lardan merge edilecek
    partitionConsumer, err := consumer.ConsumePartition("my-topic", 0, sarama.OffsetNewest)
    if err != nil {
        panic(err)
    }
    defer partitionConsumer.Close()

    for msg := range partitionConsumer.Messages() {
        fmt.Printf("Received: %s\n", string(msg.Value))
    }
}
```

## ⚙️ Konfigürasyon

Detaylı konfigürasyon örneği için `configs/example-config.yaml` dosyasına bakın.

### Temel Konfigürasyon

```yaml
server:
  listen_address: "0.0.0.0"
  port: 9092

clusters:
  - id: "cluster-1"
    bootstrap_servers:
      - "kafka1.example.com:9092"
    priority: 1
    weight: 100

producer:
  ack_policy: "MAJORITY"
  timeout: 30s

consumer:
  max_poll_records: 500
  offset_storage:
    type: "memory"

admin_api:
  enabled: true
  port: 8090
  web_ui: true

monitoring:
  metrics:
    enabled: true
    port: 9090
  health:
    enabled: true
    port: 8080
```

## 🧪 Testing

### Unit Testleri Çalıştır

```bash
# Tüm testleri çalıştır
make test

# Coverage report ile
make test-coverage

# Coverage HTML raporu görüntüle
open coverage/coverage.html  # macOS
xdg-open coverage/coverage.html  # Linux
```

### Benchmark Testleri

```bash
make bench
```

### Integration Testleri

```bash
# Docker compose ile test ortamı başlat
make docker-up

# Testleri çalıştır
go test -v -tags=integration ./...

# Temizle
make docker-down
```

## 📊 Admin API & Web UI

KafkaTwin, kapsamlı bir Admin API ve Web Dashboard sunar.

### Web Dashboard

Web UI'ya erişim: `http://localhost:8090`

Dashboard özellikleri:
- **Real-time Monitoring**: Cluster health, throughput, latency grafikleri
- **Topic Management**: Topic listesi, partition detayları, offset bilgileri
- **Schema Registry**: Schema görüntüleme, kayıt, uyumluluk kontrolü
- **Transformation Rules**: Mesaj dönüşüm kurallarını yönetme
- **Quota Management**: Client bazlı rate limit ve quota ayarları
- **System Metrics**: Detaylı Prometheus metrikleri ve trend grafikleri

![Web Dashboard Screenshot](docs/images/dashboard.png)

### API Endpoints

**Cluster Yönetimi:**
```bash
GET /api/v1/clusters              # Tüm cluster'ları listele
GET /api/v1/clusters/{id}         # Cluster detayları
GET /api/v1/clusters/{id}/health  # Cluster health
```

**Topic Yönetimi:**
```bash
GET /api/v1/topics                # Tüm topic'leri listele
GET /api/v1/topics/{topic}        # Topic detayları
```

**Schema Registry:**
```bash
GET  /api/v1/schemas/subjects                           # Subject listesi
GET  /api/v1/schemas/subjects/{subject}/versions/latest # Latest schema
POST /api/v1/schemas/subjects/{subject}                 # Schema kaydet
```

**Transformations:**
```bash
GET    /api/v1/transformations/{topic}         # Kuralları listele
POST   /api/v1/transformations/{topic}         # Kural ekle
DELETE /api/v1/transformations/{topic}/{id}    # Kural sil
```

**Quotas:**
```bash
GET    /api/v1/quotas/{client_id}  # Quota görüntüle
PUT    /api/v1/quotas/{client_id}  # Quota ayarla
DELETE /api/v1/quotas/{client_id}  # Quota sil
```

## 🔄 Message Transformation

Message transformation örneği:

```bash
curl -X POST http://localhost:8090/api/v1/transformations/my-topic \
  -H "Content-Type: application/json" \
  -d '{
    "id": "mask-pii",
    "name": "Mask PII Data",
    "enabled": true,
    "priority": 10,
    "conditions": [
      {
        "type": "field_exists",
        "field": "ssn"
      }
    ],
    "actions": [
      {
        "type": "mask_field",
        "field": "ssn"
      },
      {
        "type": "set_header",
        "field": "pii-masked",
        "value": "true"
      }
    ]
  }'
```

Desteklenen transformation action'lar:
- `set_field` - Alan değeri ayarla
- `remove_field` - Alan sil
- `rename_field` - Alan adını değiştir
- `mask_field` - Alan değerini maskele
- `set_header` - Kafka header ekle
- `uppercase_field` - Büyük harfe çevir
- `lowercase_field` - Küçük harfe çevir

## 🌍 Multi-DC Support

Multi-datacenter konfigürasyonu:

```yaml
multi_dc:
  enabled: true
  strategy: "active-active"
  local_dc: "dc1"
  prefer_local_reads: true
  datacenters:
    - id: "dc1"
      name: "US East"
      region: "us-east-1"
      priority: 1
      cluster_ids: ["cluster-1", "cluster-2"]

    - id: "dc2"
      name: "EU West"
      region: "eu-west-1"
      priority: 2
      cluster_ids: ["cluster-3", "cluster-4"]
```

Replication stratejileri:
- **active-active**: Tüm DC'lere eşzamanlı yazma
- **active-passive**: Primary DC'ye yazma, diğerleri standby
- **regional-active**: Bölge içi aktif, bölgeler arası async
- **preferred**: Local DC tercih edilir, fallback var

## 📈 Prometheus Metrics

Temel metrikler:

```
# Produce metrics
kafkatwin_produce_requests_total{topic, cluster, status}
kafkatwin_produce_latency_seconds{topic, cluster}
kafkatwin_produce_bytes_total{topic, cluster}

# Fetch metrics
kafkatwin_fetch_requests_total{topic, cluster, status}
kafkatwin_fetch_latency_seconds{topic, cluster}
kafkatwin_fetch_bytes_total{topic, cluster}

# Cluster metrics
kafkatwin_cluster_health_status{cluster}
kafkatwin_cluster_connections_active{cluster}

# Consumer group metrics
kafkatwin_consumer_group_members{group}
kafkatwin_consumer_group_rebalance_total{group}
kafkatwin_consumer_lag{group, topic, partition, cluster}
```

## 🎯 Ack Policy'leri

| Policy | Açıklama | Consistency | Performance |
|--------|----------|-------------|-------------|
| **ALL_CLUSTERS** | Tüm cluster'lardan ack | ⭐⭐⭐⭐⭐ | ⭐ |
| **MAJORITY** | Çoğunluk'tan ack | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| **QUORUM** | N cluster'dan ack | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **ANY** | Herhangi birinden ack | ⭐ | ⭐⭐⭐⭐⭐ |

## 🚦 Rate Limiting

Rate limit ayarlama:

```bash
curl -X PUT http://localhost:8090/api/v1/quotas/client-1 \
  -H "Content-Type: application/json" \
  -d '{
    "produce_byte_rate": 10485760,  # 10MB/s
    "fetch_byte_rate": 52428800,    # 50MB/s
    "request_rate": 1000            # 1000 req/s
  }'
```

## 🛣️ Roadmap

- [x] Core multi-cluster produce/consume
- [x] Health monitoring & circuit breaker
- [x] Prometheus metrics
- [x] Consumer group coordination
- [x] Schema Registry integration
- [x] Message transformation
- [x] Rate limiting & quotas
- [x] Admin REST API
- [x] Web UI
- [x] Kubernetes support
- [x] Multi-DC support
- [ ] Full Kafka wire protocol
- [ ] Kubernetes Operator
- [ ] Data encryption at rest
- [ ] Kafka Streams support
- [ ] Kafka Connect integration

## 📄 Lisans

MIT License - Detaylar için `LICENSE` dosyasına bakın.

## 🙏 Credits

- [Sarama](https://github.com/IBM/sarama) - Kafka client
- [Prometheus](https://prometheus.io/) - Monitoring
- [Uber Zap](https://github.com/uber-go/zap) - Logging
- [Gorilla Mux](https://github.com/gorilla/mux) - HTTP router

---

⭐ **Star** this repo if you find it useful!
