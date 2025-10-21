# KafkaTwin - Kafka Multi-Cluster Proxy

KafkaTwin, birden fazla Apache Kafka cluster'ını tek bir endpoint üzerinden yönetmeyi ve veri senkronizasyonunu otomatik olarak sağlamayı hedefleyen bir proxy servisidir.

## 🎯 Özellikler

- **Tek Endpoint**: Tüm producer ve consumer'lar tek bir proxy endpoint'e bağlanır
- **Multi-Cluster Yazma**: Producer'dan gelen her mesaj otomatik olarak tüm cluster'lara yazılır
- **Multi-Cluster Okuma**: Consumer group'ları tüm cluster'lardan koordineli şekilde veri okur
- **Otomatik Hata Yönetimi**: Cluster failure'larında otomatik retry ve fallback
- **Circuit Breaker**: Sürekli fail olan cluster'ları geçici olarak devre dışı bırakma
- **Health Monitoring**: Sürekli cluster health kontrolü
- **Detaylı Monitoring**: Prometheus metrics, structured logging, distributed tracing
- **Dinamik Cluster Yönetimi**: Runtime'da cluster ekleme ve çıkarma (planned)
- **Flexible Configuration**: YAML config, environment variable override

## 🏗️ Mimari

```
┌─────────────────────────────────────────────────────────────┐
│                      KafkaTwin Proxy                        │
│                                                             │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │  Protocol   │  │   Producer   │  │    Consumer      │  │
│  │   Handler   │  │   Handler    │  │    Handler       │  │
│  └─────────────┘  └──────────────┘  └──────────────────┘  │
│         │                 │                    │           │
│  ┌──────┴─────────────────┴────────────────────┴────────┐ │
│  │              Cluster Manager                          │ │
│  │  (Health Monitoring, Circuit Breaker, Conn Pool)     │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
         │                  │                   │
         ▼                  ▼                   ▼
   ┌──────────┐      ┌──────────┐       ┌──────────┐
   │ Kafka    │      │ Kafka    │       │ Kafka    │
   │ Cluster  │      │ Cluster  │       │ Cluster  │
   │    1     │      │    2     │       │    3     │
   └──────────┘      └──────────┘       └──────────┘
```

## 📋 Gereksinimler

- Go 1.21 veya üzeri
- Apache Kafka 2.8.0 veya üzeri (backend clusters)
- Linux, macOS veya Windows

## 🚀 Kurulum

### Binary ile Kurulum

```bash
# Projeyi clone edin
git clone https://github.com/serkank2/kafkatwin.git
cd kafkatwin

# Build edin
go build -o kafkatwin ./cmd/proxy

# Çalıştırın
./kafkatwin -config config.yaml
```

### Docker ile Kurulum

```bash
# Docker image build edin
docker build -t kafkatwin:latest .

# Çalıştırın
docker run -p 9092:9092 -v $(pwd)/config.yaml:/config.yaml kafkatwin:latest
```

## ⚙️ Konfigürasyon

Detaylı konfigürasyon örneği için `configs/example-config.yaml` dosyasına bakın.

### Minimal Konfigürasyon

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

  - id: "cluster-2"
    bootstrap_servers:
      - "kafka2.example.com:9092"
    priority: 1
    weight: 100

producer:
  ack_policy: "MAJORITY"  # ALL_CLUSTERS, MAJORITY, ANY, QUORUM
  timeout: 30s
  max_retries: 3

consumer:
  max_poll_records: 500
  session_timeout: 10s
  offset_storage:
    type: "memory"  # memory, redis, etcd, kafka

monitoring:
  metrics:
    enabled: true
    port: 9090
  health:
    enabled: true
    port: 8080
  logging:
    level: "INFO"
    format: "json"
```

### Cluster Security Konfigürasyonu

```yaml
clusters:
  - id: "secure-cluster"
    bootstrap_servers:
      - "kafka.example.com:9093"
    security:
      protocol: "SASL_SSL"
      sasl:
        enabled: true
        mechanism: "SCRAM-SHA-256"
        username: "kafka-user"
        password: "kafka-password"
      tls:
        enabled: true
        ca_file: "/path/to/ca-cert"
        cert_file: "/path/to/client-cert"
        key_file: "/path/to/client-key"
```

## 📊 Monitoring

### Prometheus Metrics

KafkaTwin, Prometheus formatında detaylı metrikler expose eder:

```bash
curl http://localhost:9090/metrics
```

**Temel Metrikler:**

- `kafkatwin_produce_requests_total` - Toplam produce request sayısı
- `kafkatwin_produce_latency_seconds` - Produce latency histogram
- `kafkatwin_fetch_requests_total` - Toplam fetch request sayısı
- `kafkatwin_cluster_health_status` - Cluster health durumu (1=healthy, 0=unhealthy)
- `kafkatwin_active_connections` - Aktif client connection sayısı
- `kafkatwin_consumer_group_members` - Consumer group member sayısı
- `kafkatwin_errors_total` - Toplam hata sayısı

### Health Checks

```bash
# Liveness probe
curl http://localhost:8080/health/live

# Readiness probe
curl http://localhost:8080/health/ready

# Detailed health check
curl http://localhost:8080/health
```

### Logging

KafkaTwin, structured JSON logging kullanır:

```json
{
  "level": "info",
  "timestamp": "2024-01-15T10:30:45Z",
  "msg": "Successfully produced to cluster",
  "cluster": "cluster-1",
  "topic": "my-topic",
  "messages": 100,
  "latency": "15ms"
}
```

## 🔧 Kullanım

### Producer Örneği (Go)

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

    // Mesaj gönder - otomatik olarak tüm cluster'lara yazılır
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

### Consumer Örneği (Go)

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

    // Partition consumer oluştur
    partitionConsumer, err := consumer.ConsumePartition("my-topic", 0, sarama.OffsetNewest)
    if err != nil {
        panic(err)
    }
    defer partitionConsumer.Close()

    // Mesajları oku - tüm cluster'lardan merge edilmiş data
    for msg := range partitionConsumer.Messages() {
        fmt.Printf("Message: %s\n", string(msg.Value))
    }
}
```

## 🎯 Ack Policy'leri

KafkaTwin, farklı consistency gereksinimleri için multiple ack policy destekler:

- **ALL_CLUSTERS**: Tüm cluster'lardan ack gelene kadar bekle (en güvenli, en yavaş)
- **MAJORITY**: Cluster'ların çoğunluğundan ack geldiğinde başarılı say (balanced)
- **ANY**: Herhangi bir cluster'dan ack geldiğinde başarılı say (en hızlı, en az güvenli)
- **QUORUM**: Belirlenen quorum sayısından ack geldiğinde başarılı say (configurable)

## 🔍 Troubleshooting

### Cluster Bağlantı Sorunları

```bash
# Health check ile cluster durumunu kontrol edin
curl http://localhost:8080/health

# Loglarda cluster connection hatalarını arayın
docker logs kafkatwin | grep "cluster.*failed"

# Cluster health metrics'lerini kontrol edin
curl http://localhost:9090/metrics | grep cluster_health_status
```

### Yüksek Latency

```bash
# Latency metrics'lerini kontrol edin
curl http://localhost:9090/metrics | grep latency

# Circuit breaker durumunu kontrol edin
curl http://localhost:8080/health
```

## 🛣️ Roadmap

- [x] Core multi-cluster produce/consume functionality
- [x] Health monitoring ve circuit breaker
- [x] Prometheus metrics
- [x] Consumer group coordination
- [ ] Full Kafka wire protocol implementation
- [ ] Schema Registry integration
- [ ] Message transformation
- [ ] Rate limiting ve quota management
- [ ] Admin API (REST)
- [ ] Web UI
- [ ] Kubernetes operator
- [ ] Multi-DC support

## 🤝 Katkıda Bulunma

Katkılarınızı bekliyoruz! Lütfen şu adımları takip edin:

1. Fork edin
2. Feature branch oluşturun (`git checkout -b feature/amazing-feature`)
3. Değişikliklerinizi commit edin (`git commit -m 'Add amazing feature'`)
4. Branch'inizi push edin (`git push origin feature/amazing-feature`)
5. Pull Request açın

## 📄 Lisans

Bu proje MIT lisansı altında lisanslanmıştır. Detaylar için `LICENSE` dosyasına bakın.

## 📧 İletişim

Sorularınız veya önerileriniz için issue açabilirsiniz.

## 🙏 Teşekkürler

- [Sarama](https://github.com/IBM/sarama) - Kafka client library
- [Prometheus](https://prometheus.io/) - Monitoring ve alerting
- [Uber Zap](https://github.com/uber-go/zap) - Structured logging

---

**Not**: Bu proje production-ready değildir ve aktif geliştirme aşamasındadır. Production kullanımı için kapsamlı test ve validasyon gereklidir.
