package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	KAFKA_BROKERS    = "kafka:9092"
	KAFKA_TOPIC      = "pgserver1.cdc_schema.clients"
	MONGO_URI        = "mongodb://root:rootpassword@mongo:27017"
	MONGO_DATABASE   = "cdc_db"
	MONGO_COLLECTION = "clients"

	consumerGroupID = "go-cdc-processor-group"
)

type DebeziumPayload struct {
	Schema  struct{} `json:"schema"`
	Payload struct {
		Before   map[string]interface{} `json:"before"`
		After    map[string]interface{} `json:"after"`
		Source   map[string]interface{} `json:"source"`
		Op       string                 `json:"op"`
		TsMs     int64                  `json:"ts_ms"`
		Sequence string                 `json:"sequence"`
	} `json:"payload"`
}

var (
	mongoClient *mongo.Client
	mongoDB     *mongo.Database
)

// A função getEnvWithFallback tenta ler uma variável de ambiente,
// mas retorna o fallback se ela estiver vazia.
func getEnvWithFallback(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}

func main() {
	log.Println("Iniciando Microserviço CDC...")

	kafkaBrokers := getEnvWithFallback("KAFKA_BROKERS", KAFKA_BROKERS)
	kafkaTopic := getEnvWithFallback("KAFKA_TOPIC", KAFKA_TOPIC)
	mongoURI := getEnvWithFallback("MONGO_URI", MONGO_URI)
	mongoDatabaseName := getEnvWithFallback("MONGO_DATABASE", MONGO_DATABASE)
	mongoCollectionName := getEnvWithFallback("MONGO_COLLECTION", MONGO_COLLECTION)

	// A checagem de erro não é mais estritamente necessária se garantirmos
	// que o fallback sempre tem valores válidos, mas é mantida por segurança.
	if kafkaBrokers == "" || kafkaTopic == "" || mongoURI == "" || mongoDatabaseName == "" || mongoCollectionName == "" {
		log.Fatal("Erro: Não foi possível obter todas as configurações necessárias, mesmo com os valores padrão.")
	}

	// 2. Conectar ao MongoDB
	if err := connectMongoDB(mongoURI, mongoDatabaseName); err != nil {
		log.Fatalf("Falha ao conectar ao MongoDB: %v", err)
	}
	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			log.Printf("Erro ao desconectar do MongoDB: %v", err)
		}
	}()
	log.Printf("Conectado ao MongoDB: %s", mongoDatabaseName)

	// 3. Configurar e Rodar o Consumer Kafka
	log.Printf("Conectando ao Kafka em %s, Tópico: %s", kafkaBrokers, kafkaTopic)
	runConsumer(kafkaBrokers, kafkaTopic, mongoCollectionName)
}

// ... Resto das funções connectMongoDB, runConsumer e processMessage ...
// (O resto do código permanece idêntico à última versão funcional.)

// A função connectMongoDB inicializa o cliente MongoDB.
func connectMongoDB(uri, dbName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}

	if err = client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("falha no ping do MongoDB: %w", err)
	}

	mongoClient = client
	mongoDB = client.Database(dbName)
	return nil
}

// A função runConsumer configura e inicia o loop de consumo do Kafka.
func runConsumer(brokers, topic string, collectionName string) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           consumerGroupID,
		"auto.offset.reset":  "latest",
		"enable.auto.commit": true,
	})

	if err != nil {
		log.Fatalf("Falha ao criar o consumer Kafka: %v", err)
	}
	defer consumer.Close()

	// Inscreve-se no tópico
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Falha ao se inscrever no tópico %s: %v", topic, err)
	}

	// Loop principal de processamento
	for {
		msg, err := consumer.ReadMessage(time.Second)
		if err == nil {
			log.Printf("Mensagem recebida do Tópico %s [%d] em Offset %v", *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
			processMessage(msg.Value, collectionName)
		} else if !err.(kafka.Error).IsTimeout() {
			log.Printf("Erro do consumer: %v\n", err)
		}
	}
}

// A função processMessage decodifica o payload do Debezium e executa a operação no MongoDB.
func processMessage(value []byte, collectionName string) {
	var dbzPayload DebeziumPayload
	if err := json.Unmarshal(value, &dbzPayload); err != nil {
		log.Printf("ERRO: Falha ao desserializar JSON do Debezium: %v. Mensagem: %s", err, string(value))
		return
	}

	payload := dbzPayload.Payload
	op := payload.Op
	collection := mongoDB.Collection(collectionName)

	var documentData map[string]interface{}
	var entityID interface{}

	if op == "d" {
		documentData = payload.Before
	} else if payload.After != nil {
		documentData = payload.After
	} else if op == "c" || op == "r" || op == "u" {
		log.Printf("AVISO: Operação '%s' sem payload 'After' para atualização/criação.", op)
		return
	}

	// Tenta extrair o ID.
	if documentData != nil {
		if id, ok := documentData["id"]; ok {
			entityID = id
		} else {
			log.Printf("ERRO: O campo 'id' (chave primária) não foi encontrado no payload CDC. Não é possível processar a operação '%s'.", op)
			return
		}
	} else {
		log.Printf("AVISO: Nenhum dado Before ou After presente. Operação ignorada: %s", op)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 4. Executar Operação no MongoDB
	filter := bson.D{{Key: "_id", Value: entityID}}

	switch op {
	case "c", "r", "u": // Create, Read (Snapshot), Update
		documentData["_id"] = entityID
		delete(documentData, "id")

		update := bson.M{"$set": documentData}

		opts := options.Update().SetUpsert(true)

		res, err := collection.UpdateOne(ctx, filter, update, opts)
		if err != nil {
			log.Printf("ERRO MongoDB (Upsert): Op: %s, ID: %v, Erro: %v", op, entityID, err)
			return
		}
		log.Printf("SUCESSO MongoDB (Upsert): Op: %s, ID: %v. Modificado: %d, Inserido: %d", op, entityID, res.ModifiedCount, res.UpsertedCount)

	case "d": // Delete
		res, err := collection.DeleteOne(ctx, filter)
		if err != nil {
			log.Printf("ERRO MongoDB (Delete): Op: %s, ID: %v, Erro: %v", op, entityID, err)
			return
		}
		log.Printf("SUCESSO MongoDB (Delete): Op: %s, ID: %v. Deletado: %d", op, entityID, res.DeletedCount)

	default:
		log.Printf("AVISO: Operação Debezium desconhecida ou ignorada: '%s'", op)
	}
}
