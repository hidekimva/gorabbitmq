package gorabbitmq

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func SendMessages(user string, password string, url string, queueName string, queueRName string, msg interface{}) {
	// Configura a conexão com o RabbitMQ
	connectionString := fmt.Sprintf("amqp://%s:%s@%s/", user, password, url)

	conn, err := amqp.Dial(connectionString)

	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ: ", err.Error())
	}

	log.Println("Success connecting to RabbitMQ")

	defer conn.Close()

	// Cria um canal para se comunicar com o RabbitMQ
	channel, err := conn.Channel()
	if err != nil {
		log.Fatalln("Error creating channel with rabbitmq: ", err.Error())
	}

	defer channel.Close()

	// Declara fila de retorno
	queueReturn, err := channel.QueueDeclare(
		queueRName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalln("Failed to declare return queue: ", err.Error())
	}

	// Declara fila de envio
	queue, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalln("Failed to declare send queue: ", err.Error())
	}

	// Codifica body para o envio
	body, err := json.Marshal(msg)
	if err != nil {
		log.Fatalln("Error encoding json: ", err.Error())
	}

	// Publica na fila de envio
	err = channel.Publish(
		"",
		queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			ReplyTo:     queueReturn.Name,
		},
	)
	if err != nil {
		log.Fatalln("Failed to send to queue: " + err.Error())
	}

	// Consome mensagens da fila
	msgs, err := channel.Consume(
		queueRName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Erro ao consumir fila: %s", err)
	}

	// Loop infinito para processar as mensagens recebidas
	for msg := range msgs {
		fmt.Println(msg)
	}
}
