package gorabbitmq

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func SendMessages(user string, password string, url string, queueName string, queueRName string, msg interface{}) {
	connectionString := fmt.Sprintf("amqp://%s:%s@%s/", user, password, url)

	conn, err := amqp.Dial(connectionString)

	if err != nil {
		log.Fatalln("Failed to connect to RabbitMQ: ", err)
	}

	log.Println("Success connecting to RabbitMQ")

	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalln("Error: ", err)
	}

	defer channel.Close()

	queueReturn, err := channel.QueueDeclare(
		queueRName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Falha ao declarar fila: %v", err)
	}

	queue, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalln("Error: ", err)
	}

	userJson, err := json.Marshal(msg)

	if err != nil {
		log.Fatalln("Error: ", err)
	}

	err = channel.Publish(
		"",
		queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        userJson,
			ReplyTo:     queueReturn.Name,
		},
	)
	if err != nil {
		log.Fatalln("Error: ", err)
	}
}
