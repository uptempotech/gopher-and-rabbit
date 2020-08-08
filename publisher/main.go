package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
	"github.com/uptempotech/gopher-and-rabbit/global"
)

func handleError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}

}

var (
	uri        = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	name       = flag.String("name", "add", "Queue name")
	durable    = flag.Bool("durable", true, "Queue durable?")
	autoDelete = flag.Bool("autoDelete", false, "AutoDelete messages from Queue?")
	exclusive  = flag.Bool("exclusive", false, "Is Queue exclusive?")
	noWait     = flag.Bool("noWait", false, "No waiting?")
	exchange   = flag.String("exchange", "", "Durable AMQP exchange name")
	mandatory  = flag.Bool("mandatory", false, "Is this mandatory?")
	immediate  = flag.Bool("immediate", false, "Is this immediate?")
)

func main() {
	conn, err := amqp.Dial(*uri)
	handleError(err, "Can't connect to AMQP")
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	handleError(err, "Can't create a amqpChannel")

	defer amqpChannel.Close()

	_, err = amqpChannel.QueueDeclare(*name, *durable, *autoDelete, *exclusive, *noWait, nil)
	handleError(err, "Could not declare `add` queue")

	rand.Seed(time.Now().UnixNano())

	addTask := global.AddTask{Number1: rand.Intn(999), Number2: rand.Intn(999)}
	body, err := json.Marshal(addTask)
	if err != nil {
		handleError(err, "Error encoding JSON")
	}

	err = amqpChannel.Publish(*exchange, *name, *mandatory, *immediate, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         body,
	})
	if err != nil {
		handleError(err, "Error publishing message: %s")
	}

	log.Printf("AddTask: %d+%d", addTask.Number1, addTask.Number2)

}
