package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

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
	consumer   = flag.String("consumer", "", "Declare the consumer")
	autoAck    = flag.Bool("autoAck", false, "Auto Acknowledge message")
	noLocal    = flag.Bool("noLocal", false, "No local?")
)

func main() {
	conn, err := amqp.Dial(*uri)
	handleError(err, "Can't connect to AMQP")
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	handleError(err, "Can't create a amqpChannel")

	defer amqpChannel.Close()

	_, err = amqpChannel.QueueDeclare(
		*name,
		*durable,
		*autoDelete,
		*exclusive,
		*noWait,
		nil,
	)
	handleError(err, "Could not declare `add` queue")

	err = amqpChannel.Qos(1, 0, false)
	handleError(err, "Could not configure QoS")

	messageChannel, err := amqpChannel.Consume(
		*name,
		*consumer,
		*autoAck,
		*exclusive,
		*noLocal,
		*noWait,
		nil,
	)
	handleError(err, "Could not register consumer")

	stopChan := make(chan bool)

	go func() {
		log.Printf("Consumer ready, PID: %d", os.Getpid())
		for d := range messageChannel {
			log.Printf("Received a message: %s", d.Body)

			addTask := &global.AddTask{}

			err := json.Unmarshal(d.Body, addTask)
			if err != nil {
				handleError(err, "Error decoding JSON: %s")
			}

			log.Printf("Result of %d + %d is : %d", addTask.Number1, addTask.Number2, addTask.Number1+addTask.Number2)

			if err := d.Ack(false); err != nil {
				log.Printf("Error acknowledging message : %s", err)
			} else {
				log.Printf("Acknowledged message")
			}

		}
	}()

	// Stop for program termination
	<-stopChan
}
