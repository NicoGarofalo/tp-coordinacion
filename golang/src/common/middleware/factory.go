package middleware

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	connString := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(connString)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		true,    // durability
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}
	
	qm := QueueMiddleware{
		BaseMiddleware: BaseMiddleware{
			conn: conn,
			ch: ch,
			queue: q,
			isConsuming: false,	
		},
	}
	return &qm, nil
}


func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	connString := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(connString)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"topic", // type
		true,     // durability
		false,    // auto delete
		false,    // internal
		false,    // no-wait
		nil,  // arsgs
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}
	
	em := ExchangeMiddleware{
		BaseMiddleware: BaseMiddleware{
			conn: conn,
			ch: ch,
			isConsuming: false,
		},
		exchangeName: exchange,
		topicKeys: keys,
	}
	return &em, nil
}
