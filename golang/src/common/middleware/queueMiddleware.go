package middleware

import (
	"context"
	"time"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	BaseMiddleware
}


func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	//Corroboro que no se quiera llamar a StartConsuming luego de otro StartConsuming
	if qm.isConsuming {
		return ErrMessageMiddlewareMessage
	}

	qm.tag = "tag-" + qm.queue.Name
	// Obtengo un canal para empezar a consumir los mensajes
	msgsChan, err := qm.ch.Consume(
		qm.queue.Name, // queue
		qm.tag, // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	if err != nil {
		// Se corrobora que no haya fallado porque se cerró la conexión (esta validación está en varios lugares)
		if qm.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	qm.isConsuming = true
	consumeMessages(msgsChan, callbackFunc)
	qm.isConsuming = false

	// Si se cortó el consumo porque se cortó la conexión, devolvemos error
	if qm.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	return nil
}


func (qm *QueueMiddleware) Send(msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), FIVE_SECS*time.Second)
	defer cancel()

	body := msg.Body
	// Publico a la queue
	err := qm.ch.PublishWithContext(ctx,
	"",     // exchange
	qm.queue.Name, // publish to this queue
	false,  // mandatory
	false,  // immediate
	amqp.Publishing {
		ContentType: "text/plain",
		Body:        []byte(body),
	})
	if err != nil {
		if qm.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	return nil
}

