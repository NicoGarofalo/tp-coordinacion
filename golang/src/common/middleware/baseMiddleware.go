package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type BaseMiddleware struct {
	conn *amqp.Connection
	ch *amqp.Channel
	queue amqp.Queue
	tag string
	isConsuming bool
}

const FIVE_SECS = 5

func (bm *BaseMiddleware) StopConsuming() error {
	if(bm.conn.IsClosed()){
		return ErrMessageMiddlewareDisconnected
	}
	if bm.isConsuming {
		err := bm.ch.Cancel(bm.tag, false)
		if err != nil {
			return ErrMessageMiddlewareClose
		}
		bm.isConsuming = false
	}
	return nil
}

func (bm *BaseMiddleware) Close() error {
	if bm.conn.IsClosed() {
		return nil
	}

	chErr := bm.ch.Close()
	if chErr != nil {
		return ErrMessageMiddlewareClose
	}
	connErr := bm.conn.Close()
	if connErr != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}

// Funcion auxiliar que tienen en común tanto Queue como Exchange.
// Se lee indefinidamente del canal de mensajes msgsChan.
// Obtiene mensajes del channel, y llamo a la callback function con el msg y las funciones de ack y nack.
// Se corta el flujo del for cuando se cierra el canal.
func consumeMessages(msgsChan <-chan amqp.Delivery, callbackFunc func(msg Message, ack func(), nack func())) {
	for d := range msgsChan {
		msg := Message{Body: string(d.Body)}

		ackFn := func() {
			d.Ack(false)
		}

		nackFn := func() {
			d.Nack(false, false)
		}

		callbackFunc(msg, ackFn, nackFn)
	}
}