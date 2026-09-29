package messagehandler

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	clientId      string
	totalMsgsSent uint32
}

// Genera un UUID
func generateClientId() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		clientId: generateClientId(),
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	messageHandler.totalMsgsSent++
	return inner.SerializeMessage(messageHandler.clientId, inner.FruitRecord, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOFMessage(messageHandler.clientId, messageHandler.totalMsgsSent)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	msgType, data, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	// El mensaje no es del clientId o es un mensaje de EOF entonces lo ignoramos
	if data.ClientId != messageHandler.clientId || msgType == inner.Eof {
		return nil, nil
	}
	return data.FruitRecords, nil
}
