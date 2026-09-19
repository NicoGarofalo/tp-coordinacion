package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type InternalMessage struct {
	ClientId string
	Data     []fruititem.FruitItem
}

func serializeJson(message InternalMessage) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) (InternalMessage, error) {
	var data InternalMessage
	if err := json.Unmarshal(message, &data); err != nil {
		return InternalMessage{}, err
	}
	return data, nil
}

func SerializeMessage(clientId string, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	internalMessage := InternalMessage{
		ClientId: clientId,
		Data:     fruitRecords,
	}

	body, err := serializeJson(internalMessage)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return "", nil, false, err
	}

	isEof := len(data.Data) == 0
	return data.ClientId, data.Data, isEof, nil
}
