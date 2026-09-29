package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type InternalMessageType string

const (
	FruitRecord      InternalMessageType = "FRUITRECORD"
	SumFruits        InternalMessageType = "SUMFRUITS"
	AggregatedFruits InternalMessageType = "AGGREGATEDFRUITS"
	TopFruits        InternalMessageType = "TOPFRUITS"
	Eof              InternalMessageType = "EOF"
	Error            InternalMessageType = "ERROR" // Se usa solo para retornar error
)

type ControlMessageType string

const (
	EofFromSum     ControlMessageType = "EOF_FROM_SUM"
	ProcessedBySum ControlMessageType = "PROCESSED_BY_SUM"
	Flush          ControlMessageType = "FLUSH"
	ControlError   ControlMessageType = "CONTROL_ERROR"
)

type ProtocolMessage interface {
	InternalMessage | EOFMessage | ControlMessage
}

type InternalMessage struct {
	ClientId string
	Type     InternalMessageType
	Data     []fruititem.FruitItem
}

type EOFMessage struct {
	ClientId      string
	Type          InternalMessageType
	TotalMessages uint32
}

type ControlMessage struct {
	SumId    int
	ClientId string
	Type     ControlMessageType
	Count    uint32
}

func serializeJson[ProtocolMsgType ProtocolMessage](message ProtocolMsgType) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson[ProtocolMsgType ProtocolMessage](message []byte) (ProtocolMsgType, error) {
	var data ProtocolMsgType
	if err := json.Unmarshal(message, &data); err != nil {
		return data, err
	}
	return data, nil
}

func SerializeMessage(clientId string, messageType InternalMessageType, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	internalMessage := InternalMessage{
		ClientId: clientId,
		Type:     messageType,
		Data:     fruitRecords,
	}

	body, err := serializeJson(internalMessage)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func SerializeEOFMessage(clientId string, TotalMessages uint32) (*middleware.Message, error) {
	eofMessage := EOFMessage{
		ClientId:      clientId,
		Type:          Eof,
		TotalMessages: TotalMessages,
	}

	body, err := serializeJson(eofMessage)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

type MessageData struct {
	ClientId      string
	FruitRecords  []fruititem.FruitItem
	TotalMessages uint32
}

type messageTypeHeader struct {
	Type InternalMessageType
}

func DeserializeMessage(message *middleware.Message) (InternalMessageType, MessageData, error) {
	bytes := []byte((*message).Body)

	var header messageTypeHeader
	// REvisar si esto se puede hacer funcion
	if err := json.Unmarshal(bytes, &header); err != nil {
		return Error, MessageData{}, err
	}

	if header.Type == Eof {
		data, err := deserializeJson[EOFMessage](bytes)
		if err != nil {
			return Error, MessageData{}, err
		}
		return Eof, MessageData{ClientId: data.ClientId, TotalMessages: data.TotalMessages}, nil
	}

	data, err := deserializeJson[InternalMessage](bytes)
	if err != nil {
		return Error, MessageData{}, err
	}
	return data.Type, MessageData{
		ClientId:     data.ClientId,
		FruitRecords: data.Data,
	}, nil
}

func SerializeControlMessage(sumId int, clientId string, messageType ControlMessageType) (*middleware.Message, error) {
	controlMessage := ControlMessage{
		SumId:    sumId,
		ClientId: clientId,
		Type:     messageType,
	}

	body, err := serializeJson(controlMessage)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func SerializeProcessedBySumMessage(sumId int, clientId string, count uint32) (*middleware.Message, error) {
	controlMessage := ControlMessage{
		SumId:    sumId,
		ClientId: clientId,
		Type:     ProcessedBySum,
		Count:    count,
	}

	body, err := serializeJson(controlMessage)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeControlMessage(message *middleware.Message) (int, string, ControlMessageType, uint32, error) {
	data, err := deserializeJson[ControlMessage]([]byte((*message).Body))
	if err != nil {
		return -1, "", ControlError, 0, err
	}

	return data.SumId, data.ClientId, data.Type, data.Count, nil
}
