package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type InputMessage struct {
	message      middleware.Message
	ack          func()
	nack         func()
	isControlMsg bool
}

type Sum struct {
	inputQueue      middleware.Middleware
	outputExchange  middleware.Middleware
	controlExchange middleware.Middleware
	messagesChan    chan InputMessage
	clientsEof      map[string]bool
	fruitItemMap    map[string]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	controlKeys := []string{"control"}
	controlExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, controlKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	messagesChan := make(chan InputMessage)

	return &Sum{
		inputQueue:      inputQueue,
		outputExchange:  outputExchange,
		controlExchange: controlExchange,
		messagesChan:    messagesChan,
		clientsEof:      map[string]bool{},
		fruitItemMap:    map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	// Consumo del exchange de control entre sums. Me va a avisar otro sum si ya no hay mas mensajes
	go sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.messagesChan <- InputMessage{message: msg, ack: ack, nack: nack, isControlMsg: true}
	})

	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.messagesChan <- InputMessage{message: msg, ack: ack, nack: nack, isControlMsg: false}
	})

	for inputMessage := range sum.messagesChan {
		sum.handleMessage(inputMessage)
	}
}

func (sum *Sum) handleMessage(im InputMessage) {
	defer im.ack()
	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&im.message)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		if !im.isControlMsg {
			// Me llego desde la inputQueue un eof y debo avisar a los demas sums
			if err := sum.controlExchange.Send(im.message); err != nil {
				slog.Error("While sending message", "err", err)
				return
			}
		}
		return
	}

	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId string) error {
	// Si recibi otro eof del mismo cliente lo ignoro
	if sum.clientsEof[clientId] {
		return nil
	}
	slog.Info("Received End Of Records message", "clientId", clientId)
	sum.clientsEof[clientId] = true
	for key := range sum.fruitItemMap[clientId] {
		fruitRecord := []fruititem.FruitItem{sum.fruitItemMap[clientId][key]}
		message, err := inner.SerializeMessage(clientId, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(sum.fruitItemMap, clientId)
	return nil
}

func (sum *Sum) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) error {
	// Si es la primera vez que este cliente suma frutas, le creo un map
	if _, ok := sum.fruitItemMap[clientId]; !ok {
		sum.fruitItemMap[clientId] = make(map[string]fruititem.FruitItem)
	}

	for _, fruitRecord := range fruitRecords {
		_, ok := sum.fruitItemMap[clientId][fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[clientId][fruitRecord.Fruit] = sum.fruitItemMap[clientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[clientId][fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
