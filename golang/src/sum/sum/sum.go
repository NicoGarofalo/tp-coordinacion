package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
	inputQueue        middleware.Middleware
	outputExchanges   []middleware.Middleware
	controlExchange   middleware.Middleware
	messagesChan      chan InputMessage
	doneChan          chan struct{}
	aggregationAmount int
	clientsEof        map[string]bool
	fruitItemMap      map[string]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	controlKeys := []string{"control"}
	controlExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, controlKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)
	for i := range config.AggregationAmount {
		aggregationKeys := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, i)}
		exchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, aggregationKeys, connSettings)
		if err != nil {
			inputQueue.Close()
			controlExchange.Close()
			return nil, err
		}
		outputExchanges[i] = exchange
	}

	messagesChan := make(chan InputMessage)
	doneChan := make(chan struct{})

	return &Sum{
		inputQueue:        inputQueue,
		outputExchanges:   outputExchanges,
		controlExchange:   controlExchange,
		messagesChan:      messagesChan,
		doneChan:          doneChan,
		aggregationAmount: config.AggregationAmount,
		clientsEof:        map[string]bool{},
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	// Consumo del exchange de control entre sums. Me va a avisar otro sum si ya no hay mas mensajes
	go sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		inputMessage := InputMessage{message: msg, ack: ack, nack: nack, isControlMsg: true}
		select {
		case sum.messagesChan <- inputMessage:
		case <-sum.doneChan:
			return
		}
	})

	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		inputMessage := InputMessage{message: msg, ack: ack, nack: nack, isControlMsg: false}
		select {
		case sum.messagesChan <- inputMessage:
		case <-sum.doneChan:
			return
		}
	})

	go sum.handleSignals()

	for {
		select {
		case inputMessage := <-sum.messagesChan:
			sum.handleMessage(inputMessage)
		case <-sum.doneChan:
			return
		}
	}
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.closeConnections()
}

func (sum *Sum) hashFruit(fruitName string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(fruitName))
	return h.Sum32()
}

func (sum *Sum) broadcastEof(message middleware.Message) error {
	for _, outputExchange := range sum.outputExchanges {
		err := outputExchange.Send(message)
		if err != nil {
			return err
		}
	}
	return nil
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
		aggregatorId := sum.hashFruit(key) % uint32(sum.aggregationAmount)
		if err := sum.outputExchanges[aggregatorId].Send(*message); err != nil {
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
	if err := sum.broadcastEof(*message); err != nil {
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

func (sum *Sum) closeConnections() {
	err := sum.inputQueue.StopConsuming()
	if err != nil {
		slog.Error("While stopping consuming", "err", err)
	}
	err = sum.inputQueue.Close()
	if err != nil {
		slog.Error("While closing queue", "err", err)
	}
	err = sum.controlExchange.StopConsuming()
	if err != nil {
		slog.Error("While stopping consuming", "err", err)
	}
	err = sum.controlExchange.Close()
	if err != nil {
		slog.Error("While closing exchange", "err", err)
	}
	for _, outputExchange := range sum.outputExchanges {
		err = outputExchange.Close()
		if err != nil {
			slog.Error("While closing exchange", "err", err)
		}
	}

	close(sum.doneChan)
}
