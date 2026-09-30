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
	id                int
	inputQueue        middleware.Middleware
	outputExchanges   []middleware.Middleware
	controlExchange   middleware.Middleware
	messagesChan      chan InputMessage
	doneChan          chan struct{}
	controlChan       chan InputMessage
	aggregationAmount int
	clientsEof        map[string]bool
	fruitItemMap      map[string]map[string]fruititem.FruitItem
	processedCount    map[string]uint32
	coordinator       *Coordinator
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
	controlChan := make(chan InputMessage)
	doneChan := make(chan struct{})

	return &Sum{
		id:                config.Id,
		inputQueue:        inputQueue,
		outputExchanges:   outputExchanges,
		controlExchange:   controlExchange,
		messagesChan:      messagesChan,
		controlChan:       controlChan,
		doneChan:          doneChan,
		aggregationAmount: config.AggregationAmount,
		clientsEof:        map[string]bool{},
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
		processedCount:    map[string]uint32{},
		coordinator:       NewCoordinator(),
	}, nil
}

func (sum *Sum) Run() {
	// Consumo del exchange de control entre sums. Me va a avisar otro sum si ya no hay mas mensajes
	go sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		inputMessage := InputMessage{message: msg, ack: ack, nack: nack, isControlMsg: true}
		select {
		case sum.controlChan <- inputMessage:
		case <-sum.doneChan:
			return
		}
	})

	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		inputMessage := InputMessage{message: msg, ack: ack, nack: nack}
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
		case controlMessage := <-sum.controlChan:
			sum.handleControlMessage(controlMessage)
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

func (sum *Sum) handleControlMessage(im InputMessage) {
	defer im.ack()
	sumId, clientId, messageType, count, err := inner.DeserializeControlMessage(&im.message)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if sumId == sum.id {
		return
	}

	switch messageType {
	// Me llega el aviso de eof de un cliente de otro sum
	case inner.EofFromSum:
		if err := sum.handleEofFromSum(clientId); err != nil {
			slog.Error("While handling eof from sum message", "err", err)
		}
	// Me llega el aviso del coordinador que puedo enviarle la info al aggregator
	case inner.Flush:
		if err := sum.handleFlushFromSum(clientId); err != nil {
			slog.Error("While handling flush from sum message", "err", err)
		}
	// Siendo coordinador, me llega la cantidad de frutas procesadas por otro sum (post enviar eof)
	case inner.ProcessedBySum:
		sum.coordinator.saveProcessedBySum(clientId, sumId, count)
		if sum.coordinator.isTotalProcessed(clientId) {
			sum.broadcastFlush(clientId)
		}
	}
}

func (sum *Sum) handleMessage(im InputMessage) {
	defer im.ack()
	messageType, msgData, err := inner.DeserializeMessage(&im.message)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	switch messageType {
	// Me llega el eof del gateway
	case inner.Eof:
		if err := sum.handleEndOfRecordMessage(msgData.ClientId, msgData.TotalMessages); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		ctrlMsg, err := inner.SerializeControlMessage(sum.id, msgData.ClientId, inner.EofFromSum)
		if err != nil {
			slog.Error("While serializing message", "err", err)
			return
		}
		if err := sum.controlExchange.Send(*ctrlMsg); err != nil {
			slog.Error("While sending message", "err", err)
			return
		}

	// Me llego una fruta nueva de gateway
	case inner.FruitRecord:
		if err := sum.handleDataMessage(msgData.ClientId, msgData.FruitRecords); err != nil {
			slog.Error("While handling data message", "err", err)
		}
		// Le aviso al coordinador (post recibir eof) que procesé una nueva fruta
		if sum.clientsEof[msgData.ClientId] {
			ctrlMsg, err := inner.SerializeProcessedBySumMessage(sum.id, msgData.ClientId, sum.processedCount[msgData.ClientId])
			if err != nil {
				slog.Error("While serializing message", "err", err)
				return
			}
			if err := sum.controlExchange.Send(*ctrlMsg); err != nil {
				slog.Error("While sending message", "err", err)
				return
			}
		}
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId string, totalMsgsSent uint32) error {
	slog.Info("Received End Of Records message", "clientId", clientId)
	// Me vuelvo coordinador de cierre porque recibi el eof por inputQueue
	sum.clientsEof[clientId] = true

	sum.coordinator.startCoordination(clientId, totalMsgsSent)
	sum.coordinator.saveProcessedBySum(clientId, sum.id, sum.processedCount[clientId])
	if sum.coordinator.isTotalProcessed(clientId) {
		sum.broadcastFlush(clientId)
	}
	return nil
}

func (sum *Sum) handleEofFromSum(clientId string) error {
	if sum.clientsEof[clientId] {
		return nil
	}

	sum.clientsEof[clientId] = true
	ctrlMsg, err := inner.SerializeProcessedBySumMessage(sum.id, clientId, sum.processedCount[clientId])
	if err != nil {
		slog.Error("While serializing message", "err", err)
		return err
	}
	if err := sum.controlExchange.Send(*ctrlMsg); err != nil {
		slog.Error("While sending message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) clearClientProcessedInfo(clientId string) {
	delete(sum.fruitItemMap, clientId)
	delete(sum.clientsEof, clientId)
	delete(sum.processedCount, clientId)

	sum.coordinator.clearClientProcessedInfo(clientId)
}

func (sum *Sum) handleFlushFromSum(clientId string) error {
	for key := range sum.fruitItemMap[clientId] {
		fruitRecord := []fruititem.FruitItem{sum.fruitItemMap[clientId][key]}
		message, err := inner.SerializeMessage(clientId, inner.SumFruits, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		// hasheo el id de la fruta y le aplico modulo para que quede dentro
		// del rango de los ids de los aggregators
		aggregatorId := sum.hashFruit(key) % uint32(sum.aggregationAmount)
		if err := sum.outputExchanges[aggregatorId].Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, inner.Eof, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.broadcastEof(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	sum.clearClientProcessedInfo(clientId)
	return nil
}

func (sum *Sum) broadcastFlush(clientId string) {
	ctrlMsg, err := inner.SerializeControlMessage(sum.id, clientId, inner.Flush)
	if err != nil {
		slog.Error("While serializing message", "err", err)
		return
	}
	// Le envio la orden de flushear a los sums
	if err := sum.controlExchange.Send(*ctrlMsg); err != nil {
		slog.Error("While sending message", "err", err)
		return
	}
	// Flushea el coordinador como cualquier sum
	if err := sum.handleFlushFromSum(clientId); err != nil {
		slog.Error("While sending message", "err", err)
		return
	}
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
	sum.processedCount[clientId]++
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
