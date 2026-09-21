package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue      middleware.Middleware
	inputExchange    middleware.Middleware
	fruitItemMap     map[string]map[string]fruititem.FruitItem
	topSize          int
	clientEofCounter map[string]int
	sumAmount        int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:      outputQueue,
		inputExchange:    inputExchange,
		fruitItemMap:     map[string]map[string]fruititem.FruitItem{},
		topSize:          config.TopSize,
		clientEofCounter: map[string]int{},
		sumAmount:        config.SumAmount,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		aggregation.clientEofCounter[clientId]++
		if aggregation.clientEofCounter[clientId] >= aggregation.sumAmount {
			// Ya tengo todos los EOF de todos los nodos
			if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
				slog.Error("While handling end of record message", "err", err)
			}
		}
		return
	}

	aggregation.handleDataMessage(clientId, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId string) error {
	slog.Info("Received End Of Records message")

	fruitTopRecords := aggregation.buildFruitTop(clientId)
	message, err := inner.SerializeMessage(clientId, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage := []fruititem.FruitItem{}
	message, err = inner.SerializeMessage(clientId, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	// Ya puedo borrar el clientid de los eof y de fruit item map
	delete(aggregation.clientEofCounter, clientId)
	delete(aggregation.fruitItemMap, clientId)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId string, fruitRecords []fruititem.FruitItem) {
	// Si es la primera vez que este cliente manda mensajes, le creo un map
	if _, ok := aggregation.fruitItemMap[clientId]; !ok {
		aggregation.fruitItemMap[clientId] = make(map[string]fruititem.FruitItem)
	}

	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.fruitItemMap[clientId][fruitRecord.Fruit]; ok {
			aggregation.fruitItemMap[clientId][fruitRecord.Fruit] = aggregation.fruitItemMap[clientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[clientId][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientId string) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap[clientId]))
	for _, item := range aggregation.fruitItemMap[clientId] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
