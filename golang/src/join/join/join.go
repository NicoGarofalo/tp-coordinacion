package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	clientEofCounter  map[string]int
	aggregationAmount int
	fruitItems        map[string]map[string]fruititem.FruitItem
	topSize           int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		clientEofCounter:  map[string]int{},
		aggregationAmount: config.AggregationAmount,
		fruitItems:        map[string]map[string]fruititem.FruitItem{},
		topSize:           config.TopSize,
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)

	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		join.clientEofCounter[clientId]++
		if join.clientEofCounter[clientId] == join.aggregationAmount {
			topFruits := join.getTopKFruitItems(clientId)
			topFruitsMsg, err := inner.SerializeMessage(clientId, topFruits)
			if err != nil {
				slog.Error("While serializing top", "err", err)
				return
			}
			if err := join.outputQueue.Send(*topFruitsMsg); err != nil {
				slog.Error("While sending top", "err", err)
				return
			}
			delete(join.clientEofCounter, clientId)
			delete(join.fruitItems, clientId)
		}
	} else {
		join.addFruitItems(clientId, fruitRecords)
	}
}

func (join *Join) addFruitItems(clientId string, fruitRecords []fruititem.FruitItem) {
	// Voy acumulando los resultados de los aggregators
	// Si no existe un mapa para este cliente, lo creo
	if _, ok := join.fruitItems[clientId]; !ok {
		join.fruitItems[clientId] = make(map[string]fruititem.FruitItem)
	}
	for _, fruitRecord := range fruitRecords {
		if _, ok := join.fruitItems[clientId][fruitRecord.Fruit]; ok {
			join.fruitItems[clientId][fruitRecord.Fruit] = join.fruitItems[clientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			join.fruitItems[clientId][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) getTopKFruitItems(clientId string) []fruititem.FruitItem {
	// analogo al de aggregator
	fruitItems := make([]fruititem.FruitItem, 0, len(join.fruitItems[clientId]))
	for _, item := range join.fruitItems[clientId] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
