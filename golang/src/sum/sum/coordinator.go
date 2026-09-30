package sum

type Coordinator struct {
	clientTotalMsgs map[string]uint32
	processedBySums map[string]map[int]uint32
	isCoordinator   map[string]bool
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		clientTotalMsgs: make(map[string]uint32),
		processedBySums: make(map[string]map[int]uint32),
		isCoordinator:   make(map[string]bool),
	}
}

func (c *Coordinator) startCoordination(clientId string, totalMsgs uint32) {
	c.isCoordinator[clientId] = true
	c.clientTotalMsgs[clientId] = totalMsgs
}

func (c *Coordinator) saveProcessedBySum(clientId string, sumId int, processedCount uint32) {
	if !c.isCoordinator[clientId] {
		return
	}
	if _, ok := c.processedBySums[clientId]; !ok {
		c.processedBySums[clientId] = map[int]uint32{}
	}
	c.processedBySums[clientId][sumId] = processedCount
}

func (c *Coordinator) isTotalProcessed(clientId string) bool {
	if !c.isCoordinator[clientId] {
		return false
	}
	if c.clientTotalMsgs[clientId] != 0 {
		totalProcessed := uint32(0)
		for _, processedCount := range c.processedBySums[clientId] {
			totalProcessed += processedCount
		}
		if totalProcessed == c.clientTotalMsgs[clientId] {
			return true
		}
	}
	return false
}

func (c *Coordinator) clearClientProcessedInfo(clientId string) {
	delete(c.clientTotalMsgs, clientId)
	delete(c.processedBySums, clientId)
	delete(c.isCoordinator, clientId)
}
