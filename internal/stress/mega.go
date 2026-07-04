// Mega stress test - 1000 deploys com WebSocket clients
package stress

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func MegaSimple() {
	const numClients = 100
	const totalDeploys = 2000

	clientChans := make([]chan string, numClients)
	for i := range clientChans {
		clientChans[i] = make(chan string, 50)
	}

	recvCount := atomic.Int64{}

	var clientWG sync.WaitGroup
	for i := 0; i < numClients; i++ {
		clientWG.Add(1)
		go func(c chan string) {
			defer clientWG.Done()
			for range c {
				recvCount.Add(1)
			}
		}(clientChans[i])
	}

	var deployWG sync.WaitGroup
	tokens := make(chan struct{}, 200)
	start := time.Now()

	for i := 0; i < totalDeploys; i++ {
		tokens <- struct{}{}
		deployWG.Add(1)
		go func(idx int) {
			defer deployWG.Done()
			defer func() { <-tokens }()

			for _, c := range clientChans {
				select {
				case c <- "msg":
				default:
				}
			}
		}(i)
	}

	deployWG.Wait()
	for _, c := range clientChans {
		close(c)
	}
	clientWG.Wait()

	totalDur := time.Since(start)
	fmt.Printf("\n[MEGA] %d deploys + %d clients em %s\n",
		totalDeploys, numClients, totalDur.Round(time.Millisecond))
	fmt.Printf("[MEGA] Throughput: %.0f ops/sec\n", float64(totalDeploys)/totalDur.Seconds())
	fmt.Printf("[MEGA] Mensagens recebidas: %d\n", recvCount.Load())
	fmt.Printf("[MEGA] Latencia media: %.2fms\n", float64(totalDur.Microseconds())/float64(totalDeploys)/1000)
}
