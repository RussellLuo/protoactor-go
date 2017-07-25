package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	console "github.com/AsynkronIT/goconsole"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/cluster"
	"github.com/AsynkronIT/protoactor-go/cluster/gossip"
	"github.com/AsynkronIT/protoactor-go/examples/cluster/shared"
	"github.com/AsynkronIT/protoactor-go/remote"
)

const (
	timeout = 1 * time.Second
)

func main() {
	join := flag.String("join", "", "comma separated existing nodes to join")
	flag.Parse()

	var seedNodes []string
        if len(*join) == 0 {
		log.Fatal("--join required")
        }
	seedNodes = strings.Split(*join, ",")

	//this node knows about Hello kind
	remote.Register("Hello", actor.FromProducer(func() actor.Actor {
		return &shared.HelloActor{}
	}))

	cp := gossip.New(200*time.Millisecond, seedNodes, nil)
	cluster.Start("mycluster", "127.0.0.1:8081", cp)

	sync()
	async()

	console.ReadLine()
}

func sync() {
	hello := shared.GetHelloGrain("abc")
	options := []cluster.GrainCallOption{cluster.WithTimeout(5 * time.Second), cluster.WithRetry(5)}
	res, err := hello.SayHello(&shared.HelloRequest{Name: "GAM"}, options...)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Message from SayHello: %v", res.Message)
	for i := 0; i < 10000; i++ {
		x := shared.GetHelloGrain(fmt.Sprintf("hello%v", i))
		x.SayHello(&shared.HelloRequest{Name: "GAM"})
	}
	log.Println("Done")
}

func async() {
	hello := shared.GetHelloGrain("abc")
	c, e := hello.AddChan(&shared.AddRequest{A: 123, B: 456})

	for {
		select {
		case <-time.After(100 * time.Millisecond):
			log.Println("Tick..") //this might not happen if res returns fast enough
		case err := <-e:
			log.Fatal(err)
		case res := <-c:
			log.Printf("Result is %v", res.Result)
			return
		}
	}
}
