package kafka

import (
	"log"
	"testing"

	"github.com/samuelngs/hyper"
	"github.com/samuelngs/hyper/sync"
)

func TestNew(t *testing.T) {

	m := New(
		Addrs("localhost:9092"),
	)

	h := hyper.New(
		hyper.Addr(":4000"),
		hyper.HTTP2(),
		hyper.Message(m),
	)

	h.
		Sync().
		Namespace("default").
		Name("default").
		Doc(`Default namespace`).
		Summary(`Default namespace`).
		Handle("ping", func(m []byte, n sync.Channel, c sync.Context) {
			n.Write(&sync.Packet{Message: []byte("pong")})
		})

	if err := h.Run(); err != nil {
		log.Fatal(err)
	}
}
