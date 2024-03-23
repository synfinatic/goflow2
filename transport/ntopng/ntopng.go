package ntopng

import (
	"flag"
	"fmt"

	"github.com/netsampler/goflow2/v2/transport"
	zmq "github.com/pebbe/zmq4"
)

type NtopngDriver struct {
	listen    string // proto://IP:Port to listen on for ZMQ connections
	source_id uint
	context   *zmq.Context
	publisher *zmq.Socket
}

// Implement the TransportDriver interface
func (nt *NtopngDriver) Prepare() error {
	flag.StringVar(&nt.listen, "transport.ntopng.listen", "tcp://0.0.0.0:5556", "Listen for ZMQ connections from ntopng")
	flag.UintVar(&nt.source_id, "transport.ntopng.source_id", 0, "Source ID for ZMQ messages")
	return nil
}

// Initialize the NtopngDriver
func (nt *NtopngDriver) Init() error {
	var err error
	nt.context, err = zmq.NewContext()
	if err != nil {
		return err
	}

	nt.publisher, err = nt.context.NewSocket(zmq.PUB)
	if err != nil {
		return err
	}

	err = nt.publisher.Bind(nt.listen)
	if err != nil {
		return err
	}
	return nil
}

// Close the NtopngDriver
func (nt *NtopngDriver) Close() error {
	return nt.publisher.Close()
}

// Send data to ntopng
func (nt *NtopngDriver) Send(key, data []byte) error {
	msgLen := uint16(len(data))
	header := NewZmqHeader(msgLen, uint8(nt.source_id))

	// Send the header with the topic first as a multi-part message
	headerBytes, err := header.Bytes()
	if err != nil {
		return err
	}

	bytes, err := nt.publisher.SendBytes(headerBytes, zmq.SNDMORE)
	if err != nil {
		return err
	}

	if bytes != len(headerBytes) {
		return fmt.Errorf("%s", "Wrote the wrong number of header bytes")
	}

	// Now send the actual JSON payload
	if _, err = nt.publisher.SendBytes(data, 0); err != nil {
		return err
	}

	return nil
}

// Register the NtopngDriver with the TransportDriver interface
func init() {
	nt := &NtopngDriver{}
	transport.RegisterTransportDriver("ntopng", nt)
}
