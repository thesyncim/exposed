package exposed

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"encoding/binary"
	"github.com/golang/snappy"
)

// CompressType is a compression type used for connections.
type CompressType byte

const (
	// CompressNone disables connection compression.
	//
	// CompressNone may be used in the following cases:
	//
	//   * If network bandwidth between client and server is unlimited.
	//   * If client and server are located on the same physical host.
	//   * If other CompressType values consume a lot of CPU resources.
	//
	CompressNone = CompressType(1)

	// CompressFlate uses compress/flate with default
	// compression level for connection compression.
	//
	// CompressFlate may be used in the following cases:
	//
	//     * If network bandwidth between client and server is limited.
	//     * If client and server are located on distinct physical hosts.
	//     * If both client and server have enough CPU resources
	//       for compression processing.
	//
	CompressFlate = CompressType(0)

	// CompressSnappy uses snappy compression.
	//
	// CompressSnappy vs CompressFlate comparison:
	//
	//     * CompressSnappy consumes less CPU resources.
	//     * CompressSnappy consumes more network bandwidth.
	//
	CompressSnappy = CompressType(2)
)

type handshakeConfig struct {
	sniffHeader       []byte
	protocolVersion   byte
	conn              net.Conn
	readBufferSize    int
	writeBufferSize   int
	writeCompressType CompressType
	tlsConfig         *tls.Config
	isServer          bool
	pipeline          bool
}

//todo move allow registered compress type
func newBufioConn(cfg *handshakeConfig) (*bufio.Reader, *bufio.Writer, bool, error) {

	readCompressType, pipeline, realConn, err := handshake(cfg)
	if err != nil {
		return nil, nil, false, err
	}

	r := io.Reader(realConn)
	switch readCompressType {
	case CompressNone:
	case CompressFlate:
		r = flate.NewReader(r)
	case CompressSnappy:
		r = snappy.NewReader(r)
	default:
		return nil, nil, false, fmt.Errorf("unknown read CompressType: %v", readCompressType)
	}
	readBufferSize := cfg.readBufferSize

	br := bufio.NewReaderSize(r, readBufferSize)

	w := io.Writer(realConn)
	switch cfg.writeCompressType {
	case CompressNone:
	case CompressFlate:
		zw, err := flate.NewWriter(w, flate.DefaultCompression)
		if err != nil {
			panic(fmt.Sprintf("BUG: flate.NewWriter(%d) returned non-nil err: %s", flate.DefaultCompression, err))
		}
		w = &writeFlusher{w: zw}
	case CompressSnappy:
		// From the docs at https://godoc.org/github.com/golang/snappy#NewWriter :
		// There is no need to Flush or Close such a Writer,
		// so don't wrap it into writeFlusher.
		w = snappy.NewWriter(w)

	default:
		return nil, nil, false, fmt.Errorf("unknown write CompressType: %v", cfg.writeCompressType)
	}
	writeBufferSize := cfg.writeBufferSize

	bw := bufio.NewWriterSize(w, writeBufferSize)
	return br, bw, pipeline, nil
}

func handshake(cfg *handshakeConfig) (readCompressType CompressType, pipeline bool, realConn net.Conn, err error) {
	handshakeFunc := handshakeClient
	if cfg.isServer {
		handshakeFunc = handshakeServer
	}
	deadline := time.Now().Add(3 * time.Second)
	if err = cfg.conn.SetWriteDeadline(deadline); err != nil {
		return 0, false, nil, fmt.Errorf("cannot set write timeout: %s", err)
	}
	if err = cfg.conn.SetReadDeadline(deadline); err != nil {
		return 0, false, nil, fmt.Errorf("cannot set read timeout: %s", err)
	}
	readCompressType, pipeline, realConn, err = handshakeFunc(cfg)
	if err != nil {
		return 0, false, nil, fmt.Errorf("error in handshake: %s", err)
	}
	if err = cfg.conn.SetWriteDeadline(zeroTime); err != nil {
		return 0, false, nil, fmt.Errorf("cannot reset write timeout: %s", err)
	}
	if err = cfg.conn.SetReadDeadline(zeroTime); err != nil {
		return 0, false, nil, fmt.Errorf("cannot reset read timeout: %s", err)
	}
	return readCompressType, pipeline, realConn, err
}

func handshakeServer(cfg *handshakeConfig) (CompressType, bool, net.Conn, error) {
	conn := cfg.conn
	readCompressType, isTLS, pipeline, err := handshakeRead(conn, cfg.sniffHeader, cfg.protocolVersion)
	if err != nil {
		return 0, false, nil, err
	}
	if isTLS && cfg.tlsConfig == nil {
		handshakeWrite(conn, cfg.writeCompressType, false, pipeline, cfg.sniffHeader, cfg.protocolVersion)
		return 0, false, nil, fmt.Errorf("Cannot serve encrypted client connection. " +
			"Set Server.TLSConfig for supporting encrypted connections")
	}
	if err := handshakeWrite(conn, cfg.writeCompressType, isTLS, pipeline, cfg.sniffHeader, cfg.protocolVersion); err != nil {
		return 0, false, nil, err
	}
	if isTLS {
		tlsConn := tls.Server(conn, cfg.tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			return 0, false, nil, fmt.Errorf("error in TLS handshake: %s", err)
		}
		conn = tlsConn
	}
	return readCompressType, pipeline, conn, nil
}

func handshakeClient(cfg *handshakeConfig) (CompressType, bool, net.Conn, error) {
	conn := cfg.conn
	isTLS := cfg.tlsConfig != nil
	if err := handshakeWrite(conn, cfg.writeCompressType, isTLS, cfg.pipeline, cfg.sniffHeader, cfg.protocolVersion); err != nil {
		return 0, false, nil, err
	}
	readCompressType, isTLSCheck, _, err := handshakeRead(conn, cfg.sniffHeader, cfg.protocolVersion)
	if err != nil {
		return 0, false, nil, err
	}
	if isTLS {
		if !isTLSCheck {
			return 0, false, nil, fmt.Errorf("Server doesn't support encrypted connections. " +
				"Set Server.TLSConfig for enabling encrypted connections on the server")
		}
		tlsConn := tls.Client(conn, cfg.tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			return 0, false, nil, fmt.Errorf("error in TLS handshake: %s", err)
		}
		conn = tlsConn
	}
	return readCompressType, false, conn, nil
}

func handshakeWrite(conn net.Conn, compressType CompressType, isTLS bool, pipeline bool, sniffHeader []byte, protocolVersion byte) error {
	if _, err := conn.Write(sniffHeader); err != nil {
		return fmt.Errorf("cannot write sniffHeader %q: %s", sniffHeader, err)
	}

	var buf [4]byte
	buf[0] = protocolVersion
	buf[1] = byte(compressType)
	if isTLS {
		buf[2] = 1
	}
	if pipeline {
		buf[3] = 1
	}

	if _, err := conn.Write(buf[:]); err != nil {
		return fmt.Errorf("cannot write connection header: %s", err)
	}
	return nil
}

func handshakeRead(conn net.Conn, sniffHeader []byte, protocolVersion byte) (CompressType, bool, bool, error) {
	sniffBuf := make([]byte, len(sniffHeader))
	if _, err := io.ReadFull(conn, sniffBuf); err != nil {
		return 0, false, false, fmt.Errorf("cannot read sniffHeader: %s", err)
	}
	if !bytes.Equal(sniffBuf, sniffHeader) {
		return 0, false, false, fmt.Errorf("invalid sniffHeader read: %q. Expecting %q", sniffBuf, sniffHeader)
	}

	var buf [4]byte
	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		return 0, false, false, fmt.Errorf("cannot read connection header: %s", err)
	}
	if buf[0] != protocolVersion {
		return 0, false, false, fmt.Errorf("server returned unknown protocol version: %d", buf[0])
	}
	compressType := CompressType(buf[1])
	isTLS := buf[2] != 0
	pipeline := buf[3] != 0

	return compressType, isTLS, pipeline, nil
}

var zeroTime time.Time

type wf interface {
	Flush() error
	io.Writer
}
type writeFlusher struct {
	w wf
}

func (wf *writeFlusher) Write(p []byte) (int, error) {
	n, err := wf.w.Write(p)
	if err != nil {
		return n, err
	}
	if err := wf.w.Flush(); err != nil {
		return 0, err
	}
	return n, nil
}

const MaxBytesSize = 16 << 20

func writeBytes(bw *bufio.Writer, b, sizeBuf []byte) error {
	size := len(b)
	if size > MaxBytesSize {
		return fmt.Errorf("too big size=%d. Must not exceed %d", size, MaxBytesSize)
	}
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(size))
	_, err := bw.Write(sizeBuf)
	if err != nil {
		return fmt.Errorf("cannot write size: %s", err)
	}
	_, err = bw.Write(b)
	if err != nil {
		return fmt.Errorf("cannot write body with size %d: %s", size, err)
	}
	return nil
}

func writeOperation(bw *bufio.Writer, op uint64, opBuf []byte) error {
	binary.BigEndian.PutUint64(opBuf, op)
	_, err := bw.Write(opBuf)
	if err != nil {
		return fmt.Errorf("cannot write size: %s", err)
	}
	return nil
}

func readBytes(br *bufio.Reader, b, sizeBuf []byte) ([]byte, error) {
	_, err := io.ReadFull(br, sizeBuf)
	if err != nil {
		return b, fmt.Errorf("cannot read size: %s", err)
	}
	size := int(binary.BigEndian.Uint32(sizeBuf))
	if size > MaxBytesSize {
		return b, fmt.Errorf("too big size=%d. Must not exceed %d", size, MaxBytesSize)
	}
	if cap(b) < size {
		b = make([]byte, size)
	}
	b = b[:size]
	_, err = io.ReadFull(br, b)
	if err != nil {
		return b, fmt.Errorf("cannot read body with size %d: %s", size, err)
	}
	return b, nil
}

func readOperation(br *bufio.Reader, opBuf []byte) (uint64, error) {
	_, err := io.ReadFull(br, opBuf)
	if err != nil {
		return 0, fmt.Errorf("cannot read size: %s", err)
	}

	return binary.BigEndian.Uint64(opBuf), nil
}

func getFlushTimer() *time.Timer {
	v := flushTimerPool.Get()
	if v == nil {
		return time.NewTimer(time.Hour * 24)
	}
	t := v.(*time.Timer)
	resetFlushTimer(t, time.Hour*24)
	return t
}

func putFlushTimer(t *time.Timer) {
	stopFlushTimer(t)
	flushTimerPool.Put(t)
}

func resetFlushTimer(t *time.Timer, d time.Duration) {
	stopFlushTimer(t)
	t.Reset(d)
}

func stopFlushTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

var flushTimerPool sync.Pool
