/*  
Copyright (C) <2013> <Paramananda Ponnaiyan>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the "Software"), to deal in the Software without restriction, including 
without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the 
following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions 
of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 

IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, 
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE 
OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
package distio
import "net"
import "sync"
import "strings"
import "strconv"
import "io"
import "encoding/binary"
import "encoding/json"
import "time"
import "log"
// define the Message
type InMessage struct {
	Uid int
	InpInt int
}

type OutMessage struct {
	Uid int //gets overwritten in sendMessage
	OutInt int
	OutStr string
}

type PortBaby struct {
	conn *net.Conn
	txMut sync.Mutex
	rxMut sync.Mutex
	respMut sync.Mutex 
	respMap map[int] chan<- InMessage
	readBuf []byte
}

func (v *PortBaby)InitBuffer() {
	v.readBuf = make([]byte,1024)
}

func (v *PortBaby)CreateConn(hostname string, port int, connType string) {
	hoststring := strings.Join([]string{hostname,strconv.Itoa(port)},":")
	conn,err := net.Dial(connType,hoststring)
	if err != nil {
		panic(strings.Join([]string{"Error Connecting to",hoststring}," "))
	}
	v.conn = &conn
	v.respMap = make(map[int]chan<- InMessage)
	v.readBuf = make([]byte,1024)
	go v.ReaderProc()
}

func (v *PortBaby) ReaderProc() {
	for {
		v.readMessage()
	}
}
func (v *PortBaby)CreateRoutine (om *OutMessage) <-chan InMessage {
	respchan := make(chan InMessage) 
	v.respMut.Lock()
	v.respMap[om.Uid]=respchan 
	v.sendMessage(om)
	v.respMut.Unlock()
	return respchan
}

func (v *PortBaby)readMessage() {
	//read first 2 bytes which is the message size
	//then read till the end
	nlen,err := io.ReadFull(*v.conn,v.readBuf[:2])
	if err != nil {
		panic("Could not read Full")
	}
	msgbytes := []byte{v.readBuf[1],v.readBuf[0],0,0,0,0,0,0}
	msglen,merr := binary.Uvarint(msgbytes)
	if merr <= 0 {
		panic("Failed to convert msgbytes")
	}
	nlen,err = io.ReadFull(*v.conn,v.readBuf[:msglen])
	rlen := uint64(nlen)
	if err != nil || rlen != msglen{
		panic("Could not read Full Message")
	}
	var im InMessage
	err = json.Unmarshal(v.readBuf[:msglen],&im)
	if err != nil {
		panic("Could not unmarshal data")
	}
	v.respMut.Lock()
	rchan := v.respMap[im.Uid]
	delete(v.respMap,im.Uid)
	v.respMut.Unlock()
	rchan<-im
}

func (v *PortBaby)sendMessage (om *OutMessage) {
	jenc,err := json.Marshal(om)
	if err != nil {
		panic("Cannot convert OutMessage into json string")
	}
	// write the size
	nlen := len(jenc)
	fb := uint8(((nlen >> 8)&0xFF))
	nb := uint8((nlen&0xFF))
	c := *(v.conn)
	nwrit,werr := c.Write([]byte{fb,nb})
	if nwrit != 2 || werr != nil {
		panic("failed writing size")
	}
	nwrit,werr = c.Write(jenc)
	if nwrit != len(jenc) || werr != nil {
		panic("could not write all objects")
	}
}

func (v *PortBaby)SendStop () {
	fb := uint8(0)
	nb := uint8(0)
	c := *(v.conn)
	nwrit,werr := c.Write([]byte{fb,nb})
	if nwrit != 2 || werr != nil {
		panic("failed writing size")
	}
}

// server side

func (v *PortBaby) HandleTask(conn *net.Conn) {
	v.conn = conn
	i := 0
	for {
	//read first 2 bytes which is the message size
	//then read till the end
	nlen,err := io.ReadFull(*v.conn,v.readBuf[:2])
	if err != nil {
		log.Print(strings.Join([]string{"Could not read Full:",err.Error()},":"))
		return
	}
	msgbytes := []byte{v.readBuf[1],v.readBuf[0],0,0,0,0,0,0}
	msglen,merr := binary.Uvarint(msgbytes)
	if msglen == 0 {
			log.Print("Ending Session\n")
			return
	}
	if merr <= 0 {
		log.Print("Failed to convert msgbytes")
		return
	}
	nlen,err = io.ReadFull(*v.conn,v.readBuf[:msglen])
	rlen := uint64(nlen)
	if err != nil || rlen != msglen{
		log.Print("Could not read Full Message")
		return
	}

	obuf := make([]byte,int(msglen))
	// copying the data to a local buffer
	copy(obuf,v.readBuf[:msglen])

	// do the actual work of unmarshalling and responding
	go v.TaskWrite(i,obuf)
	i++
	}
}

func (v *PortBaby)TaskWrite(workindex int, obuf []byte) {
	// do work
	var im OutMessage
	jerr := json.Unmarshal(obuf,&im)
	if jerr != nil {
		panic(strings.Join([]string{"Could not unmarshal data",jerr.Error()},":"))
	}

	sleepms := im.OutInt
	time.Sleep(time.Duration(sleepms) * time.Millisecond)

	// populate response message
	var om InMessage
	om.Uid = im.Uid
	om.InpInt = sleepms
	// send it
	jenc,err := json.Marshal(om)
	if err != nil {
		panic("Cannot convert OutMessage into json string")
	}
	// write the size
	nlen := len(jenc)
	fb := uint8(((nlen >> 8)&0xFF))
	nb := uint8((nlen&0xFF))
	c := *(v.conn)
	v.txMut.Lock()
	nwrit,werr := c.Write([]byte{fb,nb})
	if nwrit != 2 || werr != nil {
		panic("failed writing size")
	}
	nwrit,werr = c.Write(jenc)
	if nwrit != len(jenc) || werr != nil {
		panic("could not write all objects")
	}
	v.txMut.Unlock()
}

func CreateTaskServer(port int, end chan<- int) {
	portstring := strings.Join([]string{":",strconv.Itoa(port)},"")
	ln, err := net.Listen("tcp", portstring)
	if err != nil {
		// handle error
		panic("failed to start server")
	}
	for {
		conn, err := ln.Accept()
		rd := PortBaby{}
		rd.InitBuffer()
		if err != nil {
		// handle error
			end <- port
			break
		}
		go rd.HandleTask(&conn)
	}
}
