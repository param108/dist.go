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
package main
import "dist/distconfig"
import "dist/distio"
import "log"
//import "time"
func main() {
	numprocs := 10
	numtasks := 10000
	numconc := 100

	end := make (chan int)
	theend := make (chan int)

	distconfig.ReadAndParseConfig("services.yaml");
	hosts := distconfig.GetHostsConfig();
	portdist := make([][]distio.PortBaby,numprocs)
	sendchans := make([]chan *distio.OutMessage,numprocs)
	for i:=0; i<numprocs;i++ {
		portdist[i] = make([]distio.PortBaby,len(hosts))
		sendchans[i] = make(chan *distio.OutMessage,10)
		index := 0
		for _,v := range hosts {
			portdist[i][index].CreateConn(v.Hostname,v.Port,"tcp")
			//print("Creating",v.Hostname,":",v.Port,"\n");
			index++
		}
		go distribute(portdist[i],sendchans[i],end)
	}

	go EndTask(portdist,numtasks,end,theend)
	cncount := numtasks/numconc
	for i:=0;i<numconc;i++ {
		go Concurrents(cncount,sendchans,i*cncount)
	}
	_ = <-theend
}

func Concurrents(numtasks int,sendchans[]chan *distio.OutMessage,starts int) {
	numprocs := len(sendchans)
	for i:=starts ; i<(starts + numtasks);i++ {
		om := distio.OutMessage{i,100, "Hurrah"}
		// Create the Port Managers
		sendchans[i%numprocs] <- &om
	}
}

func distribute(ports []distio.PortBaby, omchan <-chan *distio.OutMessage, end chan<- int) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("work failed:", err)
		}
	}()
	for {
	chlist := make([]<-chan distio.InMessage,len(ports))
	index := 0
	om := <- omchan
	for _,v := range ports {
		chlist[index] = v.CreateRoutine(om)
		index++
	}
	// wait for all five to return
	go waitReturn(chlist,om.Uid,end)
	}
}

func waitReturn(chlist []<-chan distio.InMessage,uid int,end chan<- int){
	for _,result := range chlist {
		_ = <-result
	}
	end <- uid
}


func EndTask(ports [][]distio.PortBaby,numtasks int, donechan <-chan int, theend chan<- int) {
     	for i:=0; i<numtasks; i++ {
		_ = <- donechan
	}
	for _,v := range ports {
		for _,w := range v {
			w.SendStop()
		}
	}

	theend <- 1
}
