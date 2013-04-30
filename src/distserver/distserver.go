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