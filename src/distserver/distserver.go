package main
import "dist/distconfig"
import "dist/distio"
import "log"
//import "time"
func main() {
	distconfig.ReadAndParseConfig("services.yaml");
	hosts := distconfig.GetHostsConfig();
	// Create the Port Managers
	ports := make([]distio.PortBaby,len(hosts))
	index := 0
	for _,v := range hosts {
		ports[index].CreateConn(v.Hostname,v.Port,"tcp")
		//print("Creating",v.Hostname,":",v.Port,"\n");
		index++
	}
	sendData(ports)
}

func distribute(ports []distio.PortBaby, om distio.OutMessage, end chan<- int) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("work failed:", err)
		}
	}()
	chlist := make([]<-chan distio.InMessage,len(ports))
	index := 0
	for _,v := range ports {
		chlist[index] = v.CreateRoutine(&om)
		index++
	}
	// wait for all five to return
	go waitReturn(chlist,om.Uid,end)
}

func waitReturn(chlist []<-chan distio.InMessage,uid int,end chan<- int){
	for _,result := range chlist {
		_ = <-result
	}
	end <- uid
}

func sendData(ports []distio.PortBaby) {
	donechan := make(chan int)
	numtasks := 1000
	for i:=0 ; i<numtasks;i++ {
		om := distio.OutMessage{i,100, "Hurrah"}
		distribute(ports,om,donechan)
	}
	EndTask(ports,numtasks,donechan)
}

func EndTask(ports []distio.PortBaby,numtasks int, donechan <-chan int) {
     	for i:=0; i<numtasks; i++ {
		_ = <- donechan
	}
	for _,v := range ports {
		v.SendStop()
	}
}