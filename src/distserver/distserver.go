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
	go waitReturn(chlist,om.Uid,end)
	for _,v := range ports {
		chlist[index] = v.CreateRoutine(&om)
		index++
	}
	print("Created Routine ",om.Uid,"\n")
	// wait for all five to return
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
	go EndTask(numtasks,donechan)
	for i:=0 ; i<numtasks;i++ {
		om := distio.OutMessage{i,100, "Hurrah"}
		//print("Starting process ",strconv.Itoa(i),"\n")
		distribute(ports,om,donechan)
		//time.Sleep(10*time.Millisecond)
	}
	//result := 0
}

func EndTask(numtasks int, donechan chan<- int) {
     	for i:=0; i<numtasks; i++ {
		_ = <- donechan
		print("Process ",i," completed\n")
	}
}