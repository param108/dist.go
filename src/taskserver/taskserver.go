package main
import "dist/distconfig"
import "dist/distio"
//import "strconv"
func main() {
	distconfig.ReadAndParseConfig("services.yaml");
	hosts := distconfig.GetHostsConfig();
	endchan := make(chan int)
	// Create the Port Managers
	for _,v := range hosts {
		go distio.CreateTaskServer(v.Port,endchan)
	}

	//result := 0
	for i:=0;i<len(hosts);i++ {
		_ = <-endchan
	}
}
