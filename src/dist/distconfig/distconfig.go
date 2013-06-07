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

package distconfig
//import "net"
import "launchpad.net/goyaml"
import "os"
import "io"
import "strings"
import "log"
//import "fmt"
//import "strconv"

var GlobalConfig map[interface{}]interface{}
var CachedConfig map[string]interface{}

type HostInfo struct {
	Hostname string
	Port int
}
func ReadAndParseConfig(fname string) {
	fi, err := os.Open(fname);
	if err != nil {
		log.Fatal(strings.Join([]string{"Could not open config file",err.Error()},":"))
	}
	defer func() {
		if err := fi.Close(); err != nil {
			panic(strings.Join([]string{"Failed to close config file",err.Error()},":"))
		}
	}()
	
	// get the size
	finfo,err := fi.Stat()
	
	// make a buffer to hold the data
	buf := make([]byte, finfo.Size())
	n, err := fi.Read(buf)
	n64 := int64(n);
	if err != nil && err != io.EOF { panic(strings.Join([]string{"Failed to read config file",err.Error()},":")) }
	if n64 != finfo.Size() {
		panic("Didnt read adequate bytes from config file");
	}
	
	m := make(map[interface{}]interface{})
	err = goyaml.Unmarshal(buf,&m);
	if err != nil {
		panic(strings.Join([]string{"Failed to unmarshal config",err.Error()},":"))
	}
	GlobalConfig = m
	if CachedConfig == nil {
		CachedConfig = make(map[string]interface{})
	} else {
		for k,_ := range CachedConfig {
			delete(CachedConfig,k)
		}
	}
}
func GetHostsConfig() []HostInfo {
	oconfig,found := CachedConfig["hosts"].([]HostInfo)
	if (found) {
		return oconfig
	}

	// need to parse the host config
	hosts := GlobalConfig["hosts"]
	table,ftable := hosts.([]interface{})
	numentries := len(table)
	newconfig := make([]HostInfo,numentries)
	if (ftable) {
		index := 0;
		for _,v := range table {
			hostent,hfound := v.(map[interface{}]interface{})
			if (hfound) {
				hostname,hok := hostent["host"].(string)
				port,pok := hostent["port"].(int)
				if hok == false || pok == false {
					panic("Hosts config in config file is corrupt:key type or value type")
				}
				newconfig[index].Hostname = hostname
				newconfig[index].Port = port
			} else {
				panic("Hosts config in config file is corrupt")
			}
			index++
		}
	} else {
		panic("Hosts config in config file is corrupt:Highest Level")
	}
	CachedConfig["hosts"] = newconfig
	return newconfig
}


