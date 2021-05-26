package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var idHostMap = make(map[int]string)
var idPortMap = make(map[int]string)
var idNameMap = make(map[int]string)
var incomingMessage chan string
var conn net.Conn

func handleErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

var block = false
var wg sync.WaitGroup

func handleConnection(c net.Conn) {
	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		if scanner.Text() != "INVALID" {
			fmt.Fprintln(os.Stdout, scanner.Text())
		}
		wg.Done()
	}
}
func handleListener(ln net.Listener) {
	var c net.Conn
	var err error = nil
	for err == nil {
		c, err = ln.Accept()
		go handleConnection(c)
	}
	_ = ln.Close()
}

func main() {
	argv := os.Args[1:]
	config := argv[0]
	file, err := os.Open(config)
	handleErr(err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	incomingMessage = make(chan string, 65535)
	i := 0
	for scanner.Scan() {
		s := strings.Split(scanner.Text(), " ")
		idNameMap[i] = s[0]
		idHostMap[i] = s[1]
		idPortMap[i] = s[2]
		i++
	}
	tid := ""
	curNode, err := os.Hostname()
	handleErr(err)
	go scanMessage()
	ln, err := net.Listen("tcp", ":0")
	port := ln.Addr().String()[5:]
	handleErr(err)
	go handleListener(ln)
	rand.Seed(time.Now().UnixNano())
	for msg := range incomingMessage {
		s := strings.Split(msg, " ")
		switch s[0] {
		case "BEGIN":
			coordinator := rand.Intn(5)
			//coordinator := 0
			tid = strconv.FormatInt(time.Now().UnixNano(), 10)
			conn, err = net.Dial("tcp", idHostMap[coordinator]+":"+idPortMap[coordinator])
			fmt.Fprintf(conn, "Client,Coordinator,"+curNode+":"+port+","+tid+"\n")
			wg.Add(1)
		default:
			if conn != nil {
				fmt.Fprintf(conn, "Client,Message,"+tid+","+msg+"\n")
				wg.Add(1)
			}
		}
		wg.Wait()
	}
}

func scanMessage() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		incomingMessage <- scanner.Text()
	}
}
