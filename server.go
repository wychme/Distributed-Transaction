package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const COORDINATOR = "COORDINATOR"
const SERVER = "SERVER"

type Account struct {
	Name  string
	Value int
	RTS   string `default:""`
	WTS   string `default:""`
}

var serverCount = 5
var active = make(map[string]bool)
var clientConn = make(map[string]net.Conn)
var serverConn = make(map[string]net.Conn)
var idHostMap = make(map[int]string)
var idPortMap = make(map[int]string)
var idNameMap = make(map[int]string)
var hostIdMap = make(map[string]int)
var nameIdMap = make(map[string]int)
var incomingMessage = make(map[string]chan string)
var accounts = make(map[string]Account)             //key is name
var tentative = make(map[string]map[string]Account) //key is session
var tentativeKeys = make([]string, 0)
var m sync.Mutex
var coLock sync.Mutex
var commitCheckMap = make(map[string]int)
var Completed = make(map[string]int)
var Aborted = make(map[string]bool)
var Tx = make(map[string]bool)
var wg sync.WaitGroup

func handleErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func delete_empty(s []string) []string {
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, str)
		}
	}
	return r
}

func handleCoordinator(session string) {
	curNode, err := os.Hostname()
	handleErr(err)
	dependency := make([]string, 0)
	messages := incomingMessage[session]
	for msg := range messages {
		dependency = delete_empty(dependency)
		//fmt.Fprintf(os.Stderr, "Comming: %s\n", msg)
		if strings.Contains(msg, "Message") {
			msg = msg[8:]
			s := strings.Split(msg, " ")
			switch s[0] {
			case "COMMIT":
				txFind := false
				abFind := false
				//fmt.Fprintf(os.Stderr, "Dependencies: %s, size %d\n", dependency, len(dependency))
				for _, v := range dependency {
					_, find := Tx[v]
					if find {
						txFind = true
					}
					_, find = Aborted[v]
					if find {
						abFind = true
						dependency = make([]string, 0)
						abort(session)
						for _, v := range serverConn {
							fmt.Fprintf(v, "Coordinator,%s,%s,%s\n", curNode, session, "ABORT")
						}
						active[session] = false
						fmt.Fprintf(clientConn[session], "%s\n", "ABORTED")
						return
					}
					if !txFind && !abFind {
						for {
							time.Sleep(1 * time.Second)
							_, find = Tx[v]
							if find {
								txFind = true
								break
							}
							_, find = Aborted[v]
							if find {
								abFind = true
								break
							}
						}
					}
					if abFind {
						dependency = make([]string, 0)
						abort(session)
						for _, v := range serverConn {
							fmt.Fprintf(v, "Coordinator,%s,%s,%s\n", curNode, session, "ABORT")
						}
						active[session] = false
						fmt.Fprintf(clientConn[session], "%s\n", "ABORTED")
						return
					}
				}
				if abFind {
					continue
				}
				res := commitCheck(session)
				incomingMessage[session] <- res
				for _, v := range serverConn {
					fmt.Fprintf(v, "Coordinator,%s,%s,%s\n", curNode, session, "COMMIT")
				}
			case "ABORT":
				abort(session)
				for _, v := range serverConn {
					fmt.Fprintf(v, "Coordinator,%s,%s,%s\n", curNode, session, "ABORT")
				}
				dependency = make([]string, 0)
				active[session] = false
				fmt.Fprintf(clientConn[session], "%s\n", "ABORTED")
				return
			case "DEPOSIT", "WITHDRAW", "BALANCE":
				s2 := strings.Split(s[1], ".")
				branch := s2[0]
				//fmt.Fprintf(os.Stderr, "branch: %s, cur: %s\n", branch, idNameMap[hostIdMap[curNode]])
				if branch == idNameMap[hostIdMap[curNode]] {
					res := handleMessage(session, msg)
					incomingMessage[session] <- res
				} else {
					fmt.Fprintf(serverConn[idHostMap[nameIdMap[branch]]], "Coordinator,%s,%s,%s\n", curNode, session, msg)
				}
			case "NOTFOUND":
				abort(session)
				for _, v := range serverConn {
					fmt.Fprintf(v, "Coordinator,%s,%s,%s\n", curNode, session, "ABORT")
				}
				dependency = make([]string, 0)
				active[session] = false
				fmt.Fprintf(clientConn[session], "%s\n", "NOT FOUND, ABORTED")
				return
			default:
				fmt.Fprintf(clientConn[session], "%s\n", "INVALID")
				continue
			}
		} else {
			//fmt.Fprintf(os.Stderr, "Server: %s\n", msg)
			s := strings.Split(msg, ",")
			switch s[0] {
			case "OK":
				if len(s[2]) > 0 {
					s2 := strings.Split(s[2], ";")
					dependency = append(dependency, s2...)
				}
				fmt.Fprintf(clientConn[session], "%s\n", s[0])
			case "BALANCE":
				if len(s[3]) > 0 {
					s2 := strings.Split(s[3], ";")
					dependency = append(dependency, s2...)
				}
				fmt.Fprintf(clientConn[session], "%s\n", s[4]+" = "+s[2])
			case "ABORT":
				incomingMessage[session] <- "Message,ABORT"
			case "CHECK":
				commitCheckMap[session] += 1
				if commitCheckMap[session] == serverCount {
					res := commit(session)
					s = strings.Split(res, ",")
					for _, v := range serverConn {
						fmt.Fprintf(v, "Coordinator,%s,%s,%s\n", curNode, session, "ALLCOMMIT")
					}
					if s[0] == "COMMIT OK" {
						Completed[session] += 1
					}
				}
			case "COMMIT OK":
				{
					Completed[session] += 1
					if Completed[session] == serverCount {
						active[session] = false
						dependency = make([]string, 0)
						fmt.Fprintf(clientConn[session], "%s\n", "COMMIT OK")
						return
					}
				}
			case "NOT FOUND":
				incomingMessage[session] <- "Message,NOTFOUND"
			}

		}
	}
}

func haveAccountUpdate(v Account, a Account, flag string) bool {
	if flag == "READ" {
		if v.WTS >= a.WTS {
			return true
		} else {
			return false
		}
	} else if flag == "WRITE" {
		if v.WTS >= a.WTS && v.RTS >= a.RTS {
			return true
		} else {
			return false
		}
	} else {
		fmt.Fprintf(os.Stderr, "Cannot recognize flag")
		return false
	}
}

func getAccount(name string, flag string) Account {
	a := Account{
		Name:  name,
		Value: 0,
	}
	for i := len(tentativeKeys) - 1; i >= 0; i-- {
		key := tentativeKeys[i]
		for k, v := range tentative[key] {
			if k == name {
				if haveAccountUpdate(v, a, flag) {
					return v
				}
			}
		}
	}

	for k, v := range accounts {
		if k == name {
			if haveAccountUpdate(v, a, flag) {
				return v
			}
		}
	}
	return a
}

func deposit(session string, account string, value string) string {
	a := getAccount(account, "WRITE")
	var dependency []string
	if a.WTS != session {
		dependency = append(dependency, a.WTS)
	}
	if a.RTS != session {
		dependency = append(dependency, a.RTS)
	}

	if session >= a.WTS && session >= a.RTS {
		val, _ := strconv.Atoi(value)
		a.Value += val
		a.WTS = session
		m.Lock()
		_, ok := tentative[session]
		if ok {
			tentative[session][account] = a
		} else {
			tentative[session] = make(map[string]Account)
			tentative[session][account] = a
			tentativeKeys = append(tentativeKeys, session)
		}
		m.Unlock()

		return "OK," + session + "," + strings.Join(dependency, ";")
	} else {
		return "ABORT," + session
	}
}

func withdraw(session string, account string, value string) string {
	a := getAccount(account, "WRITE")
	if a.Value == 0 && a.RTS == "" && a.WTS == "" {
		return "NOT FOUND," + session
	}
	var dependency []string
	if a.WTS != session {
		dependency = append(dependency, a.WTS)
	}
	if a.RTS != session {
		dependency = append(dependency, a.RTS)
	}

	if session >= a.WTS && session >= a.RTS {
		val, _ := strconv.Atoi(value)
		a.Value -= val
		a.WTS = session
		m.Lock()
		_, ok := tentative[session]
		if ok {
			tentative[session][account] = a
		} else {
			tentative[session] = make(map[string]Account)
			tentative[session][account] = a
			tentativeKeys = append(tentativeKeys, session)
		}
		m.Unlock()

		return "OK," + session + "," + strings.Join(dependency, " ")
	} else {
		return "ABORT," + session
	}
}

func balance(session string, account string) string {
	a := getAccount(account, "READ")
	if a.Value == 0 && a.RTS == "" && a.WTS == "" {
		return "NOT FOUND," + session
	}
	var dependency []string
	if a.WTS != session {
		dependency = append(dependency, a.WTS)
	}

	if session >= a.WTS {
		a.RTS = session
		m.Lock()
		_, ok := tentative[session]
		if ok {
			tentative[session][account] = a
		} else {
			tentative[session] = make(map[string]Account)
			tentative[session][account] = a
			tentativeKeys = append(tentativeKeys, session)
		}
		m.Unlock()
		return "BALANCE," + session + "," + strconv.Itoa(a.Value) + "," + strings.Join(dependency, " ")
	} else {
		return "ABORT," + session
	}
}

func commitCheck(session string) string {
	_, ok := tentative[session]
	if ok {
		for _, v := range tentative[session] {
			if v.Value < 0 {
				return "ABORT," + session
			}
		}
	}
	return "CHECK," + session
}

func commit(session string) string {
	m.Lock()
	_, ok := tentative[session]
	if ok {
		for k, v := range tentative[session] {
			accounts[k] = v
		}
		delete(tentative, session)
		for i, v := range tentativeKeys {
			if v == session {
				tentativeKeys = append(tentativeKeys[:i], tentativeKeys[i+1:]...)
				break
			}
		}
	}
	m.Unlock()
	coLock.Lock()
	Tx[session] = true
	coLock.Unlock()
	first := true
	for _, v := range accounts {
		if v.Value != 0 {
			if first {
				fmt.Fprintf(os.Stdout, "%s = %d", v.Name, v.Value)
				first = false
			} else {
				fmt.Fprintf(os.Stdout, ", %s = %d", v.Name, v.Value)
			}
		}
	}
	if !first {
		fmt.Fprintf(os.Stdout, "\n")
	}
	return "COMMIT OK," + session
}

func abort(session string) string {
	m.Lock()
	_, ok := tentative[session]
	if ok {
		delete(tentative, session)
		for i, v := range tentativeKeys {
			if v == session {
				tentativeKeys = append(tentativeKeys[:i], tentativeKeys[i+1:]...)
				break
			}
		}
	}
	m.Unlock()
	coLock.Lock()
	Aborted[session] = true
	coLock.Unlock()
	return "ABORTED," + session
}

func handleMessage(session string, msg string) string {
	s := strings.Split(msg, " ")
	//fmt.Fprintf(os.Stderr, "Handle %s\n", msg)
	var res string
	switch s[0] {
	case "DEPOSIT", "WITHDRAW":
		s2 := strings.Split(s[1], ".")
		account := s2[1]
		value := s[2]
		if s[0] == "DEPOSIT" {
			res = deposit(session, account, value)
		} else {
			res = withdraw(session, account, value)
		}
	case "BALANCE":
		s2 := strings.Split(s[1], ".")
		account := s2[1]
		res = balance(session, account) + "," + s[1]
	case "COMMIT":
		res = commitCheck(session)
	case "ALLCOMMIT":
		res = commit(session)
	case "ABORT":
		res = abort(session)
	}
	return res
}

func handleServerConnection(c net.Conn) {
	wg.Wait()
	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		//fmt.Fprintln(os.Stderr, scanner.Text())
		s := strings.Split(scanner.Text(), ",")
		if s[0] == "Client" {
			if s[1] == "Coordinator" {
				conn, err := net.Dial("tcp", s[2])
				handleErr(err)
				session := s[3]
				fmt.Fprintf(conn, "OK\n")
				clientConn[session] = conn
				incomingMessage[session] = make(chan string, 65535)
				active[session] = true
				go handleCoordinator(session)
			} else if s[1] == "Message" {
				session := s[2]
				if !active[session] {
					fmt.Fprintf(clientConn[session], "%s\n", "INVALID")
					continue
				}
				incomingMessage[session] <- "Message," + s[3]
			}
		} else if s[0] == "Coordinator" {
			session := s[2]
			msg := s[3]
			res := handleMessage(session, msg)
			fmt.Fprintf(serverConn[s[1]], "Server,%s\n", res)
		} else if s[0] == "Server" {
			incomingMessage[s[2]] <- scanner.Text()[7:]
		} else {
			fmt.Fprintf(os.Stderr, "Cannot recognize %s\n", s[0])
		}
	}
}

func handleServerListener(ln net.Listener) {
	var c net.Conn
	var err error = nil
	for err == nil {
		c, err = ln.Accept()
		go handleServerConnection(c)
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
	i := 0
	for scanner.Scan() {
		s := strings.Split(scanner.Text(), " ")
		idNameMap[i] = s[0]
		idHostMap[i] = s[1]
		idPortMap[i] = s[2]
		nameIdMap[s[0]] = i
		hostIdMap[s[1]] = i
		i++
	}
	curNode, err := os.Hostname()
	curNodeId := hostIdMap[curNode]
	handleErr(err)
	ln, err := net.Listen("tcp", ":"+idPortMap[curNodeId])
	go handleServerListener(ln)
	wg.Add(1)
	for i = 0; i < serverCount; i++ {
		if i == curNodeId {
			continue
		}
		for {
			serverConn[idHostMap[i]], err = net.Dial("tcp", idHostMap[i]+":"+idPortMap[i])
			if err == nil {
				//fmt.Fprintf(os.Stderr, "Connected to %s:%s\n", idHostMap[i], idPortMap[i])
				break
			}
		}
	}
	wg.Done()
	select {}
}
