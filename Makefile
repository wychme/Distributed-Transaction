all:
	go build server.go
	go build client.go

clean:
	go clean
