package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	queueingSystem "distributed-systems-message-queue/src/queue"
)

const (
	queue_capacity = 10
)

func sendMessage(conn net.Conn, message string) {
	fmt.Fprintf(conn, message+"\n")
}

func receiveMessage(conn net.Conn, q *queueingSystem.Queue) (string, error) {
	readData, err := bufio.NewReader(conn).ReadString('\n')

	q.Enqueue(string(readData))
	log.Println("LOG:", "enqueued to queue", "SIZE:", q.GetSize())

	return readData, err
}

func createTCPserver(port string) (net.Conn, error) {
	listener, err := net.Listen("tcp", ":"+port)

	if err != nil {
		fmt.Println(err)
	}
	defer listener.Close()

	conn, err := listener.Accept()
	if err != nil {
		fmt.Println(err)
	}

	log.Println("LOG:", "established a TCP connection with client "+
		conn.LocalAddr().String())

	return conn, err
}

func handleMessagePassingSynchronously(serverConn, clientReadConn,
	clientWriteConn net.Conn, sourceQueue *queueingSystem.Queue) {
	for {
		_, err := receiveMessage(clientWriteConn, sourceQueue)

		handleError(err)

		log.Println("LOG:", "client request is received")

		message, err := sourceQueue.Dequeue()

		handleError(err)

		log.Println("LOG:", `send the request to the server and wait until received`)

		sendMessage(serverConn, message)

		log.Println("LOG:", "server received request")
		log.Println("LOG:", `send an acknowledgment to the client and wait until received`)

		ackMessage := strings.TrimSpace(message) + " has reached the server successfully"

		sendMessage(clientReadConn, ackMessage)

		log.Println("LOG:", "client received request")
	}
}

func handleServer(serverConn net.Conn, sourceQueue *queueingSystem.Queue,
	signals chan string) {
	for {
		if sourceQueue.IsEmpty() {
			continue
		}

		message, err := sourceQueue.Dequeue()

		handleError(err)

		log.Println("LOG:", `send the request to the server`)

		sendMessage(serverConn, message)

		// log.Println("LOG:", "server received request")

		signals <- strings.TrimSpace(message) + " has reached the server successfully"

		time.Sleep(8 * time.Second)
	}
}

func writeToClient(clientReadConn net.Conn, signals chan string) {
	for {
		message := <-signals

		log.Println("LOG:", `send an acknowledgment to the client`)

		sendMessage(clientReadConn, message)

		// log.Println("LOG:", "client received request")
	}
}

func readFromClient(clientWriteConn net.Conn, sourceQueue *queueingSystem.Queue) {
	for {
		_, err := receiveMessage(clientWriteConn, sourceQueue)

		handleError(err)

		log.Println("LOG:", "client request is received")
	}
}

func handleCLient(clientReadConn, clientWriteConn net.Conn,
	sourceQueue *queueingSystem.Queue, signals chan string) {
	go readFromClient(clientWriteConn, sourceQueue)
	// fmt.Println(clientReadConn.LocalAddr())
	go writeToClient(clientReadConn, signals)
}

func handleMessagePassingAsynchronously(serverConn, clientReadConn,
	clientWriteConn net.Conn, sourceQueue *queueingSystem.Queue) {
	signals := make(chan string)

	go handleCLient(clientReadConn, clientWriteConn, sourceQueue, signals)
	go handleServer(serverConn, sourceQueue, signals)

	for {
		time.Sleep(10 * time.Second)
		log.Println("LOG:", "doing something ...")
	}
}

func handleMessagePassing(messagePassingMode string, serverConn, clientReadConn,
	clientWriteConn net.Conn, sourceQueue *queueingSystem.Queue) {
	switch messagePassingMode {
	case "synchronously":
		handleMessagePassingSynchronously(serverConn, clientReadConn,
			clientWriteConn, sourceQueue)
	case "asynchronously":
		handleMessagePassingAsynchronously(serverConn, clientReadConn,
			clientWriteConn, sourceQueue)
	default:
		log.Println("ERROR:", "mode does not exist")
	}
}

func connect(serverPort, clientReadPort, clientWritePort string) (net.Conn,
	net.Conn, net.Conn) {
	serverConn, err := createTCPserver(serverPort)

	handleError(err)

	clientReadConn, err := createTCPserver(clientReadPort)

	handleError(err)

	clientWriteConn, err := createTCPserver(clientWritePort)

	handleError(err)

	return serverConn, clientReadConn, clientWriteConn
}

func getPortNumbers() (string, string, string) {
	arguments := os.Args

	return arguments[2], arguments[3], arguments[4]
}

func getMessagePassingMode() string {
	arguments := os.Args

	return arguments[1]
}

func getCommandLineArguments() (string, string, string, string) {
	messagePassingMode := getMessagePassingMode()
	serverPort, clientReadPort, clientWritePort := getPortNumbers()
	return messagePassingMode, serverPort, clientReadPort, clientWritePort
}

func handleError(err error) {
	if err != nil {
		log.Println("ERROR: ", err)
		os.Exit(1)
	}
}

func checkCommandLineArguments() error {
	arguments := os.Args

	if len(arguments) < 5 {
		return errors.New(`error: too few arguments. please provide port
		 numbers for reading and writing`)
	} else if len(arguments) > 5 {
		fmt.Println()
		return errors.New(`error: too many arguments. please provide 
		port numbers for reading and writing`)
	}

	return nil
}

func main() {
	err := checkCommandLineArguments()

	handleError(err)

	messagePassingMode, serverPort, clientReadPort, clientWritePort := getCommandLineArguments()

	serverConn, clientReadConn, clientWriteConn := connect(serverPort,
		clientReadPort, clientWritePort)

	sourceQueue := queueingSystem.CreateQueue(queue_capacity)

	handleError(err)

	handleMessagePassing(messagePassingMode, serverConn, clientReadConn,
		clientWriteConn, sourceQueue)
}
