package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	queueingSystem "distributed-systems-message-queue/src/queue"
)

const (
	queue_capacity = 10
)

func serverWriteTo(name string, readConn net.Conn, queues []*queueingSystem.Queue) {
	for {
		for _, queue := range queues {
			if queue.IsEmpty() {
				continue
			}

			message, _ := queue.Dequeue()

			log.Println("LOG:", `send message to the `+name)

			sendMessage(readConn, message)

			// log.Println("LOG:", "client received request")
		}
	}
}

func writeTo(name string, readConn net.Conn, queue *queueingSystem.Queue) {
	for {
		for queue.IsEmpty() {

		}

		message, _ := queue.Dequeue()

		log.Println("LOG:", `send message to the `+name)

		sendMessage(readConn, message)

		// log.Println("LOG:", "client received request")
	}
}

func handleMessagePassingAsynchronously(serverConn, clientReadConn,
	clientWriteConn net.Conn, sourceQueue *queueingSystem.Queue, handleBufferOverflow bool) {
	signals := make(chan string)

	go handleCLient(clientReadConn, clientWriteConn, sourceQueue, signals, handleBufferOverflow)
	go handleServer(serverConn, sourceQueue, signals)

	for {
		time.Sleep(10 * time.Second)
		log.Println("LOG:", "doing something ...")
	}
}

func runServer(name string, serverReadConn, serverWriteConn net.Conn, sourceQueue []*queueingSystem.Queue, destinationQueue *queueingSystem.Queue, handleBufferOverflow bool) {
	go readFrom(name, serverWriteConn, destinationQueue, handleBufferOverflow)
	go serverWriteTo(name, serverReadConn, sourceQueue)
}

func runClients(name string, readConns, writeConns []net.Conn, sourceQueues []*queueingSystem.Queue, destinationQueue *queueingSystem.Queue, handleBufferOverflow bool) {
	for i, queue := range sourceQueues {
		go readFrom(name+" "+fmt.Sprint(i), writeConns[i], queue, handleBufferOverflow)
	}

	go writeTo(name, readConns[0], destinationQueue)
}

func getClientsNumber() int {
	fmt.Print("Enter number of clients: ")
	input, _ := bufio.NewReader(os.Stdin).ReadString('\n')

	result, _ := strconv.Atoi(strings.TrimSpace(input))

	return result
}

func handleAsync(handleBufferOverflow bool) {
	serverReadPort, serverWritePort := getPorts("server")

	serverReadConn, serverWriteConn := createTwoWayServer(serverReadPort, serverWritePort)

	readConns := make([]net.Conn, 0)
	writeConns := make([]net.Conn, 0)
	sourceQueues := make([]*queueingSystem.Queue, 0)

	clientsNumber := getClientsNumber()

	for i := 0; i < clientsNumber; i++ {
		clientReadPort, clientWritePort := getPorts("client")
		clientReadConn, clientWriteConn := createTwoWayServer(clientReadPort, clientWritePort)
		readConns = append(readConns, clientReadConn)
		writeConns = append(writeConns, clientWriteConn)

		sourceQueue := queueingSystem.CreateQueue(queue_capacity)
		sourceQueues = append(sourceQueues, sourceQueue)
	}

	destinationQueue := queueingSystem.CreateQueue(queue_capacity)

	go runClients("client", readConns, writeConns, sourceQueues, destinationQueue, handleBufferOverflow)

	go runServer("server", serverReadConn, serverWriteConn, sourceQueues, destinationQueue, handleBufferOverflow)

	for {
		time.Sleep(10 * time.Second)
		log.Println("LOG:", "doing something ...")
	}
}

func handleSync() {
	serverReadPort, serverWritePort := getPorts("server")
	clientReadPort, clientWritePort := getPorts("client")

	serverReadConn, serverWriteConn := createTwoWayServer(serverReadPort, serverWritePort)
	clientReadConn, clientWriteConn := createTwoWayServer(clientReadPort, clientWritePort)

	sourceQueue := queueingSystem.CreateQueue(queue_capacity)

	for {
		_, err := receiveMessage(clientWriteConn, sourceQueue)

		handleError(err)

		log.Println("LOG:", "client request is received")

		message, err := sourceQueue.Dequeue()

		handleError(err)

		log.Println("LOG:", `send the request to the server and wait until received`)

		sendMessage(serverReadConn, message)

		log.Println("LOG:", "server received request")

		_, err = receiveMessage(serverWriteConn, sourceQueue)

		handleError(err)

		message, err = sourceQueue.Dequeue()

		handleError(err)

		log.Println("LOG:", `send an acknowledgment to the client and wait until received`)

		sendMessage(clientReadConn, message)

		log.Println("LOG:", "client received request")
	}
}

func handleMultiWayMessaging(messagePassingMode string, handleBufferOverflow bool) {
	switch messagePassingMode {
	case "sync":
		handleSync()
	case "async":
		handleAsync(handleBufferOverflow)
	default:
		log.Println("ERROR:", "mode does not exist")
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

func readFrom(name string, writeConn net.Conn, queue *queueingSystem.Queue, handleBufferOverflow bool) {
	for {
		_, err := receiveMessage(writeConn, queue)

		if handleBufferOverflow && err != nil {
			// clientWriteConn.Close()
			log.Println("ERROR:", err)
			time.Sleep(30 * time.Second)
		} else {
			handleError(err)
			log.Println("LOG:", name+" request is received")
		}

	}
}

func handleCLient(clientReadConn, clientWriteConn net.Conn,
	sourceQueue *queueingSystem.Queue, signals chan string, handleBufferOverflow bool) {
	go readFrom("client", clientWriteConn, sourceQueue, handleBufferOverflow)
	go writeToClient(clientReadConn, signals)
}

func sendMessage(conn net.Conn, message string) {
	fmt.Fprintf(conn, message+"\n")
}

func receiveMessage(conn net.Conn, q *queueingSystem.Queue) (string, error) {
	readData, err := bufio.NewReader(conn).ReadString('\n')

	handleError(err)

	err = q.Enqueue(string(readData))
	log.Println("LOG:", "enqueued to queue", "SIZE:", q.GetSize())

	return readData, err
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

func createTwoWayServer(ReadPort, WritePort string) (net.Conn,
	net.Conn) {

	readConn, err := createTCPserver(ReadPort)

	handleError(err)

	writeConn, err := createTCPserver(WritePort)

	handleError(err)

	return readConn, writeConn
}

func createOneWayServer(ReadPort string) net.Conn {
	readConn, err := createTCPserver(ReadPort)

	handleError(err)

	return readConn
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

func getPorts(name string) (string, string) {
	fmt.Println("Enter input: <" + name + " reading port> <" + name + " writing port>")
	input, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	inputs := strings.Split(strings.TrimSpace(input), " ")

	return inputs[0], inputs[1]
}

func getPort(name string) string {
	fmt.Println("Enter input: <" + name + " reading port>")
	input, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	inputs := strings.Split(strings.TrimSpace(input), " ")

	return inputs[0]
}

func handleOneWayMessaging(messagePassingMode string, handleBufferOverflow bool) {
	serverPort := getPort("server")
	clientReadPort, clientWritePort := getPorts("client")

	serverConn := createOneWayServer(serverPort)
	clientReadConn, clientWriteConn := createTwoWayServer(clientReadPort, clientWritePort)

	sourceQueue := queueingSystem.CreateQueue(queue_capacity)

	switch messagePassingMode {
	case "sync":
		handleMessagePassingSynchronously(serverConn, clientReadConn,
			clientWriteConn, sourceQueue)
	case "async":
		handleMessagePassingAsynchronously(serverConn, clientReadConn,
			clientWriteConn, sourceQueue, handleBufferOverflow)
	default:
		log.Println("ERROR:", "mode does not exist")
	}
}

func handleMessagePassing(messagingMode, messagePassingMode string, handleBufferOverflow bool) {
	switch messagingMode {
	case "one":
		handleOneWayMessaging(messagePassingMode, handleBufferOverflow)
	case "multi":
		handleMultiWayMessaging(messagePassingMode, handleBufferOverflow)
	default:
		log.Println("ERROR:", "mode does not exist")
	}

}

func getHandleBufferOverflow() bool {
	arguments := os.Args

	result, _ := strconv.ParseBool(arguments[3])
	return result
}

func getMessagePassingMode() string {
	arguments := os.Args

	return arguments[2]
}

func getMessagingMode() string {
	arguments := os.Args

	return arguments[1]
}

func getCommandLineArguments() (string, string, bool) {
	messagingMode := getMessagingMode()
	messagePassingMode := getMessagePassingMode()
	handleBufferOverflow := getHandleBufferOverflow()
	return messagingMode, messagePassingMode, handleBufferOverflow
}

func handleError(err error) {
	if err != nil {
		log.Println("ERROR: ", err)
		os.Exit(1)
	}
}

func checkCommandLineArguments() error {
	arguments := os.Args

	if len(arguments) < 4 {
		return errors.New(`error: too few arguments. please provide please provide <MessagingMode> <MessagePassingMode> <HandleBufferOverflow>`)
	} else if len(arguments) > 4 {
		fmt.Println()
		return errors.New(`error: too many arguments. please provide <MessagingMode> <MessagePassingMode> <HandleBufferOverflow>`)
	}

	return nil
}

// go run buffer_overflow/buffer_overflow.go one asynchronously true 8085 8086 8087 8087
func main() {
	err := checkCommandLineArguments()

	handleError(err)

	messagingMode, messagePassingMode, handleBufferOverflow := getCommandLineArguments()

	handleMessagePassing(messagingMode, messagePassingMode, handleBufferOverflow)
}
