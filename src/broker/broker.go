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

// Fucntion to write a message from client corresponding queue to server.
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

// Fucntion to write a message that is from a queue to a connection.
func writeTo(name string, readConns []net.Conn, queue *queueingSystem.Queue) {
	for {
		if queue.IsEmpty() {
			continue
		}
		message, _ := queue.Dequeue()
		inputs := strings.Split(strings.TrimSpace(message), " ")
		index, _ := strconv.Atoi(inputs[5])
		sendMessage(readConns[index], message)
	}
}

// Fucntion to handle message passing asynchronously.
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

// Fucntion to handle running server async.
func runServer(name string, serverReadConn, serverWriteConn net.Conn, sourceQueue []*queueingSystem.Queue, destinationQueue *queueingSystem.Queue, handleBufferOverflow bool) {
	go readFrom(name, serverWriteConn, destinationQueue, handleBufferOverflow)
	go serverWriteTo(name, serverReadConn, sourceQueue)
}

// Function to handle running client async. It will use goroutines for reading of each client and one goroutines for writing to server.
func runClients(name string, readConns, writeConns []net.Conn, sourceQueues []*queueingSystem.Queue, destinationQueue *queueingSystem.Queue, handleBufferOverflow bool) {
	for i, queue := range sourceQueues {
		go readFrom(name+" "+fmt.Sprint(i), writeConns[i], queue, handleBufferOverflow)
	}

	go writeTo(name, readConns, destinationQueue)
}

// Function to get number of clienst from standard input.
func getClientsNumber() int {
	fmt.Print("Enter number of clients: ")
	input, _ := bufio.NewReader(os.Stdin).ReadString('\n')

	result, _ := strconv.Atoi(strings.TrimSpace(input))

	return result
}

// Function to handle multi-way message passing asynchronously. It first initializes server.
// Aks for number of clienst. Asynchronously multi-way message passing can handle multiple clients.
// Initializes each client and its corresponding queue. Then it will run each client and server as a goroutines.
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

// Function to handle multi-way message passing synchronously.
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

// Function to handle multy-way messaging. Multi-way messaging can be handled
// synchronously or asynchronously that is based on message passing mode parameter.
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

// Function to handle server. After receiving a message from client. The message will be edqueued.
// So whenever the queue is not empty this funciton dequeues, and gets a message to send it to server.
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

// Function to handle writing to client. This function waits for a signal to
// check whether client message is sent to server or not. If it is, a signal is passed thorough channel
// an acknowledgment can be sent to client.
func writeToClient(clientReadConn net.Conn, signals chan string) {
	for {
		message := <-signals

		log.Println("LOG:", `send an acknowledgment to the client`)

		sendMessage(clientReadConn, message)

		// log.Println("LOG:", "client received request")
	}
}

// Function to handle reading. It infinitely receive message from a sender.
// If handle buffer over flow is true it will try to handle messages by ignoring new messages for
// 30 seconds so that queue gets less crowded otherwise buffer overflow results in error.
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

// Function to handle client. This function uses two goroutines for reading and writing.
// It means reading and writing will execute concurrently.
func handleCLient(clientReadConn, clientWriteConn net.Conn,
	sourceQueue *queueingSystem.Queue, signals chan string, handleBufferOverflow bool) {
	go readFrom("client", clientWriteConn, sourceQueue, handleBufferOverflow)
	go writeToClient(clientReadConn, signals)
}

// Function to send message to a receiver.
func sendMessage(conn net.Conn, message string) {
	fmt.Fprintf(conn, message+"\n")
}

// Function to receive message from a sender. The message will be enqueued to the corresponding queue.
func receiveMessage(conn net.Conn, q *queueingSystem.Queue) (string, error) {
	readData, err := bufio.NewReader(conn).ReadString('\n')

	handleError(err)

	err = q.Enqueue(string(readData))
	log.Println("LOG:", "enqueued to queue", "SIZE:", q.GetSize())

	return readData, err
}

// Function to handle massage passing synchronously.
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

// Function to create two TCP servers. One for reading, one for writing.
func createTwoWayServer(ReadPort, WritePort string) (net.Conn,
	net.Conn) {

	readConn, err := createTCPserver(ReadPort)

	handleError(err)

	writeConn, err := createTCPserver(WritePort)

	handleError(err)

	return readConn, writeConn
}

// Function to create TCP server. Usually for reading.
func createOneWayServer(ReadPort string) net.Conn {
	readConn, err := createTCPserver(ReadPort)

	handleError(err)

	return readConn
}

// Fucntion to create TCP server and establish connection.
// It first create a listener, after that it listen for any requests from clienst.
// After that it will accept and establish connection.
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

// Function to get two ports. One for reading and one for wrting.
func getPorts(name string) (string, string) {
	fmt.Println("Enter input: <" + name + " reading port> <" + name + " writing port>")
	input, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	inputs := strings.Split(strings.TrimSpace(input), " ")

	return inputs[0], inputs[1]
}

// Function to get one port number that is for reading.
func getPort(name string) string {
	fmt.Println("Enter input: <" + name + " reading port>")
	input, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	inputs := strings.Split(strings.TrimSpace(input), " ")

	return inputs[0]
}

// Function to handle one way messaging. It first initializes server, client and corresponding queue.
// Establishes TCP connections. And handle message passing synchronously or asynchronously based on message passing mode.
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

// Function to handle how program message passing work based on messaging mode that can be one or multi.
// When messaging mode is one that means server only reads from broker.
// when messaging mode is multi that means server reads and writes from and to broker.
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

// Function to get handle buffer overflow that can be true or false.
func getHandleBufferOverflow() bool {
	arguments := os.Args

	result, _ := strconv.ParseBool(arguments[3])
	return result
}

// Function to get messaging passing mode that can be sync or async.
func getMessagePassingMode() string {
	arguments := os.Args

	return arguments[2]
}

// Function to get messaging mode that can be one or multi.
func getMessagingMode() string {
	arguments := os.Args

	return arguments[1]
}

// Function to get command line arguments.
func getCommandLineArguments() (string, string, bool) {
	messagingMode := getMessagingMode()
	messagePassingMode := getMessagePassingMode()
	handleBufferOverflow := getHandleBufferOverflow()
	return messagingMode, messagePassingMode, handleBufferOverflow
}

// Function to handle error.
// If there is an error it will be logged.
func handleError(err error) {
	if err != nil {
		log.Println("ERROR: ", err)
		os.Exit(1)
	}
}

// Function to check number of command line arguments.
// There should be three arguments, for choosing messaging mode,
// message passing mode and whether to handle buffer overflow.
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

func main() {
	err := checkCommandLineArguments()

	handleError(err)

	messagingMode, messagePassingMode, handleBufferOverflow := getCommandLineArguments()

	handleMessagePassing(messagingMode, messagePassingMode, handleBufferOverflow)
}
