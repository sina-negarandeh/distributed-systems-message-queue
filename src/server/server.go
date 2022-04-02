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
)

// Function to pring a text on standard output.
func write(text string) {
	fmt.Print(">> processing " + text)
	// fmt.Fprintf(conn, "processing "+text)
}

// Function to handle server writing. It first creates a TCP client and establishes a connection.
// It tryes to write message to broekr (TCP server).
func handleWrite(port string, messages chan string) {
	conn, _ := createTCPclient(port)

	messageNumber := 0
	for {
		// a select can be used to make Non-Blocking Channel Operations
		receivedMessage := <-messages
		message := "response " + fmt.Sprint(messageNumber) + " to " + receivedMessage
		sendMessage(conn, message)
		fmt.Print(">> " + message)
		messageNumber++
	}
}

// Function to handle server reading. It first creates a TCP client and establishes a connection.
// Then starts receiving messages from broekr (TCP server).
func handleRead(port string, messages chan string) {
	conn, _ := createTCPclient(port)

	for {
		message, _ := receiveMessage(conn)
		messages <- message
	}
}

// Function to handle network errors.
func handleNetError(err error) {
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		fmt.Println("read timeout:", err) // time out
	} else {
		fmt.Println("read error:", err) // some error else, do something else, for example create new conn
	}
}

// Function to receive a message from a server with given connection.
func receiveMessage(conn net.Conn) (string, error) {
	// set SetReadDeadline
	err := conn.SetReadDeadline(time.Now().Add(50 * time.Second))
	if err != nil {
		log.Println("ERROR:", "SetReadDeadline failed:", err)
		// do something else, for example create new conn
	}

	// recvBuf := make([]byte, 1024)
	// _, err = conn.Read(recvBuf[:]) // recv data

	message, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		handleNetError(err)
	} else {
		fmt.Print("-> " + string(message))
	}

	return message, err
}

// Function to send a message to a server with given message and connection.
func sendMessage(conn net.Conn, message string) {
	time.Sleep(3 * time.Second)
	fmt.Fprintf(conn, "server "+message+"\n")
}

// Fucntion to create TCP client and establish connection.
func createTCPclient(port string) (net.Conn, error) {
	conn, err := net.Dial("tcp", ":"+port)

	handleError(err)

	return conn, err
}

// Function to handle massage passing asynchronously.
func handleMessagePassingAsynchronously(readingPort, writingPort string) {
	messages := make(chan string, 10)

	go handleRead(readingPort, messages)

	time.Sleep(1 * time.Second)

	go handleWrite(writingPort, messages)

	for {
		time.Sleep(10 * time.Second)
		fmt.Println("doing something ...")
	}
}

// Function to handle massage passing synchronously.
func handleMessagePassingSynchronously(readingPort, writingPort string) {
	fmt.Println(readingPort, writingPort)

	serverReadConn, _ := createTCPclient(readingPort)
	time.Sleep(1 * time.Second)
	serverWriteConn, _ := createTCPclient(writingPort)

	messageNumber := 0
	for {
		receivedMessage, _ := receiveMessage(serverReadConn)

		message := "response " + fmt.Sprint(messageNumber) + "to " + receivedMessage
		sendMessage(serverWriteConn, message)
		fmt.Print(">> " + message)
		messageNumber++
	}
}

// Function to get two ports. One for reading and one for wrting.
func getPorts(name string) (string, string) {
	fmt.Println("Enter input: <" + name + " reading port> <" + name + " writing port>")
	input, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	inputs := strings.Split(strings.TrimSpace(input), " ")

	return inputs[0], inputs[1]
}

// Function to handle how server message passing works when messaging mode is multi
func handleMultiWayMessaging() {
	messagePassingMode := getMessagePassingMode()
	readingPort, writingPort := getPorts("server")

	switch messagePassingMode {
	case "sync":
		handleMessagePassingSynchronously(readingPort, writingPort)
	case "async":
		handleMessagePassingAsynchronously(readingPort, writingPort)
	default:
		log.Println("ERROR:", "mode does not exist")
	}
}

// Function to get one port number that is for reading.
func getPort(name string) string {
	fmt.Println("Enter input: <" + name + " reading port>")
	input, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	inputs := strings.Split(strings.TrimSpace(input), " ")

	return inputs[0]
}

// Function to handle how server message passing works when messaging mode is one
func handleOneWayMessaging() {
	readingPort := getPort("server")

	serverReadConn, _ := createTCPclient(readingPort)
	for {
		receivedMessage, _ := receiveMessage(serverReadConn)

		write(receivedMessage)
	}

}

// Function to handle how server message passing works based on messaging mode that can be one or multi.
// When messaging mode is one that means server only reads from broker.
// when messaging mode is multi that means server reads and writes from and to broker.
func handleMessagePassing(messagingMode string) {
	switch messagingMode {
	case "one":
		handleOneWayMessaging()
	case "multi":
		handleMultiWayMessaging()
	default:
		log.Println("ERROR:", "mode does not exist")
	}
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
func getCommandLineArguments() string {
	getMessagingMode := getMessagingMode()
	return getMessagingMode
}

// Function to handle error.
// If there is an error it will be logged.
func handleError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Function to check number of command line arguments.
func checkCommandLineArguments() error {
	arguments := os.Args

	if len(arguments) < 3 {
		return errors.New(`error: too few arguments. please provide <MessagingMode> <MessagePassingMode>`)
	} else if len(arguments) > 3 {
		fmt.Println()
		return errors.New(`error: too many arguments. please provide <MessagingMode> <MessagePassingMode>`)
	}

	return nil
}

func main() {
	err := checkCommandLineArguments()

	handleError(err)

	messagingMode := getCommandLineArguments()

	handleMessagePassing(messagingMode)
}
