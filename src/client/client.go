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

// Function to handle client writing. It first creates a TCP client and establishes a connection.
// It tryes to write message to broekr (TCP server).
func handleWrite(port, name string) {
	conn, _ := createTCPclient(port)

	messageNumber := 0
	for {
		message := "request " + fmt.Sprint(messageNumber)
		sendMessage(conn, message, name)
		println(">> " + message)
		messageNumber++
	}
}

// Function to handle client reading. It first creates a TCP client and establishes a connection.
// Then starts receiving messages from broekr (TCP server).
func handleRead(port string) {
	conn, _ := createTCPclient(port)

	for {
		receiveMessage(conn)
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
func sendMessage(conn net.Conn, message, name string) {
	time.Sleep(3 * time.Second)
	fmt.Fprintf(conn, "client "+name+" "+message+"\n")
}

// Fucntion to create TCP client and establish connection.
func createTCPclient(port string) (net.Conn, error) {
	conn, err := net.Dial("tcp", ":"+port)

	handleError(err)

	return conn, err
}

// Function to handle message passing asynchronously.
func handleMessagePassingAsynchronously(readingPort, writingPort, name string) {
	go handleRead(readingPort)

	time.Sleep(1 * time.Second)

	go handleWrite(writingPort, name)

	for {
		time.Sleep(10 * time.Second)
		fmt.Println("doing something ...")
	}
}

// Function to handle message passing synchronously.
func handleMessagePassingSynchronously(readingPort, writingPort, name string) {
	clientReadConn, _ := createTCPclient(readingPort)
	time.Sleep(1 * time.Second)
	clientWriteConn, _ := createTCPclient(writingPort)

	messageNumber := 0
	for {
		message := "request " + fmt.Sprint(messageNumber)
		sendMessage(clientWriteConn, message, name)
		println(">> " + message)
		messageNumber++

		receiveMessage(clientReadConn)
	}
}

// Function to get a custom name from standard input.
func getName() string {
	fmt.Print("Enter a name: ")
	name, _ := bufio.NewReader(os.Stdin).ReadString('\n')

	return strings.TrimSpace(name)
}

// Function to get client's reading and writing port numbers.
func getPortNumbers() (string, string, error) {
	arguments := os.Args

	return arguments[2], arguments[3], nil
}

// Function to handle how program message passing work based on messaging passing mode that can be sync or async.
func handleMessagePassing(messagePassingMode string) {
	readingPort, writingPort, err := getPortNumbers()
	name := getName()

	handleError(err)

	switch messagePassingMode {
	case "sync":
		handleMessagePassingSynchronously(readingPort, writingPort, name)
	case "async":
		handleMessagePassingAsynchronously(readingPort, writingPort, name)
	default:
		log.Println("ERROR:", "mode does not exist")
	}
}

// Function to get messaging passing mode that can be sync or async.
func getMessagePassingMode() (string, error) {
	arguments := os.Args

	return arguments[1], nil
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

	if len(arguments) < 4 {
		return errors.New(`error: too few arguments. please provide port
		 numbers for reading and writing`)
	} else if len(arguments) > 4 {
		fmt.Println()
		return errors.New(`error: too many arguments. please provide 
		port numbers for reading and writing`)
	}

	return nil
}

func main() {
	err := checkCommandLineArguments()

	handleError(err)

	messagePassingMode, err := getMessagePassingMode()

	handleError(err)

	handleMessagePassing(messagePassingMode)
}
