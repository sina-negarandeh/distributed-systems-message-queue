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

func handleRead(port string) {
	conn, _ := createTCPclient(port)

	for {
		receiveMessage(conn)
	}
}

func handleNetError(err error) {
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		fmt.Println("read timeout:", err) // time out
	} else {
		fmt.Println("read error:", err) // some error else, do something else, for example create new conn
	}
}

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

func sendMessage(conn net.Conn, message, name string) {
	time.Sleep(3 * time.Second)
	fmt.Fprintf(conn, "client "+name+" "+message+"\n")
}

func createTCPclient(port string) (net.Conn, error) {
	conn, err := net.Dial("tcp", ":"+port)

	handleError(err)

	return conn, err
}

func handleMessagePassingAsynchronously(readingPort, writingPort, name string) {
	go handleRead(readingPort)

	time.Sleep(1 * time.Second)

	go handleWrite(writingPort, name)

	for {
		time.Sleep(10 * time.Second)
		fmt.Println("doing something ...")
	}
}

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

func getName() string {
	fmt.Print("Enter a name: ")
	name, _ := bufio.NewReader(os.Stdin).ReadString('\n')

	return strings.TrimSpace(name)
}

func getPortNumbers() (string, string, error) {
	arguments := os.Args

	return arguments[2], arguments[3], nil
}

func handleMessagePassing(messagePassingMode string) {
	readingPort, writingPort, err := getPortNumbers()
	name := getName()

	handleError(err)

	switch messagePassingMode {
	case "synchronously":
		handleMessagePassingSynchronously(readingPort, writingPort, name)
	case "asynchronously":
		handleMessagePassingAsynchronously(readingPort, writingPort, name)
	default:
		log.Println("ERROR:", "mode does not exist")
	}
}

func getMessagePassingMode() (string, error) {
	arguments := os.Args

	return arguments[1], nil
}

func handleError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

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
