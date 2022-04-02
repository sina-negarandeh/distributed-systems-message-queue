package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func write(conn net.Conn, text string) {
	fmt.Print(">> processing " + text)
	// fmt.Fprintf(conn, "processing "+text)
}

func read(conn net.Conn) string {
	message, _ := bufio.NewReader(conn).ReadString('\n')
	return message
}

func main() {
	arguments := os.Args

	if len(arguments) == 1 {
		fmt.Println("Please provide a socket address host:port.")
		return
	}

	PORT := arguments[1]
	conn, err := net.Dial("tcp", ":"+PORT)

	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		input := read(conn)

		if strings.TrimSpace(input) == "STOP" {
			fmt.Println("exiting ...")
			return
		}

		write(conn, input)
	}
}
