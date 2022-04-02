package queue

import (
	"errors"
)

// A structure to represent a queue
type Queue struct {
	front, rear, size int
	capacity          int
	array             []string // circular array
}

// function to create a queue of given capacity.
// It initializes size of queue as 0
func CreateQueue(capacity int) *Queue {
	array := make([]string, capacity)
	q := Queue{front: 0, rear: capacity - 1, size: 0, capacity: capacity, array: array}
	return &q
}

// Queue is full when size becomes equal to the capacity
func (q *Queue) IsFull() bool {
	return (q.size == q.capacity)
}

// Queue is empty when size is 0
func (q *Queue) IsEmpty() bool {
	return (q.size == 0)
}

// Function to add an item to the queue.
// It changes rear and size
func (q *Queue) Enqueue(item string) error {
	if q.IsFull() {
		return errors.New("queue is full")
	}
	q.rear = (q.rear + 1) % q.capacity
	q.array[q.rear] = item
	q.size = q.size + 1
	// fmt.Printf("Queueing System LOG: enqueued to queue - value: %s", item)
	return nil
}

// Function to remove an item from queue.
// It changes front and size
func (q *Queue) Dequeue() (string, error) {
	if q.IsEmpty() {
		return "", errors.New("queue is empty")
	}
	item := q.array[q.front]
	q.front = (q.front + 1) % q.capacity
	q.size = q.size - 1
	return item, nil
}

// Function to get front of queue
func (q *Queue) GetFront() (string, error) {
	if q.IsEmpty() {
		return "", errors.New("queue is empty")
	}
	return q.array[q.front], nil
}

// Function to get rear of queue
func (q *Queue) GetRear() (string, error) {
	if q.IsEmpty() {
		return "", errors.New("queue is empty")
	}
	return q.array[q.rear], nil
}

// Function to get size of queue
func (q *Queue) GetSize() int {
	return q.size
}

// Driver program to test above functions./
// func main() {
// 	q := createQueue(4)

// 	q.enqueue("a")
// 	q.enqueue("b")
// 	q.enqueue("c")
// 	q.enqueue("d")

// 	element, _ := q.dequeue()
// 	fmt.Printf("%s dequeued from queue\n\n", element)

// 	front, _ := q.getFront()
// 	rear, _ := q.getRear()
// 	fmt.Printf("Front item is %s\n", front)
// 	fmt.Printf("Rear item is %s\n", rear)
// }
