package main

import (
	"context"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"platzi.com/go/grpc/testpb"
)

func main() {
	cc, err := grpc.Dial("localhost:5070", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	defer cc.Close()

	c := testpb.NewTestServiceClient(cc)
	//DoUnary(c)
	//DoClienStreaming(c)
	//DoServerStreaming(c)
	DoBidirectionalStreaming(c)
}

func DoUnary(c testpb.TestServiceClient) {
	req := &testpb.GetTestRequest{
		Id: "t1",
	}

	res, err := c.GetTest(context.Background(), req)

	if err != nil {
		log.Fatalf("Error while calling GetTest: %v\n", err)
	}
	log.Printf("Response from GetTest: %v\n", res)
}

func DoClienStreaming(c testpb.TestServiceClient) {
	questions := []*testpb.Question{
		{
			Id:       "q8t1",
			Answer:   "Azul",
			Question: "Color asociado a Golang",
			TestId:   "t1",
		}, {
			Id:       "q9t1",
			Answer:   "Google",
			Question: "Empresa que desarrolla el lenguaje  Golang",
			TestId:   "t1",
		}, {
			Id:       "q10t1",
			Answer:   "Backend",
			Question: "Especialidad de Golang",
			TestId:   "t1",
		},
	}

	stream, err := c.SetQuestions(context.Background())
	if err != nil {
		log.Fatalf("Error while calling SetQuestions: %v\n", err)
	}
	for _, question := range questions {
		log.Println("Sending question: ", question.Id)
		stream.Send(question)
		time.Sleep(2 * time.Second)
	}

	msg, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error while receiving response: %v\n", err)
	}
	log.Printf("Response from SetQuestions: %v\n", msg)
}

func DoServerStreaming(c testpb.TestServiceClient) {
	req := &testpb.GetStudentsPerTestRequest{
		TestId: "t1",
	}
	stream, err := c.GetStudentsPerTest(context.Background(), req)
	if err != nil {
		log.Fatalf("Error while calling GetStudentsPerTest: %v\n", err)
	}

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error while receiving response: %v\n", err)
		}
		log.Printf("Response from GetStudentsPerTest: %v\n", msg)
	}
}

func DoBidirectionalStreaming(c testpb.TestServiceClient) {
	answer := testpb.TakeTestRequest{
		Answer: "42",
	}
	numberOfQuestions := 6

	waitChannel := make(chan struct{})

	stream, err := c.TakeTest(context.Background())
	if err != nil {
		log.Fatalf("Error while calling TakeTest: %v\n", err)
	}

	go func() {
		for i := 0; i < numberOfQuestions; i++ {
			stream.Send(&answer)
			time.Sleep(1 * time.Second)
		}
		stream.CloseSend()
	}()

	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error while receiving data from TakeTest: %v\n", err)
				break
			}
			log.Printf("Response from TakeTest: %v\n", res)
		}
		close(waitChannel)
	}()
	<-waitChannel

}
