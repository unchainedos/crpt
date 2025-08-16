package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"

	"github.com/labstack/echo/v4"
)

var (
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "storage",
		GroupID: "storage",
	})
)

func main() {
	e := echo.New()
	e.GET("/md", getMdHandler)
	go addNewMd()
	e.Start(":3424")
}
func getMdHandler(c echo.Context) error {
	id := c.Param("id")

	fileName := fmt.Sprintf("%s.md", id)

	mdContent, err := os.ReadFile(fileName)
	if err != nil {
		log.Print(err)
		return err
	}

	return c.String(200, string(mdContent))

}

func addNewMd() {
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Print(err)
		}

		parsed_message := strings.Split(string(msg.Value), "|-&&&-|")
		id := parsed_message[0]
		mdContent := parsed_message[1]

		fileName := fmt.Sprintf("%s.md", id)
		os.WriteFile(fileName, []byte(mdContent), 0755)

	}
}
