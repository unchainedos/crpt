package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/segmentio/kafka-go"
)

func getJSONRawBody(c echo.Context) map[string]interface{} {

	jsonBody := make(map[string]interface{})
	err := json.NewDecoder(c.Request().Body).Decode(&jsonBody)
	if err != nil {

		log.Fatal("empty json body")
		return nil
	}

	return jsonBody
}

//optimization and autoban here to be written

func quest(c echo.Context) error {
	wrighter := c.Get("writer").(*kafka.Writer)
	body := getJSONRawBody(c)
	str := body["id"].(string) + string("|-&&&-|") + body["name"].(string) + string("|-&&&-|") + body["md"].(string) + string("|-&&&-|") + body["code"].(string) + string("|-&&&-|") + body["elo"].(string)
	err := wrighter.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(str),
	})
	if err != nil {
		log.Print(err)
		return c.String(503, "unable to post your task to moderation")
	}
	return c.String(200, "ok")

}

func WithWrighter(key string, writer *kafka.Writer) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set(key, writer)
			return next(c)
		}
	}
}

func main() {
	e := echo.New()
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "moderated_quests",
		RequiredAcks: -1,                  // Подтверждение от всех реплик
		MaxAttempts:  10,                  //кол-во попыток доставки(по умолчанию всегда 10)
		BatchSize:    100,                 // Ограничение на количество сообщений(по дефолту 100)
		WriteTimeout: 10 * time.Second,    //время ожидания для записи(по умолчанию 10сек)
		Balancer:     &kafka.RoundRobin{}, //балансировщик.
	})
	e.POST("quest", quest, WithWrighter("writer", writer))
}
