package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
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

func testSolution(c echo.Context) error {
	body := getJSONRawBody(c)
	id := body["id"].(string)
	code := body["code"].(string)
	os.Chdir(id)
	timestring := time.Now().String()
	s := sha1.New()
	r := s.Sum([]byte(timestring))
	res := hex.EncodeToString(r)
	name := "last_solution" + res + ".py"
	os.WriteFile(name, []byte(code), 0755)
	cmd := exec.Command("python", name)
	out, err := cmd.Output()
	fmt.Print(out)
	if err != nil {
		os.Remove(name)
		return c.String(200, string(out))
	}
	os.Remove(name)
	return c.String(200, string(out))
}

func addNewQuest() {
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Print(err)
		}
		parsed_message := strings.Split(string(msg.Value), "|-&&&-|")
		err = os.Mkdir(parsed_message[0], 0755)
		if err != nil {
			log.Print(err)
		}
		os.Chdir(parsed_message[0])
		os.WriteFile("quest.txt", []byte(parsed_message[1]), 0755)
		os.Chdir("..")
	}
}

var (
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "moderated_quests",
		GroupID: "worker",
	})
)

func main() {
	// go( --> test solution )
	e := echo.New()
	e.POST("/checkSolution", testSolution)
	go addNewQuest()
	e.Start(":3423")
}
