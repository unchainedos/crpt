package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
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
	db := c.Get("db").(*pgxpool.Pool)

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
	if err != nil {
		os.Remove(name)
		return c.String(200, string(out))
	}
	os.Remove(name)

	_, err = db.Exec(context.Background(), "update quests set tries = tries + 1 where id = $1", id)
	if err != nil {
		log.Print("err during updating tries")
		return c.String(503, "unable to update tries at db")
	}

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

var (
	envFile, _ = godotenv.Read(".env")
)

func WithDB(db *pgxpool.Pool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("db", db)
			return next(c)
		}
	}
}

func main() {
	pool, err := pgxpool.New(context.Background(), envFile["DB_UL"])
	if err != nil {
		log.Print("unable to start a service")
	}
	// go( --> test solution )
	e := echo.New()
	e.POST("/checkSolution", testSolution, WithDB(pool))
	go addNewQuest()
	e.Start(":3423")
}
