package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
	"github.com/segmentio/kafka-go"
)

type Quest struct {
	Id       int    `json:"id"`
	Name     string `json:"name"`
	Elo      int    `json:"elo"`
	Markdown string `json:"markdown"`
	Code     string `json:"code"`
}

func getJSONRawBody(c echo.Context) map[string]interface{} {

	jsonBody := make(map[string]interface{})
	err := json.NewDecoder(c.Request().Body).Decode(&jsonBody)
	if err != nil {

		log.Fatal("empty json body")
		return nil
	}

	return jsonBody
}

func quit(c echo.Context) error {
	cookie := new(http.Cookie)
	cookie.Name = "token"
	cookie.Value = ""
	cookie.Path = "/moderator"
	c.SetCookie(cookie)
	return c.String(200, "good bye")
}

func auth(c echo.Context) error {
	pool := c.Get("db").(*pgxpool.Pool)
	body := getJSONRawBody(c)
	key := []byte(envFile["JWT_KEY"])

	hasher := sha256.New()
	l := []byte(body["password"].(string))
	hasher.Write(l)
	hsh := hasher.Sum(nil)
	body["password"] = hex.EncodeToString(hsh)

	var result int
	err := pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM moderators WHERE password=$1 AND username=$2", body["password"], body["username"]).Scan(&result)
	if err != nil {
		log.Print(err)
		return c.String(503, "error happened during work with db")
	}
	if result == 0 {
		return c.String(401, "Invalid Login or Password")
	}

	t := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": body["username"],
	})
	s, _ := t.SignedString(key)

	cookie := new(http.Cookie)
	cookie.Name = "token"
	cookie.Value = s
	cookie.Expires = time.Now().Add(30 * 24 * time.Hour)
	c.SetCookie(cookie)
	return c.String(200, "you have been authed")
}

func AuthMidleWare(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		token, err := c.Cookie("token")
		if err != nil {
			log.Printf("No token cookie: %v", err)
			return c.String(403, "forbidden")
		}

		isValid, err := verifyToken(token.Value)
		if err != nil {
			log.Printf("Token validation error: %v", err)
			return c.String(403, "forbidden")
		}
		if !isValid {
			log.Printf("Invalid token: %s", token.Value)
			return c.String(403, "forbidden")
		}

		return next(c)
	}
}

func verifyToken(tokenString string) (bool, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Проверяем, что алгоритм подписи HMAC (HS256/HS384/HS512)
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(envFile["JWT_KEY"]), nil
	})

	if err != nil {
		return false, err
	}

	return token.Valid, nil
}

func newtask(c echo.Context) error {
	reader := c.Get("reader").(*kafka.Reader)
	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Print(err)
		return c.String(503, "service is currently unavaible")
	}
	parsed_message := strings.Split(string(msg.Value), "|-&&&-|")
	id, _ := strconv.Atoi(parsed_message[0])
	elo, _ := strconv.Atoi(parsed_message[4])
	response := Quest{
		Id:       id,
		Name:     parsed_message[1],
		Markdown: parsed_message[2],
		Code:     parsed_message[3],
		Elo:      elo,
	}
	return c.JSON(200, response)
}

func posttask(c echo.Context) error {
	pool := c.Get("db").(*pgxpool.Pool)
	writermd := c.Get("mdfiles").(*kafka.Writer)
	writercode := c.Get("quests").(*kafka.Writer)
	body := getJSONRawBody(c)

	name := body["name"].(string)
	elo := body["elo"].(string)
	code := body["code"].(string)
	md := body["md"].(string)
	var id int

	err := pool.QueryRow(context.Background(), "INSERT INTO quests (name,elo) VALUES($1,$2) RETURNING id", name, elo).Scan(&id)
	if err != nil {
		return c.String(503, "error during inserting your quest in db")
	}

	writermd.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(strconv.Itoa(id) + "|-&&&-|" + md),
	})
	writercode.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(strconv.Itoa(id) + "|-&&&-|" + code),
	})

	return c.String(200, "ok")
}

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

func WithReader(reader *kafka.Reader) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("reader", reader)
			return next(c)
		}
	}
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
	pool, err := pgxpool.New(context.Background(), envFile["DB_UL"])
	if err != nil {
		log.Print(err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "fresh_quests",
		GroupID: "moderators",
	})
	defer reader.Close()

	writerMdFiles := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "quests_md_files",
		RequiredAcks: -1,                  // Подтверждение от всех реплик
		MaxAttempts:  10,                  //кол-во попыток доставки(по умолчанию всегда 10)
		BatchSize:    100,                 // Ограничение на количество сообщений(по дефолту 100)
		WriteTimeout: 10 * time.Second,    //время ожидания для записи(по умолчанию 10сек)
		Balancer:     &kafka.RoundRobin{}, //балансировщик.
	})
	defer writerMdFiles.Close()
	writerModeratedQuests := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{"localhost:9092"},
		Topic:        "moderated_quests",
		RequiredAcks: -1,                  // Подтверждение от всех реплик
		MaxAttempts:  10,                  //кол-во попыток доставки(по умолчанию всегда 10)
		BatchSize:    100,                 // Ограничение на количество сообщений(по дефолту 100)
		WriteTimeout: 10 * time.Second,    //время ожидания для записи(по умолчанию 10сек)
		Balancer:     &kafka.RoundRobin{}, //балансировщик.
	})
	defer writerModeratedQuests.Close()
	// err = writer.WriteMessages(context.Background(), kafka.Message{
	// 	Value: []byte("1|-&&&-|odjcaomscd|-&&&-|ckasdckasdcknasdc[oknasdcjno]"),
	// })
	// if err != nil {
	// 	log.Print(err)
	// }
	e := echo.New()
	// auth(jwt)
	e.POST("/moderator/auth", auth, WithDB(pool))
	// get : ->
	e.GET("/moderator/newtask", newtask, WithReader(reader), AuthMidleWare)
	// --> sending new quets --> moderators
	e.POST("/moderator/posttask", posttask, WithWrighter("mdfiles", writerMdFiles), WithWrighter("quests", writerModeratedQuests), WithDB(pool), AuthMidleWare)
	// agreed quest --> storage
	//  |
	//  V
	// workers
	// disagree
	// e.POST("/moderator/dismis", dismis)
	//quit account
	e.POST("/moderator/quit", quit)
	e.Start(":4321")
}
