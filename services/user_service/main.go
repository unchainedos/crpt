package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/veyselaksin/gomailer/pkg/mailer"
)

type User struct {
	Id             int    `json:"id"`
	Username       string `json:"username"`
	Rating         int    `json:"rating"`
	ProblemsSolved int    `json:"problems_solved"`
	ProblemsPushed int    `json:"problens_pushed"`
	Email          string `json:"email"`
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

func create(c echo.Context) error {
	pool := c.Get("db").(*pgxpool.Pool)

	x := getJSONRawBody(c)

	hasher := sha256.New()
	l := []byte(x["password"].(string))
	hasher.Write(l)
	hsh := hasher.Sum(nil)
	x["password"] = hex.EncodeToString(hsh)

	var id int
	err := pool.QueryRow(context.Background(), "INSERT INTO users (username,email,password) VALUES ($1,$2,$3) RETURNING id", x["username"], x["email"], x["password"]).Scan(&id)
	fmt.Print(id)
	if err != nil {
		log.Fatal("unable to write user to db", err)
		return c.String(503, "unable to write user to db")
	}

	auth := mailer.Authentication{
		Username: envFile["EMAIL"],
		Password: envFile["PASSWORD"],
		Host:     "smtp.mail.ru",
		Port:     "587",
	}
	sender := mailer.NewPlainAuth(&auth)

	message := mailer.NewMessage("Hello World", fmt.Sprintf("<a href='http://localhost:1234/user/activate?id=%d'>activate your account</a>", id))
	message.SetTo([]string{"unchainedos@mail.ru"})

	sender.SendMail(message)

	return c.String(200, "1234")
}

func activate(c echo.Context) error {
	pool := c.Get("db").(*pgxpool.Pool)
	id := c.QueryParam("id")
	_, err := pool.Exec(context.Background(), "update users set is_active=true where id=$1", id)
	if err != nil {
		log.Fatal("error happened during activating account")
		return c.String(503, "error happened during activating account")
	}
	return c.String(200, "congrats and welcome to our community")

}

func read(c echo.Context) error {
	pool := c.Get("db").(*pgxpool.Pool)
	id := c.Param("id")
	real_id, _ := strconv.Atoi(id)
	user := User{}
	user.Id = real_id
	err := pool.QueryRow(context.Background(), "select username,rating,problems_solved,problems_pushed,email from users where id = $1", id).Scan(&user.Username, &user.Rating, &user.ProblemsSolved, &user.ProblemsPushed, &user.Email)
	if err != nil {
		log.Fatal("err during fetching user")
		return c.String(503, "err during fetching user")
	}
	return c.JSON(200, user)
}

func delete(c echo.Context) error {
	pool := c.Get("db").(*pgxpool.Pool)
	id := c.QueryParam("id")
	_, err := pool.Exec(context.Background(), "delete from users where id = $1", id)
	if err != nil {
		log.Fatal("error happened during deleting account")
		return c.String(503, "error happened during deleting account")
	}
	return c.String(200, "congrats and welcome to our community")
}

func auth(c echo.Context) error {
	return c.String(200, "adsfasfd")
}

func WithDB(db *pgxpool.Pool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("db", db)
			return next(c)
		}
	}
}

var (
	envFile, _ = godotenv.Read(".env")
)

func main() {

	pool, err := pgxpool.New(context.Background(), envFile["DB_UL"])
	if err != nil {
		log.Fatal("unable to connect to db")
	}
	err = pool.Ping(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	e := echo.New()
	e.Use(WithDB(pool))
	// create user : POST
	e.POST("/user", create)
	// activate : PATCH
	e.GET("/user/activate", activate)
	// update any data about user : -- many handlers so i will do it later --
	// delete user : DELETE
	e.DELETE("/user", delete)
	// auth(JWT) :POST
	e.POST("/user/auth", auth)
	// read user : GET
	e.GET("/user/:id", read)

	e.Start(":1234")
}
