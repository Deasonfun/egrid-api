package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func queryDB(query string, db *sql.DB) ([]map[string]interface{}, error) {
	formattedQuery := strings.ReplaceAll(query, "_", " ")

	rows, err := db.Query(formattedQuery)
	CheckError(err)
	defer rows.Close()

	columns, err := rows.Columns()
	CheckError(err)

	var result []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePrts := make([]interface{}, len(columns))

		for i := range columns {
			valuePrts[i] = &values[i]
		}

		if err := rows.Scan(valuePrts...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			val := values[i]
			rowMap[colName] = val
		}

		result = append(result, rowMap)
	}
	return result, nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	user := os.Getenv("USER")
	password := os.Getenv("PASSWORD")
	dbname := os.Getenv("DBNAME")

	var connectionString = fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", host, user, password, dbname, port)
	db, err := sql.Open("postgres", connectionString)
	CheckError(err)
	defer db.Close()

	router := gin.Default()

	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"PUT", "GET"},
		AllowHeaders:     []string{"Origin"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	router.GET("/query/:query", func(c *gin.Context) {
		query := c.Param("query")
		data, err := queryDB(query, db)
		CheckError(err)

		jsonData, err := json.Marshal(data)
		CheckError(err)

		c.Header("Content-Type", "application/json")
		c.String(http.StatusOK, string(jsonData))
	})

	router.Run("localhost:4000")
}

func CheckError(err error) {
	if err != nil {
		fmt.Println(err)
		return
	}
}
