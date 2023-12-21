package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func CreateTable(tableName string, db *sql.DB, headers []string) {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	CheckError(err)

	createTable := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s(id SERIAL PRIMARY KEY, ", tableName,
	)
	for _, header := range headers {
		headerInsertPart := fmt.Sprintf("%s VARCHAR(255), ", header)
		createTable = createTable + headerInsertPart
	}
	createTable = strings.TrimSuffix(createTable, ", ")
	createTable = createTable + ")"
	_, err = db.Exec(createTable)
	CheckError(err)
}

func InsertData(db *sql.DB, data []string, tableName string, err error) {
	//println("%s", data)
	var rowValues []string
	for _, row := range data {
		rowValues = nil
		row := strings.Split(row, ",")
		for _, cell := range row {
			cell := fmt.Sprintf("'%s'", cell)
			rowValues = append(rowValues, cell)
		}
		values := strings.Join(rowValues, ",")
		populateCell := fmt.Sprintf("INSERT INTO %s VALUES(DEFAULT, %s) RETURNING *",
			tableName, values)

		_, err = db.Exec(populateCell)
		CheckError(err)
	}
}

func main() {

	err := godotenv.Load("../.env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	user := os.Getenv("USER")
	password := os.Getenv("PASSWORD")
	dbname := os.Getenv("DBNAME")

	connectString := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", connectString)

	CheckError(err)

	defer db.Close()
	err = db.Ping()
	CheckError(err)
	fmt.Println("Connnected to database...")

	yearFolders, err := os.ReadDir("./data/")
	CheckError(err)

	for _, yearFolders := range yearFolders {
		if strings.HasPrefix(yearFolders.Name(), ".") {
			continue
		}
		folder := fmt.Sprintf("./data/%s", yearFolders.Name())
		fmt.Printf("%s\n", folder)
		files, err := os.ReadDir(folder)
		CheckError(err)
		for _, files := range files {
			if strings.HasPrefix(files.Name(), ".") {
				continue
			}
			fileDir := fmt.Sprintf("%s/%s", folder, files.Name())
			file, err := os.ReadFile(fileDir)
			CheckError(err)
			var fileContent = string(file[:])
			var splitFile = strings.SplitAfterN(fileContent, "\n", 2)
			headers := strings.Split(splitFile[0], ",")
			fileName := strings.Split(files.Name(), ".")
			fmt.Println(fileName[0])

			data := strings.Split(splitFile[1], "\n")

			CreateTable(fileName[0], db, headers)

			InsertData(db, data, fileName[0], err)

		}
	}
}

func CheckError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
