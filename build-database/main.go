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

			_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", fileName[0]))
			CheckError(err)

			createTable := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s(id SERIAL PRIMARY KEY)", fileName[0],
			)
			_, err = db.Exec(createTable)
			CheckError(err)

			data := strings.Split(splitFile[1], "\n")

			for _, header := range headers {
				addColumn := fmt.Sprintf("ALTER TABLE %s ADD %s VARCHAR(250)",
					fileName[0], strings.TrimSpace(header))
				_, err = db.Exec(addColumn)
				CheckError(err)
			}
			headerValues := strings.Join(headers, ",")
			var rowValues []string
			for _, row := range data {
				rowValues = nil
				row := strings.Split(row, ",")
				for _, cell := range row {
					cell := fmt.Sprintf("'%s'", cell)
					rowValues = append(rowValues, cell)
				}
				values := strings.Join(rowValues, ",")
				populateCell := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s) RETURNING *",
					fileName[0], headerValues, values)

				_, err = db.Exec(populateCell)
				CheckError(err)
			}
		}
	}
}

func CheckError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
