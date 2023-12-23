package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

func CreateTable(tableName string, db *pgx.Conn, headers []string) {
	tx, err := db.Begin(context.Background())
	CheckError(err)
	_, err = tx.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	CheckError(err)

	createTable := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s(", tableName,
	)
	for h := range headers {
		headerInsertPart := fmt.Sprintf("%s VARCHAR(255), ", headers[h])
		createTable = createTable + headerInsertPart
	}
	createTable = strings.TrimSuffix(createTable, ", ")
	createTable = createTable + ")"
	_, err = db.Exec(context.Background(), createTable)
	CheckError(err)
	err = tx.Commit(context.Background())
	CheckError(err)
}

func CopyTable(db *pgx.Conn, filepath string, fileName string) {
	tx, err := db.Begin(context.Background())
	CheckError(err)
	copyDataString := fmt.Sprintf("COPY %s FROM '%s' csv header", fileName, filepath)
	_, err = tx.Exec(context.Background(), copyDataString)
	CheckError(err)
	err = tx.Commit(context.Background())
	CheckError(err)

}

func InsertData(db *pgx.Conn, data []string, tableName string, err error) {
	tx, err := db.Begin(context.Background())
	CheckError(err)
	var rowValues []string
	for row := range data {
		rowValues = nil
		row := strings.Split(data[row], ",")
		for cell := range row {
			cell := fmt.Sprintf("'%s'", row[cell])
			rowValues = append(rowValues, cell)
		}
		values := strings.Join(rowValues, ",")
		populateCell := fmt.Sprintf("INSERT INTO %s VALUES(DEFAULT, %s) RETURNING *",
			tableName, values)

		_, err = tx.Exec(context.Background(), populateCell)
		CheckError(err)
	}
	err = tx.Commit(context.Background())
	CheckError(err)
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

	//db, err := sql.Open("postgres", connectString)
	db, err := pgx.Connect(context.Background(), connectString)
	CheckError(err)

	defer db.Close(context.Background())
	err = db.Ping(context.Background())
	CheckError(err)
	fmt.Println("Connnected to database...")

	yearFolders, err := os.ReadDir("./data/")
	CheckError(err)

	for _, yearFolders := range yearFolders {
		if strings.HasPrefix(yearFolders.Name(), ".") {
			continue
		}
		folder := fmt.Sprintf("./data/%s", yearFolders.Name())
		files, err := os.ReadDir(folder)
		CheckError(err)
		for _, files := range files {
			if strings.HasPrefix(files.Name(), ".") {
				continue
			}
			fileDir := fmt.Sprintf("%s/%s", folder, files.Name())
			file, err := os.ReadFile(fileDir)
			CheckError(err)
			absFilepath, err := filepath.Abs(fileDir)
			CheckError(err)
			var fileContent = string(file[:])
			var splitFile = strings.SplitAfterN(fileContent, "\n", 2)
			headers := strings.Split(splitFile[0], ",")
			fileName := strings.Split(files.Name(), ".")

			//data := strings.Split(splitFile[1], "\n")

			CreateTable(fileName[0], db, headers)

			fmt.Println(fileDir)

			CopyTable(db, absFilepath, fileName[0])

			//fmt.Println("Inserting... ;)")
			//InsertData(db, data, fileName[0], err)
			//fmt.Println("Finished... ;)")
		}
	}
}

func CheckError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
