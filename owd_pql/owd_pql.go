package pql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/orthwade/owd_go_logs/owd_logs"

	_ "github.com/lib/pq"
)

func WaitPsql(ctx context.Context, dbHost string, dbPort string) error {
	logger := owd_logs.DefaultFromCtx(ctx)

	address := dbHost + ":" + dbPort

	maxRetries := 20

	timeout := 3 * time.Second

	for i := 0; i < maxRetries; i++ {
		_, err := net.Dial("tcp", address)

		if err == nil {
			logger.Info("PSQL is running")
			return nil
		}

		logger.Warn("Failed connection to PSQL",
			"Attempt", i+1,
			"errro", err)

		if i < maxRetries-1 {
			logger.Warn("Retrying",
				"Timeout", timeout)
			time.Sleep(timeout)
		} else {
			logger.Error("Max retries. Connection Failed")
			return fmt.Errorf("max retries. connection Failed")
		}
	}

	return nil

}

func Connect(ctx context.Context,
	dbHost string,
	dbPort string,
	dbUser string,
	dbPassword string,
	dbName string) *sql.DB {
	logger := owd_logs.DefaultFromCtx(ctx)

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		dbUser,
		dbPassword,
		dbHost,
		dbPort,
		dbName)

	logger.Info("Attempting connection to database",
		"DB name", dbName,
		"User", dbUser,
		"Host", dbHost,
		"Port", dbPort)

	db, err := sql.Open(dbName, connStr)

	if err != nil {
		logger.Error("Error connecting to database", "error", err)
	} else {
		logger.Info("Succesfully connected to database")
	}

	return db
}

func CheckDb(ctx context.Context, postgresDb *sql.DB, dbName string) bool {
	var result bool

	logger := owd_logs.DefaultFromCtx(ctx)

	queryRow := fmt.Sprintf(`SELECT EXISTS (
	SELECT 1 FROM pg_catalog.pg_database WHERE datname = '%s'
	);`, dbName)

	err := postgresDb.QueryRow(queryRow).Scan(&result)

	if err != nil {
		logger.Error(fmt.Sprintf("Error checking if db %s exists", dbName), "fatal", err)
	}

	return result
}

func CheckTable(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	logger := owd_logs.DefaultFromCtx(ctx)

	query := `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_name = $1
		);
	`
	var exists bool
	err := db.QueryRowContext(ctx, query, tableName).Scan(&exists)
	if err != nil {
		logger.Error("Failed to check if table exists", "error", err)
		return false, err
	}

	return exists, nil
}

func CreateTable(ctx context.Context, db *sql.DB, tableName string,
	columns string, indexName string, indexColumn string) error {
	logger := owd_logs.DefaultFromCtx(ctx)

	// Create table query
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s
		);
	`, tableName, columns)

	_, err := db.ExecContext(ctx, createTableQuery)
	if err != nil {
		logger.Error("Failed to create table", "error", err)
		return err
	}

	// Create index query if index info is provided
	if indexName != "" && indexColumn != "" {
		createIndexQuery := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (%s);`,
			indexName,
			tableName,
			indexColumn)

		_, err = db.ExecContext(ctx, createIndexQuery)

		if err != nil {
			logger.Error(fmt.Sprintf("Failed to create index on %s", indexColumn), "error", err)
			return err
		}
	}

	logger.Info(fmt.Sprintf("Table '%s' and index '%s' created successfully",
		tableName,
		indexName))

	return nil
}

func PrintTableColumnsNamesAndTypes(
	ctx context.Context, db *sql.DB, tableName string) {

	logger := owd_logs.DefaultFromCtx(ctx)

	// Query to get table structure
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = $1;
	`

	// Execute the query
	rows, err := db.Query(query, tableName)
	if err != nil {
		logger.Error("Query failed", "error", err)
	}
	defer rows.Close()

	// Print the table structure
	fmt.Printf("Structure of %s table:\n", tableName)
	fmt.Printf("%-20s %-15s %-10s\n", "Column Name", "Data Type", "Is Nullable")
	fmt.Println(strings.Repeat("-", 50))

	for rows.Next() {
		var columnName, dataType, isNullable string
		if err := rows.Scan(&columnName, &dataType, &isNullable); err != nil {
			logger.Error("Failed to scan row", "error", err)
		}
		fmt.Printf("%-20s %-15s %-10s\n", columnName, dataType, isNullable)
	}

	// Check for errors after iterating through rows
	if err := rows.Err(); err != nil {
		logger.Error("Error iterating rows", "error", err)
	}
}

func AddRecord(ctx context.Context, db *sql.DB,
	tableName string,
	columns []string,
	values []interface{},
	conflictColumn string) error {
	logger := owd_logs.DefaultFromCtx(ctx)

	// Construct the column and value placeholders for the query
	columnsStr := "(" + strings.Join(columns, ", ") + ")"
	placeholders := "(" + strings.Repeat("$", len(values)-1) + "$" + fmt.Sprint(len(values)) + ")"

	// Construct the full SQL query with ON CONFLICT clause
	query := fmt.Sprintf(`
		INSERT INTO %s %s
		VALUES %s
		ON CONFLICT (%s) DO NOTHING;
	`, tableName, columnsStr, placeholders, conflictColumn)

	// Execute the query with the provided values
	_, err := db.ExecContext(ctx, query, values...)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to add record to %s", tableName), "error", err)
		return err
	}

	logger.Info(fmt.Sprintf("Record added successfully to %s", tableName))
	return nil
}

func FindRecord(ctx context.Context, db *sql.DB, tableName string,
	idFieldName string, idValue int, dest interface{}) {
	logger := owd_logs.DefaultFromCtx(ctx)

	query := fmt.Sprintf(`
		SELECT * FROM %s WHERE %s = $1;
	`, tableName, idFieldName)

	// Execute the query and scan the result into the destination struct
	err := db.QueryRowContext(ctx, query, idValue).Scan(dest)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Warn(fmt.Sprintf("No record found in %s with %s = %v",
				tableName, idFieldName, idValue))

			dest = nil

			return
		}
		logger.Error(fmt.Sprintf("Failed to retrieve record from %s",
			tableName), "error", err, idFieldName, idValue)

		return
	}

	logger.Info(fmt.Sprintf("Record from %s with %s = %v retrieved successfully",
		tableName,
		idFieldName,
		idValue))
}

func RemoveRecord(ctx context.Context, db *sql.DB, tableName string, id int) error {
	logger := owd_logs.DefaultFromCtx(ctx)

	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE id = $1;
	`, tableName)

	_, err := db.ExecContext(ctx, query, id)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to delete from %s", tableName), "error", err, "ID", id)
	} else {
		logger.Info(fmt.Sprintf("Deleted from %s", tableName), "ID", id)
	}

	return err
}

func SetField(ctx context.Context, db *sql.DB, tableName, keyColumn string,
	keyValue interface{}, fieldColumn string, fieldValue interface{}) error {

	logger := owd_logs.DefaultFromCtx(ctx)

	query := fmt.Sprintf(`
        UPDATE %s
        SET %s = $1
        WHERE %s = $2;
    `, tableName, fieldColumn, keyColumn)

	logger.Info("Executing query", "query", query)

	_, err := db.ExecContext(ctx, query, fieldValue, keyValue)

	if err != nil {
		return err
	}
	return nil
}
