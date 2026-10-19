package db

import (
	"database/sql"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Store struct {
	db *gorm.DB
}

func New(conn *sql.DB) (*Store, error) {
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: conn}), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}
