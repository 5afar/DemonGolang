package postgres

import (
	"context"
	"fmt"

	"Demon.com/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Пул соединений Postgresql
var Dbpool *pgxpool.Pool

// Инициализация пула соединений
func StartDatabaseConnection(cfg *config.Config) {
	var err error
	strconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s pool_max_conns=2", cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPass, cfg.PGBase)
	Dbpool, err = pgxpool.New(context.Background(), strconn)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка подключения")
		panic("Не удалось подключиться к PostgreSQL!")
	}

	if Dbpool == nil {
		config.Log.Error("Не удалось создать пул подключений, проверьте правилность данных для авторизации!")
		panic("Не удалось создать пул соединений PostgreSQL!")
	}
	config.Log.Info("Пул PostgreSQL создан успешно!")

}
