package clickhouse_conn

import (
	"context"
	"os"
	"time"

	"Demon.com/config"
)

// false - дневной срез
// true - ночной срез
var prevTime bool

// Инициализация переменной prevTime
func TimeInit() {
	currentTime := time.Now()
	hour := currentTime.Hour()

	if hour >= 0 && hour < 12 {
		prevTime = true
	} else {
		prevTime = false
	}
	config.Log.Info("Время инициализировано!")
}

// Проверка времени и изменение БД в зависимости от среза
func CheckTime(cfg *config.Config) error {
	config.Log.Info("Проверка времени...")

	currentTime := time.Now()
	hour := currentTime.Hour()

	// В усовии проверяется ночной срез обрабатывается или дневной
	// 1-13 ночной срез
	// 13-1 дневной срез
	clickhouseConn, err := GetConn(cfg)
	if err != nil {
		clickhouseConn.Close()

		return err
	}
	var count_file uint64
	if err := clickhouseConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM file_name").Scan(&count_file); err != nil {
		clickhouseConn.Close()

		return err
	}
	if count_file < 42 {
		if hour >= 1 && hour < 13 {
			if !prevTime {
				config.Log.Info("Смена среза на 00-12")
				newSlice(cfg)
				prevTime = true
			}
		} else {
			if prevTime {
				config.Log.Info("Смена среза на 12-00")
				newSlice(cfg)
				prevTime = false
			}
		}
	} else {
		if hour >= 0 && hour < 12 {
			if !prevTime {
				newSlice(cfg)
				prevTime = true
			}
		} else {
			if prevTime {
				newSlice(cfg)
				prevTime = false
			}
		}
	}
	clickhouseConn.Close()
	config.Log.Info("Время проверено!")

	return nil

}

// Удаляет предыдущий срез из базы данных
func deletePrev(cfg *config.Config) {

	conn, err := GetConn(cfg)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка получения соединения из пула во время удаления таблицы!(deletePrev)")
		os.Exit(1)
	}
	err = conn.Exec(context.Background(), "DROP TABLE IF EXISTS grib_data_prev")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка удаления таблицы!(dp)")
		os.Exit(1)
	}
	err = conn.Exec(context.Background(), "DROP TABLE IF EXISTS grid_prev")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка удаления таблицы!(dp)")
		os.Exit(1)
	}
	err = conn.Exec(context.Background(), "DROP TABLE IF EXISTS view_prev")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка удаления таблицы!(dp)")
		os.Exit(1)
	}

	conn.Close()
}
func DelFileName(cfg *config.Config) error {
	cur := time.Now()
	hour := cur.Hour()
	if hour == 23 || hour == 11 || hour == 1 || hour == 13 {
		conn, err := GetConn(cfg)
		if err != nil {
			return err
		}
		err = conn.Exec(context.Background(), "TRUNCATE TABLE IF EXISTS file_name")
		if err != nil {
			config.Log.WithError(err).Error("Ошибка удаления таблицы!(fn)")
			os.Exit(1)
		}
	}
	return nil
}

// Переименовывает таблицу grib_data в таблицу prev_grib_data
func renameGribData(cfg *config.Config) {
	conn, err := GetConn(cfg)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка получения соединения из пула во время переноса таблицы!(renameGribData)")
		os.Exit(1)
	}
	err = conn.Exec(context.Background(), "RENAME TABLE grib_data TO grib_data_prev")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса таблицы!(rgd)")
		os.Exit(1)
	}
	err = conn.Exec(context.Background(), "RENAME TABLE grid TO grid_prev")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса таблицы!(rgd)")
		os.Exit(1)
	}
	err = conn.Exec(context.Background(), "RENAME TABLE view_data TO view_prev")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса таблицы!(rgd)")
		os.Exit(1)
	}

	conn.Close()
}

// Переименовывает временную таблицу и ее индекс из grib_data_buff в grib_data
func renameBuff(cfg *config.Config) {
	conn, err := GetConn(cfg)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка получения соединения из пула во время переноса таблицы!(renameBuff)")
		os.Exit(1)
	}
	err = conn.Exec(context.Background(), "RENAME TABLE grib_data_buff TO grib_data")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса таблицы!(rb)")
		os.Exit(1)
	}
	err = conn.Exec(context.Background(), "RENAME TABLE grid_buff TO grid")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса таблицы!(rb)")
		os.Exit(1)
	}
	err = conn.Exec(context.Background(), "RENAME TABLE view_buff TO view_data")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса таблицы!(rb)")
		os.Exit(1)
	}

	conn.Close()
}

// Если буфер пустой, то новый срез не был добавлен
func isEmptyBuff(cfg *config.Config) bool {
	conn, err := GetConn(cfg)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка получения соединения!")
		conn.Close()
		return true
	}
	var count uint64
	err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM grib_data_buff").Scan(&count)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка выполнения запроса!")
		conn.Close()
		return true
	}
	conn.Close()
	return count == 0
}

// Обобщаяющая функция, которая запускает в необходимом порядке изменения в базе данных
func newSlice(cfg *config.Config) {
	if isEmptyBuff(cfg) {
		config.Log.Warn("Буфер пустой! Новый срез не был добавлен!")
		return
	}
	deletePrev(cfg)
	renameGribData(cfg)
	renameBuff(cfg)
}
