package postgres

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"Demon.com/config"
)

func PgInitUserAndCities() error {

	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		return err
	}
	q_users := `CREATE TABLE IF NOT EXISTS users(
		id uuid PRIMARY KEY,
		username CHARACTER VARYING(20) NOT NULL UNIQUE,
		password CHARACTER VARYING(64) NOT NULL,
		role CHARACTER VARYING(5) NOT NULL default 'user',
		salt CHARACTER VARYING(64) NOT NULL,
		is_active BOOLEAN default false)`
	q_cities := `CREATE TABLE IF NOT EXISTS cities (
			id uuid NOT NULL,
			name text NULL,
			latitude float8 NULL,
			longitude float8 NULL,
			population int8 NULL,
			CONSTRAINT cities_pkey PRIMARY KEY (id)
		)`
	q_Exten := `CREATE EXTENSION IF NOT EXISTS pg_trgm`
	q_Index := `CREATE INDEX IF NOT EXISTS trgm_idx ON cities USING GIN (name gin_trgm_ops)`

	_,err = conn.Exec(context.Background(), q_users)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка создания таблицы users!")
		return err
	}
	config.Log.Info("Таблица пользователей создана!")

	_, err = conn.Exec(context.Background(), q_cities)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка создания таблицы cities!")
		return err
	}
	config.Log.Info("Таблица cities создана!")
	err = copyCSV()
	if err!=nil{
		return err
	}
	config.Log.Info("Таблица городов заполнена!")

	_, err = conn.Exec(context.Background(), q_Exten)
	if err != nil {
		return err
	}
	config.Log.Info("Расширение активировано!")

	_, err = conn.Exec(context.Background(), q_Index)
	if err != nil {
		return err
	}
	config.Log.Info("Индексы установлены!")

	conn.Release()
	config.Log.Info("Таблицы users и cities проинициализированы!")

	return nil
}
func copyCSV() error {

	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		return err
	}
	_,err = conn.Exec(context.Background(),"SET client_encoding TO 'UTF-8'")
	if err!=nil{
		config.Log.WithError(err).Error("Ошибка установки кодировки!")
		return err
	}
	config.Log.Info("Кодировка базы данных установлена!")
	file, err := os.Open("./finalCities.csv")
	if err != nil {
		return err
	}
	defer file.Close()
	config.Log.Info("Чтение csv...")

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	config.Log.Info("Запись городов в БД...")
	for index, record := range records {
		if index == 0 {
			continue
		}
		id:=record[0]
		name:=record[1]
		latitude,_:=strconv.ParseFloat(record[2],32)
		longitude,_:=strconv.ParseFloat(record[3],32)
		population,_:=strconv.Atoi(record[4])
		_, err := conn.Exec(context.Background(), "INSERT INTO cities (id, name, latitude, longitude, population) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO NOTHING",id,name,latitude,longitude,population )
		if err != nil {
			return err
		}
	}
	config.Log.Info("Таблица городов создана!")
	return nil
}

func PgInitForGribData() {
	// Подготовка таблиц к работе
	migrateGribData()
	migrateHash()
	// Инициализация времени для проверки срезов
	timeInit()
	config.Log.Info("База данных готова к работе!")
}

// Создает необходимые таблицы для работы при их отсутствии
func migrateGribData() {
	createTableSQL := `CREATE TABLE IF NOT EXISTS grib_data
	(
		id uuid NOT NULL,
		grib_datetime timestamp without time zone,
		forecast_time integer,
		parameter text COLLATE pg_catalog."default",
		surface_type text COLLATE pg_catalog."default",
		surface_value text COLLATE pg_catalog."default",
		grid_properties json,
		grib_data double precision[],
		grib_data_int integer[],
		CONSTRAINT grib_data_pkey PRIMARY KEY (id)
	)`

	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		config.Log.WithError(err).Error("Ошибка соединения с БД при создании таблицы")
		os.Exit(1)
	}
	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка выполнения запроса создания таблицы")
		conn.Release()
		os.Exit(1)
	}
	createTableSQL = `CREATE TABLE IF NOT EXISTS prev_grib_data
	(
		id uuid NOT NULL,
		grib_datetime timestamp without time zone,
		forecast_time integer,
		parameter text COLLATE pg_catalog."default",
		surface_type text COLLATE pg_catalog."default",
		surface_value text COLLATE pg_catalog."default",
		grid_properties json,
		grib_data double precision[],
		grib_data_int integer[],
		CONSTRAINT prev_grib_data_pkey PRIMARY KEY (id)
	)`

	if err != nil {
		config.Log.WithError(err).Error("Ошибка соединения с БД при создании таблицы")
		os.Exit(1)
	}
	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка выполнения запроса создания таблицы")
		conn.Release()
		os.Exit(1)
	}
	createTableSQL = `CREATE TABLE IF NOT EXISTS grib_data_buff
	(
		id uuid NOT NULL,
		grib_datetime timestamp without time zone,
		forecast_time integer,
		parameter text COLLATE pg_catalog."default",
		surface_type text COLLATE pg_catalog."default",
		surface_value text COLLATE pg_catalog."default",
		grid_properties json,
		grib_data double precision[],
		grib_data_int integer[],
		CONSTRAINT grib_data_buff_pkey PRIMARY KEY (id)
	)`

	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка выполнения запроса создания таблицы")
		conn.Release()
		os.Exit(1)
	}
	config.Log.Info("Таблица готова к работе!")
	conn.Release()
	config.Log.Info("Таблицы для хранения данных созданы!")
}
func migrateHash() {
	tableName := "hashes"

	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
	(
		grib_hash character varying(256) NOT NULL,
		CONSTRAINT hashes_pkey PRIMARY KEY (grib_hash)
	)`, tableName)

	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		config.Log.WithError(err).Error("Ошибка соединения с БД при создании таблицы")
		os.Exit(1)
	}
	_, err = conn.Exec(context.Background(), createTableSQL)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка выполнения запроса создания таблицы")
		conn.Release()
		os.Exit(1)
	}
	config.Log.Info("Таблица готова к работе!")
	conn.Release()
	config.Log.Info("Таблица хешей создана!")
}

// Обобщенная функция проверки таблиц в БД
func CheckDatabase() {
	migrateGribData()
	migrateHash()
}

// false - дневной срез
// true - ночной срез
var prevTime bool

// Инициализация переменной prevTime
func timeInit() {
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
func timeCheck() {
	currentTime := time.Now()
	hour := currentTime.Hour()
	// В усовии проверяется ночной срез обрабатывается или дневной
	// 1-13 ночной срез
	// 13-1 дневной срез
	if hour >= 1 && hour < 13 {
		if !prevTime {
			config.Log.Info("Смена среза на 00-12")
			newSlice()
			prevTime = true
		}
	} else {
		if prevTime {
			config.Log.Info("Смена среза на 12-00")
			newSlice()
			prevTime = false
		}
	}

}

// Удаляет предыдущий срез из базы данных
func deletePrev() {
	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		config.Log.WithError(err).Error("Ошибка получения соединения из пула во время удаления таблицы!(deletePrev)")
		os.Exit(1)
	}
	_, err = conn.Exec(context.Background(), "DROP TABLE IF EXISTS prev_grib_data")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка удаления таблицы!(dp)")
		os.Exit(1)
	}
	conn.Release()
}

// Переименовывает таблицу grib_data в таблицу prev_grib_data
func renameGribData() {
	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		config.Log.WithError(err).Error("Ошибка получения соединения из пула во время переноса таблицы!(renameGribData)")
		os.Exit(1)
	}
	_, err = conn.Exec(context.Background(), "ALTER TABLE grib_data RENAME TO prev_grib_data")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса таблицы!(rgd)")
		os.Exit(1)
	}
	_, err = conn.Exec(context.Background(), "ALTER INDEX grib_data_pkey RENAME TO prev_grib_data_pkey")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса индекса!(rgd)")
		os.Exit(1)
	}
	conn.Release()
}

// Переименовывает временную таблицу и ее индекс из grib_data_buff в grib_data
func renameBuff() {
	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		config.Log.WithError(err).Error("Ошибка получения соединения из пула во время переноса таблицы!(renameBuff)")
		os.Exit(1)
	}
	_, err = conn.Exec(context.Background(), "ALTER TABLE grib_data_buff RENAME TO grib_data")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса таблицы!(rb)")
		os.Exit(1)
	}
	_, err = conn.Exec(context.Background(), "ALTER INDEX grib_data_buff_pkey RENAME TO grib_data_pkey")
	if err != nil {
		config.Log.WithError(err).Error("Ошибка переноса индекса!(rb)")
		os.Exit(1)
	}
	conn.Release()
}
func isEmptyBuff() bool {
	conn, err := Dbpool.Acquire(context.Background())
	if err != nil {
		config.Log.WithError(err).Error("Ошибка получения соединения!")
		return true
	}
	var count int
	err = conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM grib_data_buff").Scan(&count)
	if err != nil {
		config.Log.WithError(err).Error("Ошибка выполнения запроса!")
		return true
	}
	return count == 0
}

// Обобщаяющая функция, которая запускает в необходимом порядке изменения в базе данных
func newSlice() {
	if isEmptyBuff() {
		config.Log.Warn("Буфер пустой! Новый срез не был добавлен!")
		return
	}
	deletePrev()
	renameGribData()
	renameBuff()
}
