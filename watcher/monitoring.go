package watcher

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"Demon.com/config"
	clickhouse_conn "Demon.com/watcher/clickhouse"
	"Demon.com/watcher/postgres"
)

// запускает Parser и ждет его завершения
func parser(str_parser string) {
	// Засекает время выполнения
	start := time.Now()
	var err error
	cmd := exec.Command(str_parser)
	// Запуск парсера
	err = cmd.Start()
	if err != nil {
		config.Log.WithError(err).Error("Ошибка парсера")
		e := err.Error()
		if e == "10" {
			config.Log.Error("Ошибка конфигурации парсера!")
		} else if e == "11" {
			config.Log.Error("Ошибка подключения парсера к БД!")
		} else if e == "12" {
			config.Log.Error("Ошибка главного потока парсера!")
		}
		config.Log.Error("Аварийное завершение программы...")
		os.Exit(1)
	}
	// Ожидание завершения парсера
	err = cmd.Wait()
	if err != nil {
		config.Log.WithError(err).Error("Ошибка парсера")
		e := err.Error()
		if e == "10" {
			config.Log.Error("Ошибка конфигурации парсера!")
		} else if e == "11" {
			config.Log.Error("Ошибка подключения парсера к БД!")
		} else if e == "12" {
			config.Log.Error("Ошибка главного потока парсера!")
		}
		config.Log.Error("Аварийное завершение программы...")
		os.Exit(1)
	}
	config.Log.Info("Парсер отработал: ", time.Since(start))
}

// Проверяет дирректорию на наличие новых grib2-файлов и запускает парсер (устаревшая функция с встроенным мониторингом)
// func CheckDirForParser(cfg *config.Config) error {
// 	str_parser := "./Parser"
// 	dirPath := cfg.SrcDir
// 	watcher, err := fsnotify.NewWatcher()
// 	if err != nil {
// 		config.Log.Fatal(err)
// 	}
// 	defer watcher.Close()

// 	time.Sleep(time.Second * 30)
// 	CheckDatabase()
// 	parser(str_parser)
// 	timeCheck()

// 	err = watcher.Add(dirPath)
// 	if err != nil {
// 		config.Log.WithError(err).Error("Ошибка установки мониторинга: ", dirPath)
// 		return err
// 	}
// 	config.Log.Info("Мониторинг запущен!")
// 	for {
// 		config.Log.Info("Сканирование папки на наличие новых grib-файлов...")
// 		select {
// 		case _, ok := <-watcher.Events:
// 			if !ok {
// 				return errors.New("Мониторинг остановлен")
// 			}
// 			// config.Log.Info("Найдены новые файлы, запуск парсера...")
// 			CheckDatabase()
// 			parser(str_parser)
// 			timeCheck()

// 		case err, ok := <-watcher.Errors:
// 			if !ok {
// 				config.Log.WithError(err).Error("Ошибка мониторинга директории")
// 				return err
// 			}
// 		default:
// 			config.Log.Info("Новые файлы не обнаружены")
// 		}
// 		time.Sleep(time.Second * 10) // Подождать перед следующей проверкой
// 	}
// }

// Проверяет дирректорию на наличие новых grib2-файлов и запускает парсер, для каждого найденного файла делает запрос в БД был ли такой файл записан,
// если нет, то запускает парсер, если да, то перемещает этот файл в отдеьную дирректорию
func CheckDir(cfg *config.Config) error {
	str_parser := "./Parser"
	dirPath := cfg.SrcDir
	postgres.StartDatabaseConnection(cfg)
	err := postgres.PgInitUserAndCities()
	if err != nil {
		return err
	}
	clickhouse_conn.TimeInit()
	err = clickhouse_conn.CheckTable(cfg)
	if err != nil {
		return err
	}
	for {
		clickhouse_conn.DelFileName(cfg)
		// Проверяется соединение с ClickHouse
		if err := clickhouse_conn.Ping(cfg); err != nil {
			config.Log.WithError(err).Warn("Не удалось установить соединение с clickhouse!")
			return err
		}
		config.Log.Info("Соединение с ClickHouse стабильно!")

		// Читаются все файлы из src
		files, err := os.ReadDir(dirPath)
		if err != nil {
			config.Log.WithError(err).Error("Не найден путь!")
			return err
		}

		// Если файлов нет, то ждем дальше
		if len(files) != 0 {
			time.Sleep(time.Minute*2)
			config.Log.Info("Проверка найденных файлов...")
			// Проходимся циклом по найденным файлам
			for _, file := range files {
				// Если найденная запись это директория, то пропускаем ее
				if !file.IsDir() {
					// Получаем полный путь к файлу
					filePath := filepath.Join(dirPath, file.Name())
					if filePath == "" {
						config.Log.WithField("file", filePath).Warn("Пустое поле!")
						continue
					}
					// Проверяет, есть ли такой файл в БД
					ok := checkExistCh(filePath, cfg)
					if !ok {
						config.Log.Info("Файл записан в систему!")
						// Перемещает файл, если он был записан ранее
						moveFile(file, cfg)
						config.Log.WithField("file", file.Name()).Warn("Уже записан!")
						continue
					}

					// Подготовка таблиц к записи
					clickhouse_conn.CheckTable(cfg)
					// Запускает парсер и ждет его завершения
					parser(str_parser)
					// Проверяет время и при смене среза меняет таблицы
					break
				}
			}
		}
		clickhouse_conn.CheckTime(cfg)
		time.Sleep(time.Second * 30)

	}

}

// Перемещает файл в отдельную дирректорию
func moveFile(file fs.DirEntry, cfg *config.Config) {
	if _, err := os.Stat(cfg.MoveDir); os.IsNotExist(err) {
		err := os.MkdirAll(cfg.MoveDir, 0755)
		if err != nil {
			config.Log.WithError(err).Error("ошибка создания директории")
			return
		}
	}
	err := os.Rename(filepath.Join(cfg.SrcDir, file.Name()), filepath.Join(cfg.MoveDir, file.Name()))
	if err != nil {
		config.Log.WithError(err).Error("Ошибка перемещения файла!")
		return
	}
}

// Проверяет записан ли файл в БД
func checkExistCh(filePath string, cfg *config.Config) bool {
	gribFile, err := os.Open(filePath)
	if err != nil {
		return false
	}
	defer gribFile.Close()
	fileInfo, err := gribFile.Stat()
	if err != nil {
		return false
	}
	clickhouseConn, err := clickhouse_conn.GetConn(cfg)
	if err != nil {
		return false
	}
	q_string := fmt.Sprintf("SELECT COUNT() FROM file_name WHERE file_name = '%s'", fileInfo.Name())
	var count uint64
	err = clickhouseConn.QueryRow(context.Background(), q_string).Scan(&count)
	if err != nil {
		return false
	}
	if count != 0 {
		return false
	}
	return true
}
