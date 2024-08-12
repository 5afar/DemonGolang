package main

import (
	"Demon.com/config"
	"Demon.com/watcher"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"golang.org/x/sync/errgroup"
)

var errGroup errgroup.Group

// sigCheck обрабатывает системные синалы и завершает процесс
func sigCheck() error {
	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	config.Log.Info("Остановка программы...")

	os.Exit(0)
	return nil
}

// init инициализирует .env
func init() {
	if err := godotenv.Load("./.env"); err != nil {
		config.Log.Warn("Не найден файл среды")
		panic("Не удалось загрузить переменные окружения!")
	}
}

func main() {
	// получение конфигурации программы
	cfg := config.New()
	// получение операционной системы, на которой запущена программа
	// currentOs := runtime.GOOS

	// инициализация лог-файла для демона
	file, err := os.OpenFile("demon_log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		config.Log.Error("Ошибка открытия файла лога: ", err)
	}
	// перевод логирования с консоли в файл
	config.LoggerStart(file)

	// запуск горутины, которая отслеживает новые файлы и запускает парсер
	errGroup.Go(func() error {
		config.Log.Info("Запуск мониторинга директории...")
		return watcher.CheckDir(cfg)
	})

	// запуск горутины, которая обрабатывает сигналы отправленные в программу
	errGroup.Go(func() error {
		config.Log.Info("Для остановки приложения нажмите 'CTRL+C'")
		return sigCheck()
	})

	// в зависимости от операционной системы запускаются разные методы отслеживания состояния (на данный момент не используется)
	// switch currentOs {
	// case "windows":
	// 	errGroup.Go(func() error {
	// 		config.Log.Info("Старт конфигурации для Windows...")
	// 		return wd.Windows(cfg)
	// 	})
	// case "linux":
	// 	errGroup.Go(func() error {
	// 		config.Log.Info("Старт конфигурации для Linux...")
	// 		return ld.Linux(cfg)
	// 	})
	// default:
	// 	config.Log.Warn("Unsupported OS: ", currentOs)
	// }

	if err := errGroup.Wait(); err != nil {
		config.Log.Info("Принудительное завершение программы!")
		return
	}

}
