package linuxDemon

import (
	"Demon.com/config"
	"Demon.com/watcher/postgres"
	"os/exec"
	"strings"
	"time"

)


/*
В зависимости от операционной системы различаются команды для проверки статуса активности БД
 Нужно добавить:
 - проверку работы postgresql
 - проверку работы clickhouse 
 - доработать cfg, чтобы можно было указать на какой БД запущен сервис
 - в зависимости от бд необходимо запускать ее, если она упала
 - дополнительные проверки полноты данных
*/
func Linux(cfg *config.Config) error {

	if !linux_isPostgresRunning() {
		err := linux_RunPostgres()
		if err != nil {
			config.Log.Warn(err)
			return err
		}
	} else {
		config.Log.Info("PostgreSQL уже запущена!")
	}
	// инициализация пула соединений с Postgresql
	postgres.StartDatabaseConnection(cfg)
	for {

		config.Log.Info("Проверка конфигурации системы, подождите...")
		if !linux_isPostgresRunning() {
			err := linux_RunPostgres()
			if err != nil {
				config.Log.Warn(err)
				return err
			}
		} else {
			config.Log.Info("PostgreSQL уже запущена!")
		}
		time.Sleep(time.Minute * 5)

	}

}

// запускает Postgresql на linux
func linux_RunPostgres() error {
	config.Log.Info("PostgreSQL не запущена, запуск...")
	cmd := exec.Command("sudo", "systemctl", "start", "postgres")
	err := cmd.Run()
	if err != nil {
		config.Log.WithError(err).Error("Ошибка запуска PostgreSQL")
		return err
	}
	config.Log.Info("PostgreSQL успешно запущен")
	return nil
}

// проверяет активность Postgresql
func linux_isPostgresRunning() bool {
	config.Log.Info("Проверка postgreSQL...")
	cmd := exec.Command("systemctl", "is-active", "postgresql")
	output, err := cmd.CombinedOutput()
	if err != nil {
		config.Log.WithError(err).Error("Ошибка проверки статуса PostgreSQL")
		return false
	}
	result := strings.TrimSpace(string(output))
	if strings.Contains(result, "active") || strings.Contains(result, "активна") {
		return true
	}
	return false
}

