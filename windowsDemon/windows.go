package windowsDemon

import (
	"Demon.com/config"
	clickhouse_conn "Demon.com/watcher/clickhouse"


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
func Windows(cfg *config.Config)error {
	if err:=clickhouse_conn.Ping(cfg);err!=nil{
		config.Log.WithError(err).Error("ClickHouse не отвечает!")
		return err
	}
	return nil
}


