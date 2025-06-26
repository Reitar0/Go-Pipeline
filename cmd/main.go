package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"testtask_pipeline/pipeline"
	"time"
)

// Result хранит результат вычисления хеша для одного файла.
type Result struct {
	Path string
	Hash string
}

func main() {
	// === Параметры запуска ===
	// -dir: директория для сканирования (по умолчанию текущая)
	// -workers: число параллельных воркеров (по умолчанию 10)
	// -v: включить подробное логирование пайплайна
	dir := flag.String("dir", ".", "директория для сканирования")
	workers := flag.Int("workers", 10, "количество параллельных воркеров для вычисления хешей")
	verbose := flag.Bool("v", false, "включить подробное логирование пайплайна")
	flag.Parse()

	// Инициализация пайплайна.

	// Создаем логгер, который будет использоваться, если включен флаг -v
	var logger *log.Logger
	if *verbose {
		logger = log.New(os.Stdout, "[pipeline] ", log.LstdFlags)
	}

	// Создаем пайплайн и корневой контекст для него
	p, ctx := pipeline.New(
		context.Background(),
		pipeline.WithLogger(logger), // Передаем наш опциональный логгер
	)

	log.Printf("Запуск сканирования директории '%s' с %d воркерами...\n", *dir, *workers)
	startTime := time.Now()

	// Сборка и запуск конвейера

	// Передаем в Source[string] функцию, которая будет рекурсивно обходить
	// директорию и отправлять пути к файлам в канал `out`.
	pathsChan := pipeline.Source(ctx, p, func(out chan<- string) {
		filepath.Walk(*dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				// Если ошибка при обходе, можно ее проигнорировать или остановить пайплайн
				p.Stop(fmt.Errorf("ошибка при обходе %s: %w", path, err))
				return err
			}

			// Проверяем, не запрошена ли остановка пайплайна
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if !info.Mode().IsRegular() {
				return nil // Пропускаем директории, симлинки и т.д.
			}

			out <- path // Отправляем путь к файлу в канал
			return nil
		})
	})

	// Мы берем канал с путями `pathsChan`, указываем количество воркеров
	// и передаем функцию, которая вычисляет MD5 для одного файла.
	resultsChan := pipeline.FanOut(ctx, p, pathsChan, *workers, func(path string) (Result, error) {
		file, err := os.Open(path)
		if err != nil {
			// Ошибка открытия файла - критична. Останавливаем весь пайплайн.
			return Result{}, fmt.Errorf("не удалось открыть файл %s: %w", path, err)
		}
		defer file.Close()

		hash := md5.New()
		if _, err := io.Copy(hash, file); err != nil {
			// Ошибка чтения критична
			return Result{}, fmt.Errorf("не удалось прочитать файл %s: %w", path, err)
		}

		return Result{Path: path, Hash: hex.EncodeToString(hash.Sum(nil))}, nil
	})

	// Берем канал с результатами `resultsChan` и передаем функцию,
	// которая просто печатает каждый результат в консоль.
	pipeline.Sink(ctx, p, resultsChan, func(res Result) {
		fmt.Printf("%s  %s\n", res.Hash, res.Path)
	})

	// Ожидание завершения пайплайна
	p.Wait()

	log.Printf("Работа завершена за %v.\n", time.Since(startTime))

	// Проверяем, не была ли работа завершена из-за ошибки
	if err := context.Cause(ctx); err != nil && err != context.Canceled {
		log.Printf("Пайплайн остановлен с ошибкой: %v\n", err)
		os.Exit(1)
	}
}
