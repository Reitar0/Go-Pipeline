package pipeline

import (
	"context"
	"sync"
)

// Source запускает генератор `fn`, который производит данные и отправляет их в выходной канал.
// Это узел-источник, у него нет входов.
// Тип [T any] делает эту функцию типобезопасной.
func Source[T any](ctx context.Context, p *Pipeline, fn func(out chan<- T)) <-chan T {
	out := make(chan T)
	p.Add(func() {
		// После завершения работы генератора, канал-выход нужно закрыть.
		// Это будет сигналом для следующих узлов, что данные закончились.
		defer close(out)
		fn(out)
	})
	return out
}

// FanOut запускает `workerCount` параллельных воркеров, которые читают данные
// из входного канала `in`, обрабатывают их с помощью функции `fn` и отправляют
// результат в выходной канал.
func FanOut[In, Out any](ctx context.Context, p *Pipeline, in <-chan In, workerCount int, fn func(item In) (Out, error)) <-chan Out {
	out := make(chan Out)

	// Эта WaitGroup нужна, чтобы дождаться завершения всех воркеров,
	// прежде чем закрывать выходной канал `out`.
	var wg sync.WaitGroup

	p.Add(func() {
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			p.Add(func() { // Каждый воркер - отдельная задача для пайплайна
				defer wg.Done()
				for {
					select {
					case item, ok := <-in:
						if !ok {
							// Входной канал закрыт, воркеру больше нечего делать.
							return
						}
						// Обрабатываем данные.
						result, err := fn(item)
						if err != nil {
							// В случае ошибки останавливаем весь пайплайн.
							p.Stop(err)
							return
						}

						// Отправляем результат дальше, но также проверяем на отмену.
						select {
						case out <- result:
						case <-ctx.Done():
							// Пайплайн был остановлен, пока мы обрабатывали данные.
							return
						}

					case <-ctx.Done():
						// Пайплайн был остановлен. Завершаем работу.
						return
					}
				}
			})
		}

		// Ожидаем завершения всех воркеров и только потом закрываем выходной канал.
		wg.Wait()
		close(out)
	})

	return out
}

// Sink запускает потребителя, который читает данные из канала `in` и выполняет над ними
// финальную операцию `fn`. Это узел-приемник, у него нет выходов.
func Sink[T any](ctx context.Context, p *Pipeline, in <-chan T, fn func(item T)) {
	p.Add(func() {
		for {
			select {
			case item, ok := <-in:
				if !ok {
					// Канал закрыт, данных больше нет.
					return
				}
				fn(item)
			case <-ctx.Done():
				// Пайплайн остановлен.
				return
			}
		}
	})
}
