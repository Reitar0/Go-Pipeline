package pipeline

import (
	"context"
	"io"
	"log"
	"sync"
)

// Pipeline управляет выполнением набора горутин, обеспечивает graceful shutdown
type Pipeline struct {
	wg     sync.WaitGroup
	cancel context.CancelCauseFunc // Функция для отмены контекста с указанием причины
	logger *log.Logger
}

// Option определяет тип для функциональных опций конструктора Pipeline
type Option func(*Pipeline)

// WithLogger задает кастомный логгер для пайплайна
func WithLogger(logger *log.Logger) Option {
	return func(p *Pipeline) {
		p.logger = logger
	}
}

// New создает новый экземпляр пайплайна.
// Пайплайн будет работать в рамках переданного родительского контекста.
func New(ctx context.Context, opts ...Option) (*Pipeline, context.Context) {
	// Создаем дочерний контекст, который мы сможем отменить.
	// Позволяет передать ошибку как причину отмены
	childCtx, cancel := context.WithCancelCause(ctx)

	p := &Pipeline{
		cancel: cancel,
		// По умолчанию логгер ничего не выводит
		logger: log.New(io.Discard, "[pipeline] ", log.LstdFlags),
	}

	for _, opt := range opts {
		opt(p)
	}

	return p, childCtx
}

// Add добавляет в пайплайн горутину для выполнения.
// Пайплайн будет ожидать её завершения.
func (p *Pipeline) Add(task func()) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		task()
	}()
}

// Stop инициирует остановку всех процессов в пайплайне.
// В качестве причины передается err. Если err == nil, остановка считается штатной
func (p *Pipeline) Stop(err error) {
	p.cancel(err)
}

// Wait блокирует выполнение до тех пор, пока все задачи в пайплайне не завершатся
func (p *Pipeline) Wait() {
	p.wg.Wait()
}
