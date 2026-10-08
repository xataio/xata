package o11y

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/elastic/go-concert/unison"
	"github.com/go-logr/zerologr"
	grpczerolog "github.com/philip-bui/grpc-zerolog"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	metricembedded "go.opentelemetry.io/otel/metric/embedded"
	noopm "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	traceembedded "go.opentelemetry.io/otel/trace/embedded"
	noopt "go.opentelemetry.io/otel/trace/noop"

	"xata/internal/o11y/version"
)

type System struct {
	tracing         *tracing
	metrics         *metrics
	logger          zerolog.Logger
	profilingServer *http.Server

	profiler profiler

	idStyle TraceIDStyle

	// store logOutput if it  was instantiated internally.
	logOutput io.Writer
	logClose  func()
}

type O struct {
	metricembedded.MeterProvider
	traceembedded.TracerProvider

	logger            zerolog.Logger
	traceProvider     trace.TracerProvider
	meterProvider     metric.MeterProvider
	textMapPropagator propagation.TextMapPropagator

	serviceNamespace string
	serviceName      string
	serviceVersion   string

	system *System
}

var (
	_ trace.TracerProvider = (*O)(nil)
	_ metric.MeterProvider = (*O)(nil)
)

type ctxKey struct{}

var initGlobalOnce sync.Once

var defaultTextMapPropagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

func New(ctx context.Context, config *Config) System {
	output := NewLogOutput(config.ConsoleJSON)
	logCloser := []io.WriteCloser{output}
	var logsOut io.Writer = output
	if addr := config.LogTCPOut; addr != "" {
		output, err := NewTCPLogOutput(addr)
		if err != nil {
			log.Fatal("failed to init tcp output: %w", err)
		}

		logCloser = append(logCloser, output)
		logsOut = io.MultiWriter(logsOut, output)
	}

	logger := NewLogger(logsOut, config)

	// per https://github.com/xataio/xata/pull/1335 - we limit the level for monitoring logs in order to reduce noise
	monitoringLogLevel := max(logger.GetLevel(), zerolog.InfoLevel)
	monitoringLogger := logger.With().Logger().Level(monitoringLogLevel)

	var (
		metrics *metrics
		tracing *tracing
	)
	if config.Tracing {
		tracing = initTracing(ctx, &monitoringLogger, resource.Default())
		metrics = initMetrics(ctx, &monitoringLogger, resource.Default(), config.MetricsPeriod)
	}


	var profiler profiler
	var profilingServer *http.Server
	initGlobalOnce.Do(func() {
		SetGlobalLogger(&logger)

		otelLogger := monitoringLogger.With().Str("module", "otel").Logger()
		otel.SetLogger(zerologr.New(&otelLogger))
		// SDK export failures reach this handler after the exporter stops retrying.
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
			otelLogger.Error().Err(err).Msg("OpenTelemetry SDK error")
		}))
		grpczerolog.GrpcLogSetZeroLogger(grpczerolog.NewGrpcZeroLogger(otelLogger))

		otel.SetTracerProvider(tracing.Provider(ctx, &otelLogger, "xata", "global"))
		otel.SetMeterProvider(metrics.Provider("xata", "global"))
	})

	var err error
	profiler, err = config.Profiling.GetValue().Start(config)
	if err != nil {
		profiler = (*noopProfiler)(nil)
	}

	if addr := config.ProfilingServer; addr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		server := &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		}

		profilingServer = server

		go func() {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error().Err(err).Msg("Failed to start profiling server")
			}
		}()
	}

	idStyle := config.IDStyle.style
	if idStyle == nil {
		idStyle = PlainIDStyle
	}

	return System{
		logger:          logger,
		tracing:         tracing,
		metrics:         metrics,
		profiler:        profiler,
		profilingServer: profilingServer,

		idStyle: idStyle,

		// internal resources for cleanup on `Shutdown`
		logOutput: logsOut,
		logClose: func() {
			for _, out := range logCloser {
				out.Close()
			}
		},
	}
}

func (sys *System) Shutdown(ctx context.Context) (err error) {
	tg := unison.TaskGroupWithCancel(ctx)
	tg.OnQuit = unison.ContinueOnErrors
	tg.Go(sys.tracing.Shutdown)
	tg.Go(sys.metrics.shutdown)
	if sys.profilingServer != nil {
		tg.Go(sys.profilingServer.Shutdown)
	}

	err = tg.Wait()

	errProf := sys.profiler.Stop()
	if err == nil && errProf != nil {
		err = errProf
	}

	if sys.logClose != nil {
		sys.logClose()
	}

	return err
}

func (sys *System) Logger() zerolog.Logger {
	return sys.logger
}

func (sys *System) MergeWith(res *resource.Resource) error {
	// tracing
	merged, err := resource.Merge(sys.tracing.defaultResource, res)
	if err != nil {
		return err
	}

	sys.tracing.defaultResource = merged

	// metrics
	merged, err = resource.Merge(sys.metrics.defaultResource, res)
	if err != nil {
		return err
	}

	sys.metrics.defaultResource = merged

	return nil
}

func (sys *System) ForService(ctx context.Context, serviceNamespace, serviceName string) O {
	// prepare logger with Otel spec fields:
	// https://github.com/open-telemetry/opentelemetry-specification/tree/main/specification/logs
	serviceVersion := version.Get()

	loggerCtx := logCtxWithServiceName(sys.logger.With(), serviceNamespace, serviceName, serviceVersion)
	logger := loggerCtx.Logger()

	tracerProvider := sys.tracing.Provider(ctx, &logger, serviceName, serviceNamespace)
	meterProvider := sys.metrics.Provider(serviceName, serviceNamespace)

	return O{
		system: sys,

		logger:            logger,
		traceProvider:     tracerProvider,
		meterProvider:     meterProvider,
		textMapPropagator: defaultTextMapPropagator,

		serviceNamespace: serviceNamespace,
		serviceName:      serviceName,
		serviceVersion:   serviceVersion,
	}
}

func Ctx(ctx context.Context) *O {
	if o, ok := ctx.Value(ctxKey{}).(*O); ok {
		return o
	}
	return nil
}

// Close stops collecting metrics for this service and exports what it recorded
// since the last collect. Services close their provider after their own
// context is cancelled, so ctx must not be that context: use one that outlives
// it, such as context.WithoutCancel(ctx). The export is bounded by the metrics
// flush timeout.
func (o *O) Close(ctx context.Context) {
	if o == nil {
		return
	}
	o.system.metrics.unregister(ctx, o.meterProvider)
}

func (o *O) WithContext(ctx context.Context) context.Context {
	if old := Ctx(ctx); old == o {
		return ctx
	}

	ctx = context.WithValue(ctx, ctxKey{}, o)
	ctx = o.logger.WithContext(ctx)
	return ctx
}

func (o *O) ServiceName() string {
	if o == nil {
		return ""
	}
	return o.serviceName
}

func (o *O) ServiceNamespace() string {
	if o == nil {
		return ""
	}
	return o.serviceNamespace
}

func (o *O) ServiceVersion() string {
	if o == nil {
		return ""
	}
	return o.serviceVersion
}

func (o *O) Logger() zerolog.Logger {
	if o == nil {
		return zerolog.Nop()
	}
	return o.logger
}

func (o *O) Tracer(instrumentationName string, opts ...trace.TracerOption) trace.Tracer {
	if o == nil || o.traceProvider == nil {
		return noopt.NewTracerProvider().Tracer(instrumentationName, opts...)
	}
	return o.traceProvider.Tracer(instrumentationName, opts...)
}

func (o *O) Meter(instrumentationName string, opts ...metric.MeterOption) metric.Meter {
	if o == nil || o.meterProvider == nil {
		return noopm.NewMeterProvider().Meter(instrumentationName, opts...)
	}
	return o.meterProvider.Meter(instrumentationName, opts...)
}

func (o *O) ForService(ctx context.Context, serviceNamespace, serviceName string) O {
	if o == nil || o.system == nil {
		return O{
			serviceNamespace: serviceNamespace,
			serviceName:      serviceName,
			serviceVersion:   version.Get(),
		}
	}
	return o.system.ForService(ctx, serviceNamespace, serviceName)
}

func (o *O) TraceIDStyle() TraceIDStyle {
	if o == nil || o.system == nil {
		return PlainIDStyle
	}
	return o.system.idStyle
}

func (o *O) TextMapPropagator() propagation.TextMapPropagator {
	if o == nil || o.system == nil {
		return defaultTextMapPropagator
	}
	return o.textMapPropagator
}
