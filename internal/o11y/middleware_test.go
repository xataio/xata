package o11y

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"xata/internal/api/clienthttpheaders"
)

func TestTracingMiddlewareClientHeadersContext(t *testing.T) {
	tests := map[string]struct {
		userAgent string
		xataAgent string
		want      *clienthttpheaders.ParsedHeaders
	}{
		"no headers": {
			want: &clienthttpheaders.ParsedHeaders{},
		},
		"user agent only": {
			userAgent: "curl/8.7.1",
			want:      &clienthttpheaders.ParsedHeaders{UserAgent: "curl/8.7.1"},
		},
		"xata agent only": {
			xataAgent: "client=@xata.io/api; version=0.1.0; service=cli",
			want: &clienthttpheaders.ParsedHeaders{
				XataAgent: clienthttpheaders.ParsedXataAgent{Client: "@xata.io/api", Version: "0.1.0", Service: "cli"},
			},
		},
		"user agent with invalid xata agent": {
			userAgent: "curl/8.7.1",
			xataAgent: "client=" + strings.Repeat("a", customHeadersMaxLength),
			want:      &clienthttpheaders.ParsedHeaders{UserAgent: "curl/8.7.1"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var got *clienthttpheaders.ParsedHeaders
			handler := newSpanMiddleware("test", nil, nil, PlainIDStyle)(func(c echo.Context) error {
				got = clienthttpheaders.FromContext(c.Request().Context())
				return c.NoContent(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.userAgent != "" {
				req.Header.Set("User-Agent", tt.userAgent)
			}
			if tt.xataAgent != "" {
				req.Header.Set(headerXAgent, tt.xataAgent)
			}

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			c.SetPath("/test")

			err := handler(c)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTracingMiddlewareRecordError(t *testing.T) {
	tests := map[string]struct {
		handler    echo.HandlerFunc
		wantEvents int
	}{
		"success": {
			handler: func(c echo.Context) error { return c.NoContent(http.StatusOK) },
		},
		"bad request": {
			handler: func(c echo.Context) error { return echo.NewHTTPError(http.StatusBadRequest, "invalid signature") },
		},
		"internal error": {
			handler:    func(c echo.Context) error { return echo.NewHTTPError(http.StatusInternalServerError) },
			wantEvents: 1,
		},
		"client gave up": {
			handler: func(echo.Context) error { return context.Canceled },
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			previous := otel.GetTracerProvider()
			otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
			t.Cleanup(func() { otel.SetTracerProvider(previous) })

			e := echo.New()
			c := e.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), httptest.NewRecorder())
			c.SetPath("/test")

			//nolint:errcheck // the span is what we assert on
			newSpanMiddleware("test", nil, nil, PlainIDStyle)(tt.handler)(c)

			spans := exporter.GetSpans()
			require.Len(t, spans, 1)
			// RecordError is the only thing adding events to these spans
			require.Len(t, spans[0].Events, tt.wantEvents)
		})
	}
}

func TestLoggerMiddlewareLogLevel(t *testing.T) {
	tests := map[string]struct {
		handler echo.HandlerFunc
		want    string
	}{
		"success": {
			handler: func(c echo.Context) error { return c.NoContent(http.StatusOK) },
			want:    "info",
		},
		"bad request": {
			handler: func(c echo.Context) error { return echo.NewHTTPError(http.StatusBadRequest, "invalid signature") },
			want:    "info",
		},
		"unauthorized": {
			handler: func(c echo.Context) error { return echo.NewHTTPError(http.StatusUnauthorized) },
			want:    "info",
		},
		"not found": {
			handler: func(c echo.Context) error { return echo.NewHTTPError(http.StatusNotFound) },
			want:    "info",
		},
		"internal error": {
			handler: func(c echo.Context) error { return echo.NewHTTPError(http.StatusInternalServerError) },
			want:    "error",
		},
		"unknown error defaults to 500": {
			handler: func(c echo.Context) error { return errors.New("broken") },
			want:    "error",
		},
		"error after the response was committed": {
			handler: func(c echo.Context) error {
				if err := c.NoContent(http.StatusOK); err != nil {
					return err
				}
				return errors.New("broken")
			},
			want: "error",
		},
		"client gave up": {
			handler: func(echo.Context) error { return context.Canceled },
			want:    "info",
		},
		"client gave up mid-handler": {
			handler: func(echo.Context) error { return fmt.Errorf("read rows: %w", context.Canceled) },
			want:    "info",
		},
		"deadline exceeded is ours": {
			handler: func(echo.Context) error { return context.DeadlineExceeded },
			want:    "error",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := zerolog.New(&buf)
			o := NewTestService(t)

			e := echo.New()
			c := e.NewContext(httptest.NewRequest(http.MethodPost, "/", nil), httptest.NewRecorder())
			c.SetPath("/test")

			// the metrics middleware sits in between in SetupRouter, and used to swallow the error
			err := LoggerMiddleware(&logger)(MetricsMiddleware(&o)(tt.handler))(c)
			require.NoError(t, err)

			var got struct {
				Level   string `json:"level"`
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
			require.Equal(t, "http api request", got.Message)
			require.Equal(t, tt.want, got.Level)
		})
	}
}

func TestLoggerMiddlewareRemoteIP(t *testing.T) {
	tests := map[string]struct {
		header string
	}{
		"direct request": {},
		"spoofed real IP": {
			header: echo.HeaderXRealIP,
		},
		"spoofed forwarded IP": {
			header: echo.HeaderXForwardedFor,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := zerolog.New(&buf)
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = "192.0.2.10:1234"
			if tt.header != "" {
				req.Header.Set(tt.header, "198.51.100.77")
			}
			c := echo.New().NewContext(req, httptest.NewRecorder())
			c.SetPath("/test")

			err := LoggerMiddleware(&logger)(func(c echo.Context) error {
				return c.NoContent(http.StatusOK)
			})(c)
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
			require.Equal(t, "192.0.2.10", got[keyLogNetRemoteIP])
		})
	}
}

func TestMetricsMiddlewareReturnsHandlerError(t *testing.T) {
	o := NewTestService(t)
	want := errors.New("broken")

	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), httptest.NewRecorder())

	got := MetricsMiddleware(&o)(func(echo.Context) error { return want })(c)
	require.ErrorIs(t, got, want)
}

// hijackableWriter is what net/http hands a server: a ResponseWriter that can also be hijacked
// for a protocol upgrade and flushed.
type hijackableWriter struct {
	http.ResponseWriter
	hijacked bool
}

func (w *hijackableWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	return nil, nil, nil
}

func (w *hijackableWriter) Flush() {}

// TestMetricsMiddlewareKeepsWriterInterfaces pins the websocket path: the middleware swaps the
// writer echo holds for the one otelhttp wraps around it, and an upgrade needs Hijack to survive
// that swap.
func TestMetricsMiddlewareKeepsWriterInterfaces(t *testing.T) {
	o := NewTestService(t)
	inner := &hijackableWriter{ResponseWriter: httptest.NewRecorder()}

	e := echo.New()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), inner)

	var gotHijacker, gotFlusher bool
	handler := func(c echo.Context) error {
		_, gotHijacker = c.Response().Writer.(http.Hijacker)
		_, gotFlusher = c.Response().Writer.(http.Flusher)

		_, _, err := c.Response().Hijack()
		return err
	}

	require.NoError(t, MetricsMiddleware(&o)(handler)(c))
	require.True(t, gotHijacker, "the writer otelhttp wraps must stay hijackable")
	require.True(t, gotFlusher, "the writer otelhttp wraps must stay flushable")
	require.True(t, inner.hijacked, "Hijack must reach the writer underneath")
	require.Same(t, http.ResponseWriter(inner), c.Response().Writer, "the original writer must be restored")
}

func TestMetricsMiddlewareRecordsResponseStatus(t *testing.T) {
	tests := map[string]int64{
		"200": http.StatusOK,
		"400": http.StatusBadRequest,
		"500": http.StatusInternalServerError,
	}

	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			o := O{meterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}

			e := echo.New()
			c := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), httptest.NewRecorder())
			c.SetPath("/test")

			handler := func(c echo.Context) error { return c.NoContent(int(want)) }
			require.NoError(t, MetricsMiddleware(&o)(handler)(c))

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &rm))
			require.Contains(t, recordedStatusCodes(rm), want)
		})
	}
}

// recordedStatusCodes collects the status code otelhttp attributed to every data point it recorded.
func recordedStatusCodes(rm metricdata.ResourceMetrics) []int64 {
	var out []int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range hist.DataPoints {
				if status, ok := dp.Attributes.Value("http.response.status_code"); ok {
					out = append(out, status.AsInt64())
				}
			}
		}
	}
	return out
}
