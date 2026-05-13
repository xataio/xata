package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/elastic/go-concert/ctxtool"
	"github.com/elastic/go-concert/unison"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

type Session interface {
	ServeSQLSession(ctx context.Context) error
	BranchID() string
}

type session struct {
	tracer       trace.Tracer
	branch       string
	inboundConn  net.Conn
	outboundConn net.Conn
}

func New(tracer trace.Tracer, branch string, inboundConn, outboundConn net.Conn) Session {
	return &session{
		tracer:       tracer,
		branch:       branch,
		inboundConn:  inboundConn,
		outboundConn: outboundConn,
	}
}

func (s *session) BranchID() string { return s.branch }

func (s *session) ServeSQLSession(ctx context.Context) error {
	// Set the context that will be used during the `close` call.
	// This is needed to avoid race conditions when the context is cancelled
	// before or while `ServeSQLSession` sets up the context, overwriting `ctx`
	// variable in that context.
	closeCtx := ctx
	ctx, cancel := ctxtool.WithFunc(ctx, func() { s.close(closeCtx) })
	defer cancel()

	logger := log.Ctx(ctx).With().Str("branchID", s.branch).Logger()
	ctx = logger.WithContext(ctx)

	logger.Info().Msg("Start serving SQL session")
	defer logger.Info().Msg("End serving SQL session")

	tg := unison.TaskGroupWithCancel(ctx)
	tg.OnQuit = unison.StopAll
	tg.Go(func(ctx context.Context) error {
		defer cancel()
		logger := log.Ctx(ctx).With().Str("direction", "postgres -> client").Logger()
		ctx = logger.WithContext(ctx)

		err := copyLoop(ctx, s.branch, s.inboundConn, s.outboundConn)
		if err != nil {
			logger.Error().Err(err).Msg("Copy loop error")
		}
		return nil
	})
	tg.Go(func(ctx context.Context) error {
		defer cancel()
		logger := log.Ctx(ctx).With().Str("direction", "client -> postgres").Logger()
		ctx = logger.WithContext(ctx)

		err := copyLoop(ctx, s.branch, s.outboundConn, s.inboundConn)
		if err != nil {
			logger.Error().Err(err).Msg("Copy loop error")
		}
		return nil
	})
	tg.Wait()
	return nil
}

func (s *session) close(ctx context.Context) {
	if s.inboundConn != nil {
		if err := s.inboundConn.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("close inbound connection")
		}
	}
	if s.outboundConn != nil {
		if err := s.outboundConn.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("close outbound connection")
		}
	}
}

func copyLoop(ctx context.Context, branch string, to io.Writer, from io.Reader) error {
	_, err := io.Copy(to, from)
	if err != nil {
		if errors.Is(err, io.EOF) || isClosedConnError(err) {
			log.Ctx(ctx).Info().Msgf("connection from branch [%s] has been closed", branch)
			return nil
		}

		var netOpError *net.OpError
		if errors.As(err, &netOpError) {
			if netOpError.Op == "read" {
				if wrappedErr := errors.Unwrap(err); wrappedErr != nil {
					log.Ctx(ctx).Info().Err(wrappedErr).Msgf("wrapped error: %T", wrappedErr)
				}
			}
		}

		// return err
		return fmt.Errorf("copy loop: %+w [%T]", err, err)
	}
	return nil
}

func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, net.ErrClosed) {
		return true
	}

	// some error values are private e.g. poll.errNetClosed. These are normally
	// wrapped inside a net.OpError.
	// Unfortunately we need to test by string matching the error message.

	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		return false
	}
	if err = opErr.Unwrap(); err == nil {
		return false
	}

	errmsg := err.Error()
	if strings.Contains(errmsg, "use of closed network connection") {
		return true
	}
	if strings.Contains(errmsg, "reset by peer") {
		return true
	}

	return false
}
