// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// Session sends SQL over the simple protocol, as psql does, so a statement's parameters are typed by the server.
type Session interface {
	// Exec runs sql (one or more statements) and reads every result. The first error, from the connection or
	// from any statement, is returned.
	Exec(ctx context.Context, sql string) ([]*pgconn.Result, error)
}

// NewSession returns a Session on pc. The caller owns pc.
func NewSession(pc *pgconn.PgConn) Session { return pgSession{pc} }

type pgSession struct{ pc *pgconn.PgConn }

func (s pgSession) Exec(ctx context.Context, sql string) ([]*pgconn.Result, error) {
	res, err := s.pc.Exec(ctx, sql).ReadAll()
	if err != nil {
		return res, err
	}
	for _, r := range res {
		if r.Err != nil {
			return res, r.Err
		}
	}
	return res, nil
}

// errText is a one-line description of err; a server error gives its SQLSTATE and message.
func errText(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return fmt.Sprintf("%s: %s", pe.Code, pe.Message)
	}
	return firstLine(err.Error())
}

func isSQLState(err error, code string) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == code
}
