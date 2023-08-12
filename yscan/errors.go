package yscan

import "errors"

var (
	// ErrNoRows occurs when rows are expected but none are returned.
	ErrNoRows = errors.New("no rows in result set")
)
