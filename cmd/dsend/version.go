package main

// Build metadata, injected at link time via -ldflags. See the Makefile.
var (
	Version   = "dev"
	Commit    = "none"
	BuildTime = "unknown"
)
