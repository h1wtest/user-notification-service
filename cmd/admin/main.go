package main

import (
	"user-notification-service/internal/audit"
	"user-notification-service/internal/platform"
)

func main() { platform.Serve("admin", audit.New) }
