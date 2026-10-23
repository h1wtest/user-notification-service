package main

import (
	"user-notification-service/internal/notification"
	"user-notification-service/internal/platform"
)

func main() { platform.Serve("notification", notification.Build) }
