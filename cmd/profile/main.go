package main

import (
	"user-notification-service/internal/platform"
	"user-notification-service/internal/profile"
)

func main() { platform.Serve("profile", profile.New) }
