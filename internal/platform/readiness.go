package platform

import (
	"context"
	"errors"
	"net/http"
)

func (c *Client) DependencyReady(ctx context.Context, base string) error {
	request, err := http.NewRequestWithContext(ctx, "GET", base+"/health/ready", nil)
	if err != nil {
		return errors.New("dependency configuration invalid")
	}
	if request.URL.Scheme != "https" {
		return errors.New("dependency HTTPS required")
	}
	response, err := c.HTTP.Do(request)
	if err != nil {
		return errors.New("dependency unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return errors.New("dependency not ready")
	}
	return nil
}
