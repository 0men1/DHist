package coinbaseadvanced

import (
	"log"
	"os"

	"github.com/coinbase/cdp-sdk/go/auth"
)

func generateJWT(method, path string) string {
	// Generate the JWT using the CDP SDK
	jwt, err := auth.GenerateJWT(auth.JwtOptions{
		// Value must be the full "organizations/{org_id}/apiKeys/{key_id}" string
		KeyID:         os.Getenv("COINBASE_APP_KEY_ID"),
		KeySecret:     os.Getenv("COINBASE_PRIVATE_KEY"),
		RequestMethod: method,
		RequestHost:   "api.coinbase.com",
		RequestPath:   path,
		ExpiresIn:     120,
	})

	if err != nil {
		log.Fatalf("error building jwt: %v", err)
	}

	return jwt
}
