package twitter_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	twitterscraper "github.com/imperatrona/twitter-scraper"
)

func TestAuth(t *testing.T) {
	username := "Athena_WQST"

	var cookies []*http.Cookie
	cookieFile := filepath.Join("./", fmt.Sprintf("%s_twitter_cookies.json", username))
	f, _ := os.Open(cookieFile)
	json.NewDecoder(f).Decode(&cookies)

	scraper := twitterscraper.New()
	err := scraper.SetProxy("http://localhost:7890")
	if err != nil {
		panic("Failed to set proxy, " + err.Error())
	}

	scraper.SetCookies(cookies)
	if !scraper.IsLoggedIn() {
		panic("Invalid cookies")
	}

	println("Saving cookies for user", username)
	cookieFile0 := filepath.Join("./", fmt.Sprintf("%s_twitter_cookies0.json", username))
	cookies0 := scraper.GetCookies()

	data, err := json.Marshal(cookies0)
	if err != nil {
		panic("Failed to marshal cookies, " + err.Error())
	}

	if err = os.WriteFile(cookieFile0, data, 0644); err != nil {
		panic("Failed to save cookies, " + err.Error())
	}

	println("Successfully saved cookies in ", cookieFile0)
}
