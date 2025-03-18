package twitter

import (
	"fmt"
	"time"

	twitterscraper "github.com/imperatrona/twitter-scraper"

	data_types "github.com/masa-finance/masa-oracle/pkg/workers/types"

	"github.com/sirupsen/logrus"
)

func ScrapeFollowersForProfile(username string, count int) ([]*twitterscraper.Profile, *data_types.LoginEvent, error) {
	scraper, account, loginEvent, err := getAuthenticatedScraper()
	if err != nil {
		return nil, loginEvent, err
	}

	logrus.Error("ScrapeFollowersForProfile", username, count)

	cursor := ""
	profiles := make([]*twitterscraper.Profile, 0)

	for {
		followingResponse, cursor, err := scraper.FetchFollowers(username, count, cursor)
		if err != nil {
			if handleRateLimit(err, account) {
				return nil, loginEvent, fmt.Errorf("rate limited")
			}
			logrus.Errorf("Error fetching followers: %v", err.Error())
			return nil, loginEvent, fmt.Errorf("%v", err.Error())
		}
		profiles = append(profiles, followingResponse...)
		if cursor == "" || len(profiles) >= count {
			break
		}
	}
	account.LastScraped = time.Now()
	return profiles[:count], loginEvent, nil
}
