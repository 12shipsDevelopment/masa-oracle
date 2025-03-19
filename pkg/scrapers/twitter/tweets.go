package twitter

import (
	"context"

	twitterscraper "github.com/imperatrona/twitter-scraper"
	"github.com/sirupsen/logrus"

	data_types "github.com/masa-finance/masa-oracle/pkg/workers/types"
)

type TweetResult struct {
	Tweet *twitterscraper.Tweet
	Error error
}

func ScrapeTweetByID(id string) (*twitterscraper.Tweet, *data_types.LoginEvent, error) {
	scraper, account, loginEvent, err := getAuthenticatedScraper()
	if err != nil {
		return nil, loginEvent, err
	}

	tweet, err := scraper.GetTweet(id)
	if err != nil {
		if handleRateLimit(err, account) {
			return nil, loginEvent, err
		}
		return nil, loginEvent, err
	}
	return tweet, loginEvent, nil
}

func ScrapeTweetsByQuery(query string, count int) ([]*TweetResult, *data_types.LoginEvent, error) {
	scraper, account, loginEvent, err := getAuthenticatedScraper()
	if err != nil {
		return nil, loginEvent, err
	}

	var tweets []*TweetResult
	ctx := context.Background()
	scraper.SetSearchMode(twitterscraper.SearchLatest)
	for tweet := range scraper.SearchTweets(ctx, query, count) {
		if tweet.Error != nil {
			if handleRateLimit(tweet.Error, account) {
				return nil, loginEvent, tweet.Error
			}
			return nil, loginEvent, tweet.Error
		}
		tweets = append(tweets, &TweetResult{Tweet: &tweet.Tweet})
	}
	return tweets, loginEvent, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func ScrapeTweetsByQueryByAccountsRound(query string, count int) ([]*TweetResult, *data_types.LoginEvent, error) {
	var tweets []*TweetResult
	cursor := ""
	l1 := 0
	batchSize := 20
	for {
		scraper, account, loginEvent, err := getAuthenticatedScraper()
		if err != nil {
			return nil, loginEvent, err
		}

		ctx := context.Background()
		scraper.SetSearchMode(twitterscraper.SearchLatest)

		l1 = len(tweets)
		for tweet := range scraper.SearchTweetsWithCursor(ctx, query, cursor, min(count-l1, batchSize)) {
			if tweet.Error != nil {
				if handleRateLimit(tweet.Error, account) {
					logrus.Errorf("%s search tweet %s error: exceed rate limit", account.Username, query)
				} else {
					logrus.Errorf("%s search tweet %s error: ", account.Username, query)
				}
				break
			}

			tweets = append(tweets, &TweetResult{Tweet: &tweet.Tweet})
			cursor = tweet.Cursor
		}
		curLen := len(tweets)
		if curLen-l1 > 0 {
			logrus.Infof("%s fetch %d tweets, last tweet: %s, next cursor: %s", account.Username, curLen-l1, tweets[curLen-1].Tweet.ID, cursor)
		}
		if len(tweets) >= count {
			break
		}
	}
	return tweets[:count], nil, nil
}
