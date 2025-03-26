package twitter

import (
	"context"
	"time"

	twitterscraper "github.com/imperatrona/twitter-scraper"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	data_types "github.com/masa-finance/masa-oracle/pkg/workers/types"
)

type TweetResult struct {
	Tweet *twitterscraper.Tweet
	Error error
}
type SimpleTweetResult struct {
	Tweet *SimpleTweet
	Error error
}
type SimpleTweet struct {
	Hashtags   []string
	ID         string
	Name       string
	Text       string
	TimeParsed time.Time
	Timestamp  int64
	UserID     string
	Username   string
}

func SimplifyTweetResult(full []*TweetResult) []*SimpleTweetResult {
	var simple []*SimpleTweetResult
	for _, f := range full {
		simple = append(simple, &SimpleTweetResult{
			Error: f.Error,
			Tweet: &SimpleTweet{
				Hashtags:   f.Tweet.Hashtags,
				ID:         f.Tweet.ID,
				Name:       f.Tweet.Name,
				Text:       f.Tweet.Text,
				TimeParsed: f.Tweet.TimeParsed,
				Timestamp:  f.Tweet.Timestamp,
				UserID:     f.Tweet.UserID,
				Username:   f.Tweet.Username,
			},
		})
	}
	return simple
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

func ScrapeTweetsByQueryWithRetry(query string, count int) ([]*TweetResult, *data_types.LoginEvent, error) {
	tried := make(map[string]bool)
	for {
		scraper, account, loginEvent, err := getAuthenticatedScraper()
		if err != nil {
			continue
		}
		if _, exists := tried[account.Username]; exists {
			return nil, nil, errors.Errorf("all accounts fail to fetch")
		}

		tried[account.Username] = true

		var tweets []*TweetResult
		ctx := context.Background()
		scraper.SetSearchMode(twitterscraper.SearchLatest)
		hasErr := false
		for tweet := range scraper.SearchTweets(ctx, query, count) {
			if tweet.Error != nil {
				handleRateLimit(tweet.Error, account)
				hasErr = true
				break
			}
			logrus.Info(tweet.Timestamp)
			tweets = append(tweets, &TweetResult{Tweet: &tweet.Tweet})
		}
		if !hasErr {
			return tweets, loginEvent, nil
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func ScrapeTweetsByQueryByAccountsRound(query string, count int, cursor string) ([]*TweetResult, *data_types.LoginEvent, string, error) {
	var tweets []*TweetResult
	l1 := 0
	batchSize := 1000
	totalAccounts := getAccountsCount()
	i := 0
	for {
		i++
		logrus.Infof("i %d total accounts %d", i, totalAccounts)
		if i > totalAccounts {
			return nil, nil, cursor, errors.Errorf("all accounts fail to fetch")
		}
		scraper, account, _, err := getAuthenticatedScraper()
		if err != nil {
			logrus.Error(err)
			continue
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
	return tweets, nil, cursor, nil
}
