package twitter

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	data_types "github.com/masa-finance/masa-oracle/pkg/workers/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
const BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

type TwitterCacher struct {
	lock       sync.Mutex
	lockMap    map[string]*sync.Mutex
	httpClient *http.Client

	rdb *redis.Client
}

func NewTwitterCacher(rdb *redis.Client) *TwitterCacher {
	httpClient := &http.Client{
		Timeout: 20 * time.Second,
	}
	cache := TwitterCacher{
		httpClient: httpClient,
		lockMap:    make(map[string]*sync.Mutex),
		rdb:        rdb,
	}
	return &cache
}

func (c *TwitterCacher) Start(ctx context.Context) {
	logrus.Info("twitter cacher started")

	for {
		start := time.Now()
		trendingQueries, err := c.getTrendingQueries()
		if err != nil {
			logrus.Errorf("failed to get trending queries: %v", err)
			continue
		}
		keywords := trendingQueries[:1]
		logrus.Infof("top 10 trending queries: %v", keywords)
		keywords = append(keywords, "crypto", "btc", "eth")
		for _, keyword := range keywords {
			lock := c.getLock(keyword)
			if lock.TryLock() {
				tweets, _, err := ScrapeTweetsByQueryByAccountsRound(fmt.Sprintf(`"%s"`, strings.TrimSpace(keyword)), 100)
				if err != nil {
					logrus.Errorf("cache %s failed: %v", keyword, err)
					lock.Unlock()
					continue
				}
				// c.cachedMap[keyword] = tweets
				if err = c.cache(keyword, tweets); err != nil {
					logrus.Error(err)
				}
				lock.Unlock()
			} else {
				// no need to cache since other goroutine is fetching.
				logrus.Infof("other goroutine is fetching %s", keyword)
			}
		}
		elapsed := time.Since(start)

		logrus.Infof("cache takes: %s\n", elapsed)
		// logrus.Info("cached keys: ", len(c.cachedMap))
		// TODO: need a strategy to remove old keyword

		select {
		case <-time.After(10 * time.Minute):
		case <-ctx.Done():
			logrus.Info("twitter cacher stopped")
			return
		}
	}
}

func (c *TwitterCacher) getTrendingQueries() ([]string, error) {
	guestToken, err := c.getGuestToken()
	if err != nil {
		return nil, errors.Errorf("failed to get guest token: %v", err)
	}

	trending_topics_url := "https://api.x.com/1.1/trends/place.json?id=23424977"
	req, err := http.NewRequest("GET", trending_topics_url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+BEARER_TOKEN)
	req.Header.Set("User-Agent", USER_AGENT)
	req.Header.Set("X-Guest-Token", guestToken)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-GB,en-US;q=0.9,en;q=0.8")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br, zstd")
	req.Header.Set("Referer", "https://x.com/")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://x.com")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-site")
	req.Header.Set("Sec-Ch-Ua", `"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"`)
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", "macOS")
	req.Header.Set("X-Twitter-Active-User", "yes")
	req.Header.Set("X-Twitter-Client-Language", "en-GB")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respContentEncoding := resp.Header.Get("Content-Encoding")
	var reader io.ReadCloser
	switch respContentEncoding {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	type Trend struct {
		Name            string `json:"name"`
		Url             string `json:"url"`
		PromotedContent string `json:"promoted_content"`
		Query           string `json:"query"`
		TweetVolume     *int   `json:"tweet_volume"`
	}
	type TrendingResponse struct {
		Trends []Trend `json:"trends"`
	}
	var trendingResp []TrendingResponse
	err = json.Unmarshal(body, &trendingResp)
	if err != nil {
		return nil, err
	}
	trends := trendingResp[0].Trends
	sort.Slice(trends, func(i, j int) bool {
		a := 0
		b := 0
		if trends[i].TweetVolume != nil {
			a = *trends[i].TweetVolume
		}
		if trends[j].TweetVolume != nil {
			b = *trends[j].TweetVolume
		}
		return a > b
	})
	var trendings []string
	for _, trend := range trends {
		if trend.TweetVolume != nil && *trend.TweetVolume > 0 {
			trendings = append(trendings, trend.Query)
		}
	}
	return trendings, nil
}

func (c *TwitterCacher) getGuestToken() (string, error) {
	guest_token_url := "https://api.twitter.com/1.1/guest/activate.json"
	req, err := http.NewRequest("POST", guest_token_url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("User-Agent", USER_AGENT)
	req.Header.Set("Authorization", "Bearer "+BEARER_TOKEN)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	type GuestTokenResponse struct {
		GuestToken string `json:"guest_token"`
	}
	var guestTokenResp GuestTokenResponse
	err = json.Unmarshal(body, &guestTokenResp)
	if err != nil {
		return "", err
	}
	return guestTokenResp.GuestToken, nil
}

func (c *TwitterCacher) GetTweets(query string, count int) ([]*TweetResult, *data_types.LoginEvent, error) {
	logrus.Infof("Get Tweets query: %s count: %d", query, count)

	lock := c.getLock(query)
	lock.Lock()
	defer lock.Unlock()

	exists, err := c.rdb.Exists(context.Background(), query).Result()
	if err != nil {
		logrus.Errorf("[%s] check exist error: %v", query, err)
	} else {
		if exists > 0 {
			result, err := c.rdb.Get(context.Background(), query).Result()
			if err != nil {
				logrus.Errorf("[%s] get error: %v", query, err)
			} else {
				var tweets []*TweetResult
				err = json.Unmarshal([]byte(result), &tweets)
				if err != nil {
					logrus.Errorf("[%s] unmarshal error: %v", query, err)
				} else {
					logrus.Infof("[%s] hit cache", query)
					if len(tweets) >= count {
						return tweets, nil, nil
					}
				}
			}
		}
	}
	// if value := c.cachedMap[query]; value != nil {
	// 	if len(value) >= count {
	// 		logrus.Info("hit cache for query: ", query)
	// 		return value[:count], nil, nil
	// 	} else {
	// 		logrus.Infof("cache %s len: %d, request count: %d", query, len(value), count)
	// 	}
	// }

	// TODO: quicker if cache cursor as well
	tweets, loginEvent, err := ScrapeTweetsByQueryByAccountsRound(query, count)
	if err != nil {
		return nil, nil, err
	}

	if err = c.cache(query, tweets); err != nil {
		logrus.Error(err)
	}
	// c.cachedMap[query] = tweets

	return tweets, loginEvent, nil
}

func (c *TwitterCacher) cache(key string, tweets []*TweetResult) error {
	bytes, err := json.Marshal(tweets)
	if err != nil {
		return errors.Errorf("[%s] marshal failed: %v", key, err)
	} else {
		err = c.rdb.Set(context.Background(), key, bytes, 10*time.Minute).Err()
		if err != nil {
			return errors.Errorf("[%s] update cache failed: %v", key, err)
		} else {
			logrus.Info("update cache for query: ", key)
		}
	}
	return nil
}

func (c *TwitterCacher) getLock(key string) *sync.Mutex {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, exists := c.lockMap[key]; !exists {
		c.lockMap[key] = &sync.Mutex{}
	}
	return c.lockMap[key]
}
