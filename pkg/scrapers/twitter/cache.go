package twitter

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	data_types "github.com/masa-finance/masa-oracle/pkg/workers/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// 对于非trending中的，只查第一个请求并缓存，后面的都返回缓存中的，不管多少。
// 对于trending中的，每10分钟各处理N个。N根据机器在不超时情况下根据处理能力决定。

const USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
const BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

type QueryMeta struct {
	Cursor string `json:"cursor"`
}
type TwitterCacher struct {
	lock       sync.Mutex
	lockMap    map[string]*sync.Mutex
	httpClient *http.Client
	trending   map[string]bool

	rdb *redis.Client
}

func NewTwitterCacher(rdb *redis.Client) *TwitterCacher {
	httpClient := &http.Client{}
	cache := TwitterCacher{
		httpClient: httpClient,
		lockMap:    make(map[string]*sync.Mutex),
		trending:   make(map[string]bool),
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

		keywords := trendingQueries[:10]
		logrus.Infof("top 10 trending queries: %v", keywords)
		keywords = append(keywords, "crypto", "btc", "eth")
		for _, keyword := range keywords {
			c.trending[keyword] = true

			lock := c.getLock(keyword)
			if lock.TryLock() {
				c.Fetch(keyword, 1000)
				lock.Unlock()
			} else {
				// no need to cache since other goroutine is fetching.
				logrus.Infof("other goroutine is fetching %s", keyword)
			}
		}
		elapsed := time.Since(start)

		logrus.Infof("cache takes: %s\n", elapsed)

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

func (c *TwitterCacher) getCacheTweets(key string) ([]*TweetResult, error) {
	exists, err := c.rdb.Exists(context.Background(), key).Result()
	if err != nil {
		return nil, errors.Errorf("[%s] check exist error: %v", key, err)
	} else {
		if exists > 0 {
			result, err := c.rdb.Get(context.Background(), key).Result()
			if err != nil {
				return nil, errors.Errorf("[%s] get error: %v", key, err)
			} else {
				var tweets []*TweetResult
				err = json.Unmarshal([]byte(result), &tweets)
				if err != nil {
					return nil, errors.Errorf("[%s] unmarshal error: %v", key, err)
				} else {
					logrus.Infof("[%s] hit cache", key)
					return tweets, nil
				}
			}
		} else {
			return nil, nil
		}
	}

}

func (c *TwitterCacher) cache(key string, tweets []*TweetResult) error {
	bytes, err := json.Marshal(tweets)
	if err != nil {
		return errors.Errorf("[%s] marshal failed: %v", key, err)
	} else {
		err = c.rdb.Set(context.Background(), key, bytes, 24*time.Hour).Err()
		if err != nil {
			return errors.Errorf("[%s] update cache failed: %v", key, err)
		} else {
			logrus.Info("update cache for query: ", key)
		}
	}
	return nil
}
func (c *TwitterCacher) metaKey(query string) string {
	return fmt.Sprintf("%s-meta", query)
}
func (c *TwitterCacher) cacheMeta(query string, cursor string) error {
	key := c.metaKey(query)
	bytes, err := json.Marshal(QueryMeta{
		Cursor: cursor,
	})
	if err != nil {
		return errors.Errorf("[%s] marshal failed: %v", key, err)
	}

	err = c.rdb.Set(context.Background(), key, bytes, 0).Err()
	if err != nil {
		return errors.Errorf("[%s] update cache failed: %v", key, err)
	}
	return nil
}

func (c *TwitterCacher) getMeta(query string) (string, error) {
	key := c.metaKey(query)
	exists, err := c.rdb.Exists(context.Background(), key).Result()
	if err != nil {
		return "", errors.Errorf("[%s] check exist error: %v", key, err)
	} else {
		if exists > 0 {
			result, err := c.rdb.Get(context.Background(), key).Result()
			if err != nil {
				return "", errors.Errorf("[%s] get error: %v", key, err)
			} else {
				var meta QueryMeta
				err = json.Unmarshal([]byte(result), &meta)
				if err != nil {
					return "", errors.Errorf("[%s] unmarshal error: %v", key, err)
				} else {
					return meta.Cursor, nil
				}
			}
		} else {
			return "", nil
		}
	}

}

func (c *TwitterCacher) getLock(key string) *sync.Mutex {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, exists := c.lockMap[key]; !exists {
		c.lockMap[key] = &sync.Mutex{}
	}
	return c.lockMap[key]
}

func (c *TwitterCacher) validate(query string, array []*TweetResult) {
	exists := make(map[string]bool)
	for i, a := range array {
		if _, exist := exists[a.Tweet.ID]; exist {
			logrus.Errorf("[%s] duplicate tweet id: %s", query, a.Tweet.ID)
		}
		exists[a.Tweet.ID] = true

		if i > 0 {
			if array[i].Tweet.Timestamp > array[i-1].Tweet.Timestamp {
				logrus.Errorf("[%s] %s ts %d > previous %s ts %d",
					query,
					array[i].Tweet.ID,
					array[i].Tweet.Timestamp,
					array[i-1].Tweet.ID,
					array[i-1].Tweet.Timestamp,
				)
			}
		}
	}
}

func (c *TwitterCacher) yesterday() int64 {
	now := time.Now().UTC()
	startOfToday := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	startOfYesterday := startOfToday.AddDate(0, 0, -1)
	return startOfYesterday.Unix()
}

func (c *TwitterCacher) removeExpire(array []*TweetResult, deadline int64) []*TweetResult {
	for i := len(array) - 1; i >= 0; i-- {
		if array[i].Tweet.Timestamp > deadline {
			logrus.Infof("remove expired: %d", len(array)-1-i)
			return array[:i+1]
		}
	}
	return array
}

func (c *TwitterCacher) GetTweets(query string, count int) ([]*TweetResult, *data_types.LoginEvent, error) {
	if c.trending[query] {
		// 如果是trending中的，拿得到锁就每次多取些，拿不到就返回缓存的。
		return c.Fetch(query, 1000)
	} else {
		// 非trending的关键字,不参与打分，只返回第一次缓存值。
		lock := c.getLock(query)
		lock.Lock()
		defer lock.Unlock()

		existingTweets, err := c.getCacheTweets(query)
		if err != nil {
			return nil, nil, err
		}
		if existingTweets == nil {
			result, loginEvent, _, err := ScrapeTweetsByQueryByAccountsRound(query, min(count, 1000), "")
			c.cache(query, result)
			return result, loginEvent, err
		}
		return existingTweets, nil, nil
	}
}

func (c *TwitterCacher) Fetch(query string, max int) ([]*TweetResult, *data_types.LoginEvent, error) {
	updateCache := true
	existingTweets, err := c.getCacheTweets(query)
	if err != nil {
		logrus.Errorf("[%s] get failed: %v", query, err)
		updateCache = false
		// 不返回错误，假设没有缓存，并取max返回。
	}

	lock := c.getLock(query)
	if !lock.TryLock() {
		return existingTweets, nil, nil
	}
	defer lock.Unlock()

	accumulated := 0
	var allTweets []*TweetResult
	if len(existingTweets) > 0 {
		latestCachedTs := existingTweets[0].Tweet.Timestamp
		oldestCachedTs := existingTweets[len(existingTweets)-1].Tweet.Timestamp
		logrus.Infof("[%s] cached: %d ts: %d->%d", query, len(existingTweets), oldestCachedTs, latestCachedTs)

		cursor := ""
		var latestTweets []*TweetResult
		start := time.Now()
		for {
			// 每次取N个，目前不确定获取频率，应该不会刷新很多新的。 N 可能和query热度有关。
			batch := 100
			latestTweets, _, cursor, err = ScrapeTweetsByQueryByAccountsRound(query, 100, cursor)
			if err != nil {
				logrus.Errorf("[%s] scrape error: %v", query, err)
				break
			}
			accumulated += batch
			if len(latestTweets) <= 0 {
				// 应该不会到这里,总会和existing交叉才对
				break
			}

			latestTs := latestTweets[0].Tweet.Timestamp
			oldestTs := latestTweets[len(latestTweets)-1].Tweet.Timestamp
			if latestTs <= latestCachedTs {
				logrus.Infof("[%s] No new tweets found: new latest %d <= cached latest %d", query, latestTs, latestCachedTs)
				break
			}
			if oldestTs <= latestCachedTs {
				logrus.Infof("[%s] new oldest ts %d <= latest cached ts %d", query, oldestTs, latestCachedTs)
				// 有交叉，补全了
				for _, t := range latestTweets {
					if t.Tweet.Timestamp > latestCachedTs {
						allTweets = append(allTweets, t)
					} else {
						break
					}
				}
				break
			} else {
				allTweets = append(allTweets, latestTweets...)
			}
		}
		logrus.Infof("[%s] preappend takes: %s", query, time.Since(start))
		allTweets = append(allTweets, existingTweets...)
	}

	yesterday := c.yesterday()
	logrus.Infof("[%s] yesterday: %d", query, yesterday)

	finalProcess := func(tweets []*TweetResult, deadline int64) []*TweetResult {
		finalTweets := c.removeExpire(tweets, deadline)
		if updateCache {
			err = c.cache(query, finalTweets)
			if err != nil {
				logrus.Errorf("[%s] cache failed: %v", query, err)
			}
		} else {
			logrus.Infof("[%s] won't update cache", query)
		}
		c.validate(query, finalTweets)
		// TODO: remove useless field
		logrus.Infof("[%s] len: %d, first: %s %d, last: %s %d",
			query,
			len(finalTweets),
			finalTweets[0].Tweet.ID,
			finalTweets[0].Tweet.Timestamp,
			finalTweets[len(finalTweets)-1].Tweet.ID,
			finalTweets[len(finalTweets)-1].Tweet.Timestamp,
		)
		return finalTweets
	}

	if accumulated >= max {
		logrus.Infof("[%s] accumulated %d, no need to append more", query, accumulated)
		return finalProcess(allTweets, yesterday), nil, nil
	}

	if len(allTweets) > 0 && allTweets[len(allTweets)-1].Tweet.Timestamp <= yesterday {
		// 没必要从尾巴继续取了，超过一天了。
		logrus.Infof("[%s] last %d <= yesterday %d no need to append more", query, allTweets[len(allTweets)-1].Tweet.Timestamp, yesterday)
		return finalProcess(allTweets, yesterday), nil, nil
	}

	// 从尾巴上继续获取更多
	cursor, err := c.getMeta(query)
	if err != nil {
		logrus.Errorf("[%s] get meta failed: %v", query, err)
		if len(allTweets) > 0 {
			// 获取Cursor失败，直接cache并返回。
			return finalProcess(allTweets, yesterday), nil, nil
		}
	}
	for {
		logrus.Infof("[%s] get %d, cursor: %s", query, min(1000, max), cursor)
		var tweets []*TweetResult
		tweets, _, cursor, err = ScrapeTweetsByQueryByAccountsRound(query, min(1000, max), cursor)
		if err != nil {
			logrus.Errorf("[%s] scrape error: %v", query, err)
			err = c.cacheMeta(query, "")
			if err != nil {
				logrus.Errorf("[%s-meta] cache failed: %v", query, err)
			}
			break
		}
		accumulated += len(tweets)
		expire := false
		for _, t := range tweets {
			if t.Tweet.Timestamp > yesterday {
				allTweets = append(allTweets, t)
			} else {
				logrus.Infof("[%s] fetch stops: %d > yesterday %d", query, t.Tweet.Timestamp, c.yesterday())
				expire = true
				break
			}
		}
		if expire {
			err = c.cacheMeta(query, "")
			if err != nil {
				logrus.Errorf("[%s-meta] cache failed: %v", query, err)
			}
			break
		}
		if accumulated >= max {
			err = c.cacheMeta(query, cursor)
			if err != nil {
				logrus.Errorf("[%s-meta] cache failed: %v", query, err)
			}
			break
		}
	}
	if len(allTweets) > 0 {
		return finalProcess(allTweets, yesterday), nil, nil
	}
	return allTweets, nil, nil
}
