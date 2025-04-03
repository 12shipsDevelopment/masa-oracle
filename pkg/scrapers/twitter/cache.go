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

const INTERVAL_FETCH = 10
const FETCH_MAX_TASK = 1000
const FETCH_PER_ROUND = 1000
const FETCH_FOR_PREAPPEND = 200
const TRENDING_COUNT = 10

const CURSOR_END = "CURSOR_END"

// 对于非trending中的，只查第一个请求并缓存，后面的都返回缓存中的，不管多少。
// 对于trending中的，每10分钟各处理N个。N根据机器在不超时情况下根据处理能力决定。

// key: <query>-meta
type QueryMeta struct {
	Cursor string `json:"cursor"`
}

type TwitterCacher struct {
	lock       sync.Mutex
	lockMap    map[string]*sync.Mutex
	httpClient *http.Client

	// TODO: 需要在redis中么？
	allTrendings map[string]bool

	rdb *redis.Client

	onlyReadCache    bool
	clearExpireCache bool
	fetchInterval    int
	fetchMaxPerTask  int
	fetchPerRound    int
	fetchPreappend   int
}

func NewTwitterCacher(
	rdb *redis.Client,
	onlyReadCache bool,
	clearExpireCache bool,
	fetch_interval int,
	fetch_max_per_task int,
	fetch_per_round int,
	fetch_preappend int,
) *TwitterCacher {
	httpClient := &http.Client{}
	fetchInterval := INTERVAL_FETCH
	if fetch_interval > 0 {
		fetchInterval = fetch_interval
	}
	fetchMaxPerTask := FETCH_MAX_TASK
	if fetch_max_per_task > 0 {
		fetchMaxPerTask = fetch_max_per_task
	}
	fetchPerRound := FETCH_PER_ROUND
	if fetch_per_round > 0 {
		fetchPerRound = fetch_per_round
	}
	fetchPreappend := FETCH_FOR_PREAPPEND
	if fetch_preappend > 0 {
		fetchPreappend = fetch_preappend
	}
	logrus.Infof("cache param: fetchInterval=%d, fetchMaxPerTask=%d, fetchPerRound=%d, fetchPreappend=%d",
		fetchInterval,
		fetchMaxPerTask,
		fetchPerRound,
		fetchPreappend,
	)
	cache := TwitterCacher{
		httpClient:       httpClient,
		lockMap:          make(map[string]*sync.Mutex),
		allTrendings:     make(map[string]bool),
		rdb:              rdb,
		onlyReadCache:    onlyReadCache,
		clearExpireCache: clearExpireCache,
		fetchInterval:    fetchInterval,
		fetchMaxPerTask:  fetchMaxPerTask,
		fetchPerRound:    fetchPerRound,
		fetchPreappend:   fetchPreappend,
	}
	return &cache
}

func (c *TwitterCacher) Clear(ctx context.Context) {
	if c.clearExpireCache {
		for {
			logWithPrefix("twitter expiration cleaner started")
			start := time.Now()

			iter := c.rdb.Scan(ctx, 0, "*", 0).Iterator()
			for iter.Next(ctx) {
				key := iter.Val()
				if !strings.HasSuffix(key, "-meta") {
					lock := c.getLock(key)
					if lock.TryLock() {
						tweets, _ := c.getCacheTweets(key)
						if len(tweets) > 0 {
							newTweets := removeExpire(tweets, yesterday())
							if len(newTweets) < len(tweets) {
								if len(newTweets) <= 0 {
									_, err := c.rdb.Del(ctx, key).Result()
									if err == nil {
										logrus.Infof("[%s] del", key)
									}
									_, err = c.rdb.Del(ctx, c.metaKey(key)).Result()
									if err == nil {
										logrus.Infof("[%s] del", c.metaKey(key))
									}
								} else {
									if err := c.cacheTweets(key, newTweets); err == nil {
										logrus.Infof("[%s] rm %d expired", key, len(tweets)-len(newTweets))
									}
								}
							}
						}
						lock.Unlock()
					}
				}
			}
			elapsed := time.Since(start)
			logWithPrefix("expiration clean takes: %s\n", elapsed)

			select {
			case <-time.After(12 * time.Hour):
			case <-ctx.Done():
				logWithPrefix("twitter expiration cleaner stopped")
				return
			}
		}
	}
}

func (c *TwitterCacher) Start(ctx context.Context) {
	logWithPrefix("twitter cacher started")

	if !c.onlyReadCache {
		for {
			start := time.Now()

			keywords := c.getKeywords()
			logWithPrefix("start fetching another round: %v", keywords)
			for _, keyword := range keywords {
				// TODO: concurrency
				c.allTrendings[keyword] = true
				logWithPrefix("add %s to all trending", keyword)

				c.fetch(keyword, c.fetchPerRound, true)
			}

			elapsed := time.Since(start)
			logWithPrefix("this round takes: %s\n", elapsed)

			select {
			case <-time.After(time.Duration(c.fetchInterval) * time.Minute):
			case <-ctx.Done():
				logWithPrefix("twitter cacher stopped")
				return
			}
		}
	} else {
		logrus.Info("ONLY-READ-CACHE mode, won't fetch trending query periodly")
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

// for test, should disable in prod
func (c *TwitterCacher) validate(query string, array []*SimpleTweetResult) {
	// check duplication
	// check time order
	exists := make(map[string]bool)
	for i, a := range array {
		if _, exist := exists[a.Tweet.ID]; exist {
			logErrorWithPrefix("[%s] duplicate tweet id: %s", query, a.Tweet.ID)
		}
		exists[a.Tweet.ID] = true

		if i > 0 {
			if array[i].Tweet.Timestamp > array[i-1].Tweet.Timestamp {
				logErrorWithPrefix("[%s] %s ts %d > previous %s ts %d",
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

func yesterday() int64 {
	now := time.Now().UTC()
	startOfToday := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	startOfYesterday := startOfToday.AddDate(0, 0, -1)
	return startOfYesterday.Unix()
}

func removeDuplicates(array []*SimpleTweetResult) []*SimpleTweetResult {
	var newArray []*SimpleTweetResult
	exists := make(map[string]bool)
	duplicates := 0
	for _, t := range array {
		if _, exist := exists[t.Tweet.ID]; !exist {
			newArray = append(newArray, t)
			exists[t.Tweet.ID] = true
		} else {
			duplicates++
		}
	}
	logWithPrefix("remove total duplicates: %d", duplicates)
	return newArray
}

func removeExpire(array []*SimpleTweetResult, deadline int64) []*SimpleTweetResult {
	if array[0].Tweet.Timestamp <= deadline {
		return []*SimpleTweetResult{}
	}
	for i := len(array) - 1; i >= 0; i-- {
		logrus.Infof("%d %d", array[i].Tweet.Timestamp, deadline)
		if array[i].Tweet.Timestamp > deadline {
			logWithPrefix("remove expired: %d", len(array)-1-i)
			return array[:i+1]
		}
	}
	return array
}

func (c *TwitterCacher) GetTweets(query string, count int) ([]*SimpleTweetResult, *data_types.LoginEvent, error) {
	// if c.allTrendings[query] {
	// logWithPrefix("[%s] in trending", query)
	if c.onlyReadCache {
		existingTweets, err := c.getCacheTweets(query)
		if err == nil && existingTweets != nil && len(existingTweets) > 0 {
			logWithPrefix("[%s] only-read-cache, return cached %d ", query, len(existingTweets))
			return existingTweets, nil, nil
		}

		// lock := c.getLock(query)
		// if lock.TryLock() {
		// 	defer lock.Unlock()

		// 	result, loginEvent, _, err := ScrapeTweetsByQueryByAccountsRound(query, c.fetchPerRound, "")
		// 	if err != nil {
		// 		return nil, loginEvent, err
		// 	}
		// 	tweets := SimplifyTweetResult(result)
		// 	logWithPrefix("[%s] only-read-cache, return newly %d", query, len(tweets))
		// 	return tweets, loginEvent, err
		// }
		return []*SimpleTweetResult{}, nil, nil
	} else {
		return c.fetch(query, c.fetchMaxPerTask, false)
	}
	// } else {
	// 	logWithPrefix("[%s] not in trending", query)
	// 	// 非trending的关键字,不参与打分，只返回第一次缓存值。
	// 	lock := c.getLock(query)
	// 	lock.Lock()
	// 	defer lock.Unlock()

	// 	existingTweets, err := c.getCacheTweets(query)
	// 	if err != nil {
	// 		return nil, nil, err
	// 	}
	// 	if existingTweets == nil {
	// 		result, loginEvent, _, err := ScrapeTweetsByQueryByAccountsRound(query, min(count, FETCH_PER_ROUND), "")
	// 		if err != nil {
	// 			return nil, loginEvent, err
	// 		}
	// 		tweets := SimplifyTweetResult(result)
	// 		c.cacheTweets(query, tweets)
	// 		return tweets, loginEvent, err
	// 	}
	// 	return existingTweets, nil, nil
	// }
}

func (c *TwitterCacher) fetch(query string, max int, most bool) ([]*SimpleTweetResult, *data_types.LoginEvent, error) {
	logWithPrefix("[%s] advanced fetch: %d", query, max)
	updateCache := true
	existingTweets, err := c.getCacheTweets(query)
	if err != nil {
		logErrorWithPrefix("[%s] get failed: %v", query, err)
		updateCache = false // 取缓存失败，就不要替换原有的缓存
	}

	lock := c.getLock(query)
	if !lock.TryLock() {
		return existingTweets, nil, nil
	}
	defer lock.Unlock()

	accumulated := 0
	var allTweets []*SimpleTweetResult
	if len(existingTweets) > 0 {
		latestCachedTs := existingTweets[0].Tweet.Timestamp
		oldestCachedTs := existingTweets[len(existingTweets)-1].Tweet.Timestamp
		logWithPrefix("[%s] cached: %d ts: %d->%d", query, len(existingTweets), oldestCachedTs, latestCachedTs)

		cursor := ""
		start := time.Now()
		for {
			// TODO: 每次取N个，目前不确定获取频率，应该不会刷新很多新的。 N 可能和query热度有关。
			var result []*TweetResult
			result, _, cursor, err = ScrapeTweetsByQueryByAccountsRound(query, c.fetchPreappend, cursor)
			if err != nil {
				logErrorWithPrefix("[%s] scrape error: %v", query, err)
				break
			}
			latestTweets := SimplifyTweetResult(result)
			accumulated += c.fetchPreappend
			if len(latestTweets) <= 0 {
				// 应该不会到这里,总会和existing交叉才对
				break
			}

			latestTs := latestTweets[0].Tweet.Timestamp
			oldestTs := latestTweets[len(latestTweets)-1].Tweet.Timestamp
			if latestTs <= latestCachedTs {
				logWithPrefix("[%s] No new tweets found: new latest %d <= cached latest %d", query, latestTs, latestCachedTs)
				break
			}
			if oldestTs <= latestCachedTs {
				logWithPrefix("[%s] new oldest ts %d <= latest cached ts %d", query, oldestTs, latestCachedTs)
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
			if !most && accumulated >= max {
				// 允许漏掉一些
				break
			}
		}
		logWithPrefix("[%s] preappend takes: %s", query, time.Since(start))
		allTweets = append(allTweets, existingTweets...)
	}

	yesterday := yesterday()
	logWithPrefix("[%s] yesterday: %d", query, yesterday)

	finalProcess := func(tweets []*SimpleTweetResult, deadline int64) []*SimpleTweetResult {
		// remove cursor if fetches all
		if tweets[len(tweets)-1].Tweet.Timestamp < deadline {
			err = c.cacheMeta(query, CURSOR_END)
			if err != nil {
				// TODO: if cache fail, may have duplications
				logErrorWithPrefix("[%s-meta] rm cache cursor failed: %v", query, err)
			} else {
				logrus.Infof("[%s-meta] cursor removed", query)
			}
		}

		// 删除过期的
		finalTweets := removeExpire(tweets, deadline)
		finalTweets = removeDuplicates(finalTweets)
		if updateCache {
			err = c.cacheTweets(query, finalTweets)
			if err != nil {
				logErrorWithPrefix("[%s] cache failed: %v", query, err)
			}
		} else {
			logWithPrefix("[%s] won't update cache", query)
		}
		c.validate(query, finalTweets)
		logWithPrefix("[%s] len: %d, first: %s %d, last: %s %d",
			query,
			len(finalTweets),
			finalTweets[0].Tweet.ID,
			finalTweets[0].Tweet.Timestamp,
			finalTweets[len(finalTweets)-1].Tweet.ID,
			finalTweets[len(finalTweets)-1].Tweet.Timestamp,
		)
		return finalTweets
	}

	if !most && accumulated >= max {
		logWithPrefix("[%s] accumulated %d, no need to append more", query, accumulated)
		return finalProcess(allTweets, yesterday), nil, nil
	}

	if len(allTweets) > 0 && allTweets[len(allTweets)-1].Tweet.Timestamp <= yesterday {
		// 没必要从尾巴继续取了，超过一天了。
		logWithPrefix("[%s] last %d <= yesterday %d no need to append more", query, allTweets[len(allTweets)-1].Tweet.Timestamp, yesterday)
		return finalProcess(allTweets, yesterday), nil, nil
	}

	// 从尾巴上继续获取更多
	cursor, err := c.getMeta(query)
	if err != nil {
		logErrorWithPrefix("[%s] get meta failed: %v", query, err)
		if len(allTweets) > 0 {
			// 获取Cursor失败，直接cache并返回。
			return finalProcess(allTweets, yesterday), nil, nil
		}
	}
	if cursor != CURSOR_END {
		for {
			logWithPrefix("[%s] get %d, cursor: %s", query, min(1000, max-accumulated), cursor)
			var result []*TweetResult
			result, _, cursor, err = ScrapeTweetsByQueryByAccountsRound(query, min(1000, max-accumulated), cursor)
			if err != nil {
				logErrorWithPrefix("[%s] scrape error: %v", query, err)
				break
			}
			if len(result) <= 0 {
				logErrorWithPrefix("[%s] result: %d", query, len(result))
				break
			}
			tweets := SimplifyTweetResult(result)
			accumulated += len(tweets)
			expire := false
			for _, t := range tweets {
				if t.Tweet.Timestamp > yesterday {
					allTweets = append(allTweets, t)
				} else {
					logWithPrefix("[%s] fetch stops: %d > yesterday %d", query, t.Tweet.Timestamp, yesterday)
					expire = true
					break
				}
			}
			if expire {
				err = c.cacheMeta(query, CURSOR_END)
				if err != nil {
					logErrorWithPrefix("[%s-meta] cache failed: %v", query, err)
				}
				break
			}
			if accumulated >= max {
				err = c.cacheMeta(query, cursor)
				if err != nil {
					logErrorWithPrefix("[%s-meta] cache failed: %v", query, err)
				}
				break
			}
		}
	}
	if len(allTweets) > 0 {
		return finalProcess(allTweets, yesterday), nil, nil
	}
	return allTweets, nil, nil
}

// -------------- redis -----------------------
// nil = no key
func (c *TwitterCacher) getCacheTweets(key string) ([]*SimpleTweetResult, error) {
	exists, err := c.rdb.Exists(context.Background(), key).Result()
	if err != nil {
		return nil, errors.Errorf("[%s] check exist error: %v", key, err)
	}
	if exists > 0 {
		result, err := c.rdb.Get(context.Background(), key).Result()
		if err != nil {
			return nil, errors.Errorf("[%s] get error: %v", key, err)
		} else {
			var tweets []*SimpleTweetResult
			err = json.Unmarshal([]byte(result), &tweets)
			if err != nil {
				return nil, errors.Errorf("[%s] unmarshal error: %v", key, err)
			} else {
				return tweets, nil
			}
		}
	} else {
		return nil, nil
	}
}

func (c *TwitterCacher) cacheTweets(key string, tweets []*SimpleTweetResult) error {
	bytes, err := json.Marshal(tweets)
	if err != nil {
		return errors.Errorf("[%s] marshal failed: %v", key, err)
	}
	if err = c.rdb.Set(context.Background(), key, bytes, 0).Err(); err != nil {
		return errors.Errorf("[%s] update cache failed: %v", key, err)
	}
	logWithPrefix("update cache for query: %s", key)
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
	if err = c.rdb.Set(context.Background(), key, bytes, 0).Err(); err != nil {
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

// -------------- get trending queries ----------------------------------
const USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
const BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

func (c *TwitterCacher) getKeywords() []string {
	keywords := []string{"\"crypto\"", "\"btc\"", "\"eth\""}
	// keywords := []string{}

	guestToken, err := c.getGuestToken()
	if err != nil {
		logErrorWithPrefix("failed to get guest token: %v", err)
		return []string{"\"crypto\"", "\"btc\"", "\"eth\""}
	} else {
		trendingQueries, err := c.getTrendings(guestToken)
		if err != nil {
			logErrorWithPrefix("fetch trending failed: %v", err)
			return keywords
		}
		if len(trendingQueries) >= TRENDING_COUNT {
			for _, tq := range trendingQueries[:TRENDING_COUNT] {
				// strip in py
				keywords = append(keywords, fmt.Sprintf("\"%s\"", strings.TrimSpace(tq)))
			}
		} else {
			for _, tq := range trendingQueries {
				keywords = append(keywords, fmt.Sprintf("\"%s\"", strings.TrimSpace(tq)))
			}
		}
	}
	return keywords
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

func (c *TwitterCacher) getTrendings(guestToken string) ([]string, error) {
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
		logrus.Error(string(body))
		return nil, err
	}
	trends := trendingResp[0].Trends
	// 下面的排序和过滤逻辑必须和Validator中一致。
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
			trendings = append(trendings, trend.Name)
		}
	}
	return trendings, nil
}

func logWithPrefix(format string, args ...interface{}) {
	logrus.Infof("|cache| "+format, args...)
}

func logErrorWithPrefix(format string, args ...interface{}) {
	logrus.Errorf("|cache| "+format, args...)
}
