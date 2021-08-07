package main

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

func main() {
	stop := make(chan bool)
	defer close(stop)

	concurrent := 4
	for item := range Crawl("https://golang.org/", concurrent, stop) {
		if item.error != nil {
			fmt.Printf("[%v] (%v) %v => %v\r\n", item.name, item.timestamp.Format("2006-01-02T15:04:05.000"), item.url, item.error.Error())
		} else {
			fmt.Printf("[%v] (%v) %v => %v\r\n", item.name, item.timestamp.Format("2006-01-02T15:04:05.000"), item.url, item.body)
		}
	}
}

type FetchResult struct {
	timestamp time.Time
	name      string
	url       string
	body      string
	error     error
}

type safeStringSet struct {
	mutex sync.RWMutex
	store map[string]bool
}

func newSafeStringSet() *safeStringSet {
	return &safeStringSet{store: make(map[string]bool)}
}

func (s *safeStringSet) set(value string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.store[value]; ok {
		return false
	} else {
		s.store[value] = true
		return true
	}
}

func (s *safeStringSet) fetch(name string, urlChanel <-chan string, urlsChanel chan<- []string, fetchResult chan<- FetchResult) chan<- bool {

	stop := make(chan bool)

	go func() {
		for {
			select {
			case url := <-urlChanel:

				s.set(url)

				currentTime := time.Now()
				body, urls, err := fetcher.Fetch(url)
				fetchResult <- FetchResult{
					timestamp: currentTime,
					name:      name,
					url:       url,
					body:      body,
					error:     err,
				}

				validUrls := make([]string, 0, len(urls))

				for _, url := range urls {
					if s.set(url) {
						validUrls = append(validUrls, url)
					}
				}

				urlsChanel <- validUrls
			case <-stop:
				return
			}
		}
	}()

	return stop
}

func Crawl(url string, concurrent int, stop chan bool) <-chan FetchResult {

	result := make(chan FetchResult)

	go func(url string) {

		defer close(result)
		visitedUrl := newSafeStringSet()

		urlChanel := make(chan string, concurrent)

		urlsChanel := make(chan []string, concurrent)

		stops := make([]chan<- bool, concurrent)

		for index := 0; index < concurrent; index++ {
			stops[index] = visitedUrl.fetch(fmt.Sprintf("Worker:%d", index+1), urlChanel, urlsChanel, result)
		}

		urlChanel <- url
		urlQueue := list.New()
		counter := 1

	for_:
		for {
			select {
			case urls := <-urlsChanel:
				counter--

				for _, item := range urls {
					urlQueue.PushBack(item)
				}

				if counter == 0 && urlQueue.Len() == 0 {
					break for_
				}

				for ; counter < concurrent && urlQueue.Len() > 0; counter++ {
					node := urlQueue.Front()
					urlQueue.Remove(node)
					urlChanel <- node.Value.(string)
				}
			case <-stop:
				break for_
			}
		}

		for index := 0; index < concurrent; index++ {
			close(stops[index])
		}

	}(url)

	return result
}

type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body    string
	elapsed time.Duration
	urls    []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		time.Sleep(res.elapsed)
		return res.body, res.urls, nil
	}
	time.Sleep(time.Duration(500))
	return "", nil, fmt.Errorf("not found")
}

var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		1000 * time.Millisecond,
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
			"https://golang.org/internal/",
		},
	},
	"https://golang.org/internal/": &fakeResult{
		"Packages internal",
		1000 * time.Millisecond,
		[]string{
			"https://golang.org/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		1000 * time.Millisecond,
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
			"https://golang.org/pkg/container/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		1000 * time.Millisecond,
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/container/": &fakeResult{
		"Package container",
		1000 * time.Millisecond,
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/pkg/container/list/",
			"https://golang.org/pkg/container/heap/",
		},
	},
	"https://golang.org/pkg/container/list/": &fakeResult{
		"Package list",
		1000 * time.Millisecond,
		[]string{"https://golang.org/pkg/container/"},
	},
	"https://golang.org/pkg/container/heap/": &fakeResult{
		"Package heap",
		1000 * time.Millisecond,
		[]string{"https://golang.org/pkg/container/"},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		1000 * time.Millisecond,
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
