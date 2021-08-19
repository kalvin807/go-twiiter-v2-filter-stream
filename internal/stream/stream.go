package stream

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/go-querystring/query"
)

const streamV2Endpoint = "https://api.twitter.com/2/tweets/search"

type StreamService struct {
	client *http.Client
	token  string
}

func NewStreamService(client *http.Client, token string) *StreamService {
	return &StreamService{
		client: client,
		token:  token,
	}
}

func createStreamRequest(params *StreamFilterParams, token string) (*http.Request, error) {
	url := fmt.Sprintf("%s/%s", streamV2Endpoint, "stream")
	println(url)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	q, _ := query.Values(params)
	req.URL.RawQuery = q.Encode()
	return req, nil
}

func (srv *StreamService) Connect(params *StreamFilterParams) (*Stream, error) {
	req, err := createStreamRequest(params, srv.token)
	if err != nil {
		return nil, err
	}
	return newStream(srv.client, req), nil
}

type StreamFilterParams struct {
	Expansions  []string `url:"expansions,omitempty,comma"`
	MediaFields []string `url:"media.fields,omitempty,comma"`
	PlaceFields []string `url:"place.fields,omitempty,comma"`
	PollFields  []string `url:"poll.fields,omitempty,comma"`
	TweetFields []string `url:"tweet.fields,omitempty,comma"`
	UserFields  []string `url:"user.fields,omitempty,comma"`
}

type StreamData struct {
	Tweet         *Tweet `json:"data,omitempty"`
	MatchingRules []struct {
		Id  string `json:"id,omitempty"`
		Tag string `json:"tag,omitempty"`
	} `json:"matching_rules,omitempty"`
}

// Stream maintains a connection to the Twitter Streaming API, receives
// messages from the streaming response, and sends them on the Messages
// channel from a goroutine. The stream goroutine stops itself if an EOF is
// reached or retry errors occur, also closing the Messages channel.
//
// The client must Stop() the stream when finished receiving, which will
// wait until the stream is properly stopped.
type Stream struct {
	client   *http.Client
	Messages chan *StreamData
	done     chan struct{}
	group    *sync.WaitGroup
	body     io.Closer
}

// newStream creates a Stream and starts a goroutine to retry connecting and
// receive from a stream response. The goroutine may stop due to retry errors
// or be stopped by calling Stop() on the stream.
func newStream(client *http.Client, req *http.Request) *Stream {
	s := &Stream{
		client:   client,
		Messages: make(chan *StreamData),
		done:     make(chan struct{}),
		group:    &sync.WaitGroup{},
	}
	s.group.Add(1)
	go s.retry(req, newExponentialBackOff(), newAggressiveExponentialBackOff())
	return s
}

// Stop signals retry and receiver to stop, closes the Messages channel, and
// blocks until done.
func (s *Stream) Stop() {
	close(s.done)
	// Scanner does not have a Stop() or take a done channel, so for low volume
	// streams Scan() blocks until the next keep-alive. Close the resp.Body to
	// escape and stop the stream in a timely fashion.
	if s.body != nil {
		s.body.Close()
	}
	// block until the retry goroutine stops
	s.group.Wait()
}

// retry retries making the given http.Request and receiving the response
// according to the Twitter backoff policies. Callers should invoke in a
// goroutine since backoffs sleep between retries.
// https://dev.twitter.com/streaming/overview/connecting
func (s *Stream) retry(req *http.Request, expBackOff backoff.BackOff, aggExpBackOff backoff.BackOff) {
	// close Messages channel and decrement the wait group counter
	defer close(s.Messages)
	defer s.group.Done()

	var wait time.Duration
	for !stopped(s.done) {
		resp, err := s.client.Do(req)
		if err != nil {
			// stop retrying for HTTP protocol errors
			panic(err)
		}
		// when err is nil, resp contains a non-nil Body which must be closed
		defer resp.Body.Close()
		s.body = resp.Body
		switch resp.StatusCode {
		case http.StatusOK:
			// receive stream response Body, handles closing
			s.receive(resp.Body)
			expBackOff.Reset()
			aggExpBackOff.Reset()
		case http.StatusServiceUnavailable:
			// exponential backoff
			wait = expBackOff.NextBackOff()
		case 420, http.StatusTooManyRequests:
			// 420 Enhance Your Calm is unofficial status code by Twitter on being rate limited.
			// aggressive exponential backoff
			wait = aggExpBackOff.NextBackOff()
		default:
			// stop retrying for other response codes
			resp.Body.Close()
			return
		}
		// close response before each retry
		resp.Body.Close()
		if wait == backoff.Stop {
			return
		}
		sleepOrDone(wait, s.done)
	}
}

// receive scans a stream response body, JSON decodes tokens to messages, and
// sends messages to the Messages channel. Receiving continues until an EOF,
// scan error, or the done channel is closed.
func (s *Stream) receive(body io.Reader) {
	reader := newStreamResponseBodyReader(body)
	for !stopped(s.done) {
		data, err := reader.readNext()
		if err != nil {
			return
		}
		if len(data) == 0 {
			// empty keep-alive
			continue
		}
		select {
		// allow client to Stop(), even if not receiving
		case <-s.done:
			return
		// send messages, data, or errors
		default:
			msg, _ := getMessage(data)
			s.Messages <- msg
		}
	}
}

// getMessage unmarshals the token and returns a message struct, if the type
// can be determined. Otherwise, returns the token unmarshalled into a data
// map[string]interface{} or the unmarshal error.
func getMessage(token []byte) (*StreamData, error) {
	// unmarshal JSON encoded token into a map for
	data := &StreamData{}
	err := json.Unmarshal(token, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
