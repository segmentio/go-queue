# queue

The `queue` package wraps NSQ consumers to help reduce the amount of boilerplate
that each program has to implement to get rolling. It does not attempt to abstract NSQ away entirely.

View the go-nsq [documentation](http://godoc.org/github.com/bitly/go-nsq#Config) for configuration options.

## Example

```go
c := queue.NewConsumer("events", "ingestion")

c.Set("nsqd", ":5001")
c.Set("concurrency", 15)
c.Set("max_attempts", 10)
c.Set("max_in_flight", 150)
c.Set("default_requeue_delay", "15s")

c.Start(nsq.HandlerFunc(func(msg *nsq.Message) error {
  // do something
  return nil
}))
```

## Usage

#### type Consumer

```go
type Consumer struct {}
```

Consumer convenience layer.

#### func  NewConsumer

```go
func NewConsumer(topic, channel string) *Consumer
```
NewConsumer returns a new consumer of `topic` and `channel`.

#### func (*Consumer) Set

```go
func (c *Consumer) Set(option string, value interface{})
```
Set `option` to `value`, any error will be returned in `.Start()`.

Custom options implemented:

    - `topic` consumer topic
    - `channel` consumer channel
    - `nsqd` nsqd address
    - `nsqds` nsqd addresses
    - `nsqlookupd` nsqlookupd address
    - `nsqlookupds` nsqlookupd addresses
    - `concurrency` concurrent handlers [1]

#### func (*Consumer) SetLogger

```go
func (c *Consumer) SetLogger(log logger, level nsq.LogLevel)
```
SetLogger replaces the default logger.

#### func (*Consumer) Start

```go
func (c *Consumer) Start(handler nsq.Handler) error
```
Start consumer with `handler`.

#### func (*Consumer) Stop

```go
func (c *Consumer) Stop() error
```
Stop and wait.

# License

 MIT