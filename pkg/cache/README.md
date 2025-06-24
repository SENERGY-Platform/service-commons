
## Cache
The cache may be created with a second layer and a fallback.

- L2: may be useful if a resource is shared over multiple services
- Fallback: may be useful if the service may lose internet connection and wants to fall back to a local store
- ReadCacheHook and CacheMissHook may be used to collect statistics/metrics of the cache use
- CacheInvalidationSignalHooks: can be used to handle cache invalidation via signal.Broker (ref [Invalidation](#Invalidation))

example:

```go
cache, err := New(Config{
    L1Provider:       localcache.NewProvider(10*time.Minute, 50*time.Millisecond),
    L2Provider:       memcached.NewProvider(10, 10*time.Second, memcachUrls...),
    FallbackProvider: fallback.NewProvider(t.TempDir() + "/fallback.json"),
    Debug:            false,
    ReadCacheHook: func(duration time.Duration) {
        mux.Lock()
        defer mux.Unlock()
        cacheReads = append(cacheReads, duration)
    },
    CacheMissHook: func() {
        mux.Lock()
        defer mux.Unlock()
        cacheMisses = cacheMisses + 1
    },
    CacheInvalidationSignalHooks: map[Signal]ToKey{
        signal.Known.CacheInvalidationAll: nil,
        signal.Known.DeviceTypeCacheInvalidation: func(signalValue string) (cacheKey string) {
            return "dt." + signalValue
        },
    },
})
```

### Use
the cache package provides the `Use` function.

`func Use[T any](cache *Cache, key string, get func() (T, error), validate func(T) error, exp time.Duration, l2Exp ...time.Duration) (result T, err error)`

Use tries to retrieve a key from the Cache and cast the result as T.
If unsuccessful (because of a cache-miss or a validation error) the cache is updated wit a value received from the 'get' function parameter.
The 'exp' and 'l2Exp' parameters are passed to the 'Cache.Set' method and define the expiration time of newly cached values.
   - 'exp' defines the time until the value is expired and cant be retrieved
   - 'l2Exp' (optional) is used to define a separate expiration date for the l2 cache (only used if Cache.l2 is set, only first value is used,if no l2Exp is set, the exp parameter is used)
   - the 'validate' function parameter is passed to the Get function to prevent poisoned or stale cache values. if no validation is needed `NoValidation[T]` or `nil` may be used

if the expiration is dependent on the response of the 'get' parameter, the `UseWithExpInGet` function can be used instead

example:
```go
func (this *DeviceRepo) GetFunction(id string) (result models.Function, err error) {
	return cache.Use(this.cache, "functions."+id, func() (result models.Function, err error) {
		return this.getFunction(id)
	}, func(function models.Function) error {
		if function.Id == "" {
			return errors.New("invalid function returned from cache")
		}
		return nil
	}, this.cacheDuration)
}
```

## Invalidation

the user may want to create a cache in a dependency-client implementation. 
this cache may be created transparent from a using controller while other clint implementations don't use a cache.
but the controller may still want to invalidate cache values in all caches the application uses. to enable this, the cache supports signal.Broker.

example: 

```go
//cache creation, for example somewhere in pkg/eventrepo/cloud/cloudimpl.go
c, err := cache.New(cache.Config{
    CacheInvalidationSignalHooks: map[cache.Signal]cache.ToKey{
        signal.Known.CacheInvalidationAll: nil,
        signal.Known.ConceptCacheInvalidation: func(signalValue string) (cacheKey string) {
            return "concept." + signalValue
        },
        signal.Known.CharacteristicCacheInvalidation: func(signalValue string) (cacheKey string) {
            return "characteristics." + signalValue
        },
        signal.Known.FunctionCacheInvalidation: func(signalValue string) (cacheKey string) {
            return "functions." + signalValue
        },
        signal.Known.AspectCacheInvalidation: nil, //invalidate everything, because an aspect corresponds to multiple aspect-nodes
    },
})
 if err != nil {
    t.Error(err)
    return
}

// invalidator start, for example somewhere in pkg/pkg.go
err = invalidator.StartKnownCacheInvalidators(ctx, kafka.Config{
    KafkaUrl:      kafkaUrl,
    ConsumerGroup: "test",
    StartOffset:   kafka.LastOffset,
    Wg:            wg,
}, invalidator.KnownTopics{
    AspectTopic:			"aspects"
    ConceptTopic: 			"concepts"
    CharacteristicTopic:	"characteristics",
    FunctionTopic: 			"functions",
}, nil)
if err != nil {
    t.Error(err)
    return
}

 err = invalidator.StartCacheInvalidatorAll(ctx, kafka.Config{
    KafkaUrl:      kafkaUrl,
    ConsumerGroup: "test",
    StartOffset:   kafka.LastOffset,
    Wg:            wg,
}, []string{"topics", "where", "all", "cache", "values", "should", "be", "reset"}, nil)
if err != nil {
    t.Error(err)
    return
}

// manual invalidation by separate kafka handler, for example somewhere in pkg/consumer/cloud/consumer.go
err = NewKafkaLastOffsetConsumer(basectx, wg, config.KafkaUrl, "consumergroup", config.ProcessDeploymentDoneTopic, func(delivery []byte) error {
    signal.DefaultBroker.Pub(signal.Known.CacheInvalidationAll, "")
	//other work
}
```