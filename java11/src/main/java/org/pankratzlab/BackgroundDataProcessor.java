package org.pankratzlab;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.function.Function;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class BackgroundDataProcessor<K, V> implements Function<K, V> {
  private final ListeningExecutorService executorService;
  private final LoadingCache<K, V> cache;

  public BackgroundDataProcessor(Collection<K> list, Function<K, V> valueLoader,
                                 Function<Throwable, V> exceptionHandler,
                                 RemovalListener<K, V> removalListener) {
    executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(Runtime.getRuntime()
                                                                                           .availableProcessors()));
    var builder = CacheBuilder.newBuilder().weakValues();
    if (removalListener != null) {
      builder.removalListener(removalListener);
    }
    cache = builder.build(new CacheLoader<>() {
      public V load(K key) {
        return compute(valueLoader, key, exceptionHandler);
      }

      public ListenableFuture<V> reload(K key, V oldValue) {
        return executorService.submit(() -> compute(valueLoader, key, exceptionHandler));
      }
    });
    list.forEach(this::asyncPreloadCache);
  }

  private V compute(Function<K, V> valueLoader, K key, Function<Throwable, V> exceptionHandler) {
    try {
      return valueLoader.apply(key);
    } catch (Throwable t) {
      if (exceptionHandler != null) {
        return exceptionHandler.apply(t);
      } else {
        throw new RuntimeException("Unhandled exception during cache load", t);
      }
    }
  }

  private void asyncPreloadCache(K key) {
    executorService.submit(() -> cache.refresh(key));
  }

  public V get(K key) {
    V value = cache.getUnchecked(key);
    cache.invalidate(key);
    return value;
  }

  @Override
  public V apply(K t) {
    return get(t);
  }
}