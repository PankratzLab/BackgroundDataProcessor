package org.pankratzlab;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Function;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Upon creation, will start to load data into a cache in background threads. Values in the cache
 * are weak, and are invalidated immediately after access, so as soon as they are no longer
 * referenced they will disappear!
 * 
 * @param <K> Key type of the cache.
 * @param <V> Value type of the cache.
 */
public class BackgroundDataProcessor<K, V> implements Function<K, V> {
  private final ListeningExecutorService executorService;
  private final LoadingCache<K, V> cache;

  public BackgroundDataProcessor(Collection<K> list, Function<K, V> valueLoader) {
    this(list, valueLoader, null, null);
  }

  public BackgroundDataProcessor(Collection<K> list, Function<K, V> valueLoader,
                                 Function<Throwable, V> valueLoadingExceptionHandler) {
    this(list, valueLoader, valueLoadingExceptionHandler, null);
  }

  public BackgroundDataProcessor(Collection<K> list, Function<K, V> valueLoader,
                                 Function<Throwable, V> valueLoadingExceptionHandler,
                                 RemovalListener<K, V> removalListener) {
    executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(Runtime.getRuntime()
                                                                                           .availableProcessors()));

    // Use casting here to match the type expected for LoadingCache
    @SuppressWarnings("unchecked")
    CacheBuilder<K, V> builder = (CacheBuilder<K, V>) CacheBuilder.newBuilder().weakValues();
    if (removalListener != null) {
      builder.removalListener(removalListener);
    }

    cache = builder.build(new CacheLoader<K, V>() {
      public V load(K key) {
        return compute(valueLoader, key, valueLoadingExceptionHandler);
      }

      @Override
      public ListenableFuture<V> reload(K key, V oldValue) {
        return executorService.submit(new Callable<V>() {
          public V call() {
            return compute(valueLoader, key, valueLoadingExceptionHandler);
          }
        });
      }
    });

    // Preload the cache
    list.forEach(this::asyncPreloadCache);
  }

  private V compute(Function<K, V> valueLoader, K key,
                    Function<Throwable, V> valueLoadingExceptionHandler) {
    try {
      return valueLoader.apply(key);
    } catch (Throwable t) {
      if (valueLoadingExceptionHandler != null) {
        return valueLoadingExceptionHandler.apply(t);
      } else {
        throw new RuntimeException("Unhandled exception during cache load", t);
      }
    }
  }

  private void asyncPreloadCache(K key) {
    executorService.submit(() -> cache.refresh(key));
  }

  public V get(K key) {
    V value = cache.getUnchecked(key); // this call blocks until the value is ready
    cache.invalidate(key); // Invalidate the entry after access to ensure it is eligible for garbage
                           // collection
    return value;
  }

  @Override
  public V apply(K t) {
    return get(t);
  }
}