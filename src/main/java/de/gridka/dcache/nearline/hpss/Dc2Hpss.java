package de.gridka.dcache.nearline.hpss;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.lang.Integer;
import java.lang.NumberFormatException;
import java.lang.StringBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.pool.nearline.AbstractBlockingNearlineStorage;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dc2Hpss extends AbstractBlockingNearlineStorage
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2Hpss.class);
  
  private String mountpoint = null;
  
  private static final String MOUNTPOINT = "mountpoint";
  private static final String CONCURRENT_PUTS = "puts";
  private static final String CONCURRENT_GETS = "gets";
  private static final String CONCURRENT_DELS = "dels";
  
  private static final int DEFAULT_FLUSH_THREADS = 10;
  private static final int DEFAULT_STAGE_THREADS = 1000;
  private static final int DEFAULT_REMOVE_THREADS = 1;
  
  ExecutorService flusher = Executors.newFixedThreadPool(DEFAULT_FLUSH_THREADS);
  ExecutorService stager = Executors.newFixedThreadPool(DEFAULT_STAGE_THREADS);
  ExecutorService remover = Executors.newFixedThreadPool(DEFAULT_REMOVE_THREADS);
  
  public Dc2Hpss(String type, String name)
  {
      super(type, name);
  }
  
  /**
   * Applies a new configuration.
   *
   * @param properties
   * @throws IllegalArgumentException if the configuration is invalid.
   * @throws IllegalStateException if there are current requests.
   */
  @Override
  public void configure(Map<String, String> properties) throws IllegalArgumentException, NumberFormatException, InvalidPathException
  {
    LOGGER.trace("Configuring HSM interface '{}' with type '{}'.", name, type);
    String mnt = properties.get(MOUNTPOINT);
    checkArgument(mnt != null || mountpoint != null, MOUNTPOINT + " attribute is required!");
    if (mnt != null) {
      checkArgument(Files.isDirectory(Paths.get(mnt)), mnt + " is not a directory!");
      this.mountpoint = mnt;
      LOGGER.trace("Set mountpoint to {}.", mnt);
    }
    
    int iNewConcurrentPuts;
    String newConcurrentPuts = properties.get(CONCURRENT_PUTS);
    if (newConcurrentPuts != null) {
      try {
        iNewConcurrentPuts = Integer.parseInt(newConcurrentPuts);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(CONCURRENT_PUTS + " is not an integer number!", e);
      }
      flusher.shutdown();
      this.flusher = Executors.newFixedThreadPool(iNewConcurrentPuts);
      LOGGER.trace("Recreated flusher with {} threads.", newConcurrentPuts);
    }
    
    int iNewConcurrentGets;
    String newConcurrentGets = properties.get(CONCURRENT_GETS);
    if (newConcurrentGets != null) {
      try {
        iNewConcurrentGets = Integer.parseInt(newConcurrentGets);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(CONCURRENT_GETS + " is not an integer number!", e);
      }
      stager.shutdown();
      this.stager = Executors.newFixedThreadPool(iNewConcurrentGets);
      LOGGER.trace("Recreated stager with {} threads.", newConcurrentGets);
    }
    
    int iNewConcurrentDels;
    String newConcurrentDels = properties.get(CONCURRENT_DELS);
    if (newConcurrentDels != null) {
      try {
        iNewConcurrentDels = Integer.parseInt(newConcurrentDels);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(CONCURRENT_DELS + " is not an integer number!", e);
      }
      remover.shutdown();
      this.remover = Executors.newFixedThreadPool(iNewConcurrentDels);
      LOGGER.trace("Recreated remover with {} threads.", newConcurrentDels);
    }
  }
  
  @Override
  protected Executor getFlushExecutor()
  {
      return flusher;
  }
  
  @Override
  protected Executor getStageExecutor()
  {
      return stager;
  }
  
  @Override
  protected Executor getRemoveExecutor()
  {
      return remover;
  }
  
  // Optionally evaluate any information of the file to construct the path.
  private String getHsmPath(StorageInfo storageInfo, String pnfsId)
  {
    StringBuilder sb = new StringBuilder();
    //sb.append('/' + storageInfo.getKey("store"));
    //sb.append('/' + storageInfo.getKey("group"));
    sb.append('/' + pnfsId);
    return sb.toString();
  }
  
  @Override
  public Set<URI> flush(FlushRequest request) throws CacheException, URISyntaxException
  {
    FileAttributes fileAttributes = request.getFileAttributes();
    String pnfsId = fileAttributes.getPnfsId().toString();
    Path path = request.getFile().toPath();
    String hsmPath = getHsmPath(fileAttributes.getStorageInfo(), pnfsId);
    LOGGER.trace("Constructed {} as hsm path.", hsmPath);
    Path externalPath = Paths.get(mountpoint, hsmPath);
    
    LOGGER.debug("Start copy of {}.", pnfsId);
    try {
      Files.copy(path, externalPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new CacheException(2, "Copy to " + externalPath.toString() + " failed.", e);
    }
    LOGGER.debug("Finished copy of {}.", pnfsId);
    
    URI uri = new URI(type, name, hsmPath, null, null);
    return Collections.singleton(uri);
  }

  @Override
  public Set<Checksum> stage(StageRequest request) throws CacheException
  {
    FileAttributes fileAttributes = request.getFileAttributes();
    String pnfsId = fileAttributes.getPnfsId().toString();
    Path path = request.getFile().toPath();
    String hsmPath = getHsmPath(fileAttributes.getStorageInfo(), pnfsId);
    LOGGER.trace("Constructed {} as hsm path.", hsmPath);
    Path externalPath = Paths.get(mountpoint, hsmPath);
    
    LOGGER.debug("Start copy of {}.", pnfsId);
    try {
      Files.copy(externalPath, path);
    } catch (IOException e) {
      throw new CacheException(3, "Copy of " + externalPath.toString() + " failed.", e);
    }
    LOGGER.debug("Finished copy of {}.", pnfsId);
    
    // No easy way to get the file's checkusm.
    return Collections.emptySet();
  }

  @Override
  public void remove(RemoveRequest request) throws CacheException
  {
    String hsmPath = request.getUri().getPath();
    Path externalPath = Paths.get(mountpoint, hsmPath);
    
    LOGGER.trace("Delete {}.", externalPath.toString());
    try {
      Files.deleteIfExists(externalPath);
    } catch (IOException e) {
      throw new CacheException("Deletion of " + externalPath.toString() + " failed.", e);
    }
  }

  /**
   * Cancels all requests and initiates a shutdown of the nearline storage
   * interface.
   * <p>
   * This method does not wait for actively executing requests to
   * terminate.
   */
  @Override
  public void shutdown()
  {
    super.shutdown();
    flusher.shutdown();
    stager.shutdown();
    remover.shutdown();
  }
  
}
