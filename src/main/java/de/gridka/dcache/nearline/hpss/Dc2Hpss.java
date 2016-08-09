package de.gridka.dcache.nearline.hpss;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.dcache.pool.nearline.spi.AbstractBlockingNearlineStorage;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.pool.nearline.spi.RemoveRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dc2Hpss extends AbstractBlockingNearlineStorage
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Dc2Hpss.class);
  
  private Path mountpoint = null;
  
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
  public void configure(Map<String, String> properties) throws IllegalArgumentException
  {
    LOGGER.trace("Configuring HSM interface '{}' with type '{}'.", name, type);
    String mnt = properties.get(MOUNTPOINT);
    checkArgument(mnt == null && mountpoint == null,
                  MOUNTPOINT + " attribute is required!");
    if (mnt != null) {
      Path dir = FileSystems.getDefault().getPath(mnt);
      checkArgument(Files.isDirectory(dir), dir + " is not a directory.");
      this.mountpoint = mnt;
      LOGGER.trace("Set mountpoint to {}.", mnt);
    }
    
    int newConcurrentPuts = properties.get(CONCURRENT_PUTS);
    if (newConcurrentPuts != null) {
      flusher.shutdown();
      this.flusher = Executors.newFixedThreadPool(newConcurrentPuts);
      LOGGER.trace("Recreated flusher with {} threads.", newConcurrentPuts);
    }
    
    int newConcurrentGets = properties.get(CONCURRENT_GETS);
    if (newConcurrentGets != null) {
      stager.shutdown();
      this.stager = Executors.newFixedThreadPool(newConcurrentGets);
      LOGGER.trace("Recreated stager with {} threads.", newConcurrentGets);
    }
    
    int newConcurrentDels = properties.get(CONCURRENT_DELS);
    if (newConcurrentDels != null) {
      remover.shutdown();
      this.remover = Executors.newFixedThreadPool(newConcurrentDels);
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
  
  private Path getExternalPath(String storageClass, String pnfsId)
  {
    return FileSystems.getDefault().getPath(
        mountpoint + '/' + storageClass + '/' + pnfsId
    );
  }
  
  @Override
  public Set<URI> flush(FlushRequest request) throws IOException
  {
    FileAttributes fileAttributes = request.getFileAttributes();
    String pnfsId = fileAttributes.getPnfsId().toString();
    Path path = request.getFile().toPath();
    Path externalPath = getExternalPath(
        fileAttributes().getStorageClass(), pnfsId
    );
    LOGGER.trace("Constructed {} as external path.", externalPath);
    
    LOGGER.debug("Start copy of {}.", pnfsId);
    Files.copy(path, externalPath, StandardCopyOption.REPLACE_EXISTING);
    LOGGER.debug("Finished copy of {}.", pnfsId);
    
    URI uri = new URI(type, name, externalPath, null, null);
    LOGGER.trace("Return {} as result URI.", uri);
    return Collections.singleton(uri);
  }

  @Override
  public Set<Checksum> stage(StageRequest request) throws IOException
  {
    FileAttributes fileAttributes = request.getFileAttributes();
    String pnfsId = fileAttributes.getPnfsId().toString();
    Path path = request.getFile().toPath();
    Path externalPath = getExternalPath(
        fileAttributes().getStorageClass(), pnfsId
    );
    LOGGER.trace("Constructed {} as external path.", externalPath);
    
    LOGGER.debug("Start copy of {}.", pnfsId);
    Files.copy(externalPath, path);
    LOGGER.debug("Finished copy of {}.", pnfsId);
    
    return Collections.emptySet();
  }

  @Override
  public void remove(RemoveRequest request) throws IOException
  {
    Path externalPath = getExternalPath(
        request.getFileAttributes().getStorageClass(),
        request.getFileAttributes().getId().toString()
    );
    
    LOGGER.trace("Delete {}.", externalPath);
    Files.deleteIfExists(externalPath);
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
