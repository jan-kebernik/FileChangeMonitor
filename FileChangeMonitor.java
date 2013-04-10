package ie.filechangemonitor;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.ConcurrentHashMap;
import static java.nio.file.StandardWatchEventKinds.*;
import java.nio.file.WatchEvent;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-Safe facility for monitoring file changes (creation, deletion, modification). 
 * <p>
 * To watch a file or directory, a valid Path to that file as well as a FileChangeListener
 * need to be registered with the FileChangeMonitor. Whenever a change is 
 * detected, all listeners that are registered for that file are notified of 
 * the type of change. 
 * <p>
 * Note that the listeners are notified of changes asynchronously, with no guarantee
 * as to the order of the changes. It is therefore the listeners' responsibility to 
 * introduce synchronization if necessary and to not rely on a specific order of 
 * events. 
 * <p>
 * If an observed directory is moved or deleted and then re-created, the listeners
 * will cease to receive further notifications. This case cannot be avoided, 
 * because a directory cannot detect its own re-creation while it does not exist.
 * <p>
 * It is the listeners' responsibility to unregister themselves from the monitor! 
 * 
 * @author Jan Kebernik
 */
public class FileChangeMonitor{

	private static final WatchService watcher = newWatchService();
	private static final ExecutorService listenerExec = Executors.newSingleThreadExecutor();
	private static final ExecutorService dispatcherExec = Executors.newCachedThreadPool();
	private static final ConcurrentMap<Path, Directory> register = new ConcurrentHashMap<>();
	private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	static {
		if (watcher == null) {
			throw new IllegalStateException("FileChangeMonitor could not be created!");
		}
		else {
			final Thread watcherCloser = new Thread(new ShutDownTask());
			Runtime.getRuntime().addShutdownHook(watcherCloser);
			listenerExec.submit(new WatcherTask());
		}
	}
	
	private static WatchService newWatchService() {
		try {
			return FileSystems.getDefault().newWatchService();
		} catch (IOException ex) {
			return null;
		}
	}
	
	private static Directory getDirectory(Path path, WatchKey key) throws IOException{
		final Directory newDirectory = new Directory(key);
		final Directory oldDirectory = register.putIfAbsent(path, newDirectory);
		if (oldDirectory == null) {
			return newDirectory;
		}
		return oldDirectory;
	}
	
	public static void register(FileChangeListener listener, Path file){
		try {
		 	if (Files.isDirectory(file)){
				final Path dirPath = file.toRealPath();
				final WatchKey key = dirPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
				lock.readLock().lock();
				try {
					final Directory dir = getDirectory(dirPath, key);
					dir.addDirectoryListener(listener);
				}
				finally {
					lock.readLock().unlock();
				}
			} 
			else if (Files.isRegularFile(file)) {
				final Path dirPath = file.getParent().toRealPath();
				final Path filePath = file.getFileName();
				final WatchKey key = dirPath.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
				
				lock.readLock().lock();
				try{
					final Directory dir = getDirectory(dirPath, key);
					dir.addFileListener(filePath, listener);
				}
				finally {
					lock.readLock().unlock();
				}
			}
		}
		catch (IOException ex) {
			
		}
	}
	
	public static void unregister(FileChangeListener listener){
		lock.writeLock().lock();
		try {
			for (Entry<Path, Directory> e : register.entrySet()){
				final Directory directory = e.getValue();
				for (Entry<Path, CopyOnWriteArrayList<FileChangeListener>> f : directory.registeredFiles.entrySet()){
					final CopyOnWriteArrayList<FileChangeListener> l = f.getValue();
					if (l.remove(listener)){
						if (l.isEmpty()){
							directory.registeredFiles.remove(f.getKey());
						}
					}
				}
				directory.directoryListeners.remove(listener);
				if (directory.directoryListeners.isEmpty() && directory.registeredFiles.isEmpty()){
					register.remove(e.getKey());
					directory.key.cancel();
				}
			}
		}
		finally {
			lock.writeLock().unlock();
		}
	}
	
	public static void unregister(FileChangeListener listener, Path file) {
		try {
			if (Files.isDirectory(file)){
				final Path dirPath = file.toRealPath();
				lock.writeLock().lock();
				try {
					final Directory dir = register.get(dirPath);
					if (dir != null){
						if (dir.directoryListeners.remove(listener)){
							if (dir.directoryListeners.isEmpty() && dir.registeredFiles.isEmpty()){
								register.remove(dirPath);
								dir.key.cancel();
							}
						}
					}
				}
				finally {
					lock.writeLock().unlock();
				}
			} 
			else if (Files.isRegularFile(file)) {
				final Path dirPath = file.getParent().toRealPath();
				final Path filePath = file.getFileName();
				lock.writeLock().lock();
				try {
					final Directory dir = register.get(dirPath);
					if (dir != null){
						final CopyOnWriteArrayList<FileChangeListener> list = dir.registeredFiles.get(filePath);
						if (list != null){
							if (list.remove(listener)){
								if (list.isEmpty()){
									dir.registeredFiles.remove(filePath);
									if (dir.registeredFiles.isEmpty() && dir.directoryListeners.isEmpty()){
										register.remove(dirPath);
										dir.key.cancel();
									}
								}
							}
						}
					}
				}
				finally {
					lock.writeLock().unlock();
				}
			}
		}
		catch (IOException ex) {}
	}
	
	private static class Directory {
		private final CopyOnWriteArrayList<FileChangeListener> directoryListeners;
		private final ConcurrentMap<Path, CopyOnWriteArrayList<FileChangeListener>> registeredFiles;
		private final WatchKey key;
		
		private Directory(WatchKey key) {
			this.key = key;
			directoryListeners = new CopyOnWriteArrayList<>();
			registeredFiles = new ConcurrentHashMap<>();
		}

		private CopyOnWriteArrayList<FileChangeListener> getFileListeners(Path file) {
			final CopyOnWriteArrayList<FileChangeListener> newList = new CopyOnWriteArrayList<>();
			final CopyOnWriteArrayList<FileChangeListener> oldList = registeredFiles.putIfAbsent(file, newList);
			if (oldList == null) {
				return newList;
			}
			return oldList;
		}

		private void addFileListener(Path file, FileChangeListener listener) {
			final CopyOnWriteArrayList<FileChangeListener> fileListeners = getFileListeners(file);
			fileListeners.addIfAbsent(listener);
		}

		private void addDirectoryListener(FileChangeListener listener) {
			directoryListeners.addIfAbsent(listener);
		}
		
		private void eventCreate(Path directoryPath, Path fileName) throws IOException{
			final Path file = directoryPath.resolve(fileName);
			for (FileChangeListener l : directoryListeners){
				l.fileCreated(file);
			}
			final CopyOnWriteArrayList<FileChangeListener> fileListeners = registeredFiles.get(fileName);
			if (fileListeners != null) {
				for (FileChangeListener l : fileListeners) {
					l.fileCreated(file);
				}
			}
		}
		
		private void eventModify(Path directoryPath, Path fileName) throws IOException {
			final Path file = directoryPath.resolve(fileName);
			for (FileChangeListener l : directoryListeners){
				l.fileModified(file);
			}
			final CopyOnWriteArrayList<FileChangeListener> fileListeners = registeredFiles.get(fileName);
			if (fileListeners != null) {
				for (FileChangeListener l : fileListeners) {
					l.fileModified(file);
				}
			}
		}
		
		private void eventDelete(Path directoryPath, Path fileName) throws IOException {
			final Path file = directoryPath.resolve(fileName);
			for (FileChangeListener l : directoryListeners){
				l.fileDeleted(file);
			}
			final CopyOnWriteArrayList<FileChangeListener> fileListeners = registeredFiles.get(fileName);
			if (fileListeners != null) {
				for (FileChangeListener l : fileListeners) {
					l.fileDeleted(file);
				}
			}
		}
	}

	private static class ShutDownTask implements Runnable {
		@Override
		public void run() {
			try {
				watcher.close();
			} catch (IOException ex) {
				
			}
		}
	}
	
	private static class WatcherTask implements Runnable {
		
		@Override
		public void run() {
			while (true) {
				final WatchKey key;
				try {
					key = watcher.take();
				} catch (InterruptedException x) {
					return;
				}
				final List<WatchEvent<?>> events = key.pollEvents();
				try {
					final Directory directory = register.get(((Path)key.watchable()).toRealPath());
					if (directory != null) {
						dispatcherExec.submit(new DispatcherTask(directory, events));
					}
				}
				catch (IOException iox){
					// path is gone!
				}
				key.reset();
			}
		}
	}
	
	
	private static class DispatcherTask implements Runnable {
		
		private final Directory directory;
		private final List<WatchEvent<?>> events;
		
		private DispatcherTask(Directory directory, List<WatchEvent<?>> events) {
			this.directory = directory;
			this.events = events;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			try {
				final Path directoryPath = ((Path)directory.key.watchable()).toRealPath();
				for (WatchEvent<?> e : events) {
					try {
						final WatchEvent.Kind<?> kind = e.kind();
						if (kind == ENTRY_CREATE){
							directory.eventCreate(directoryPath, ((WatchEvent<Path>)e).context());
						}
						else if (kind == ENTRY_MODIFY) {
							directory.eventModify(directoryPath, ((WatchEvent<Path>)e).context());
						}
						else if (kind == ENTRY_DELETE) {
							directory.eventDelete(directoryPath, ((WatchEvent<Path>)e).context());
						}
					}
					catch (IOException ex) {}
				}
			} catch (IOException ex) {}
		}
	}
	
}
