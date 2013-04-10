package ie.filechangemonitor;

import java.nio.file.Path;


/**
 * 
 * @author Jan Kebernik
 */
public interface FileChangeListener {
  
	public void fileModified(Path file);
	
	public void fileCreated(Path file);
	
	public void fileDeleted(Path file);
	
}
