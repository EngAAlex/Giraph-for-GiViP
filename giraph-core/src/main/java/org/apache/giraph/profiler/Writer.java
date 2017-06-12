/**
 * 
 */
package org.apache.giraph.profiler;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.giraph.utils.io.AsyncHDFSWriteService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.protobuf.GeneratedMessage;

/**
 * @author maria
 *
 */
public abstract class Writer {

	protected FileSystem fileSystem = null;
	private String HDFSMainPath = "ProfilerCopier/";

	public Writer(){
		try {
			fileSystem = FileSystem.get(new Configuration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public abstract GeneratedMessage generateMessageToWrite();

	/**
	 * @return Path where the concrete class writes the file in HDFS
	 */
	public abstract String generateFullPath();

	public abstract void clearMessages();

	/**
	 * @return Number of message elements
	 */
	public abstract int countMessages();

	/**
	 * Write data locally
	 * 
	 */
	public void localWrite(){
		if (countMessages() > 0){
			Path path = Paths.get(System.getProperty("user.home") +"/"+ this.generateFullPath());
			try {
				Files.createDirectories(path.getParent());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				Files.createFile(path);
			} catch (IOException e) {
				e.printStackTrace();

			}

			FileOutputStream output=null;
			try {
				output = new FileOutputStream(path.toString());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(output != null){
				try {

					this.generateMessageToWrite().writeTo(output);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					output.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		this.clearMessages();
	}

	public org.apache.hadoop.fs.Path generateHDFSMainPath(){
		org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(this.fileSystem.getHomeDirectory()+this.HDFSMainPath); 
		return path;
	}

	/**
	 * Write the messagesBook in HDFS file
	 * 
	 */ 
	public void writeToHDFS() {
		// Write data in HDFS file
		//      if (this.countMessage() > 0){
		AsyncHDFSWriteService.writeToHDFS(
				this.generateMessageToWrite(), fileSystem, this.generateFullPath());
		//        }
		// Clears from all contents 
		this.clearMessages();
	}

}
