package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.nio.file.Files;
//import java.util.concurrent.TimeUnit;
import java.io.*;

public class RUStoreClient {

	/* any necessary class members here */
	private Socket clientSocket;
	private DataOutputStream out;
	private BufferedReader in;
	private DataInputStream input; 
	private String host;
	private int port;
	/**
	 * RUStoreClient Constructor, initializes default values
	 * for class members
	 *
	 * @param host	host url
	 * @param port	port number
	 */
	public RUStoreClient(String host, int port) {

		// Implement here
		this.host = host;
		this.port = port;

	}
	
	

	/**
	 * Opens a socket and establish a connection to the object store server
	 * running on a given host and port.
	 *
	 * @return		n/a, however throw an exception if any issues occur
	 */
	public void connect() throws UnknownHostException, IOException {

		// Implement here
		
		clientSocket = new Socket(host, port);
        out = new DataOutputStream(clientSocket.getOutputStream());
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        input = new DataInputStream(new BufferedInputStream(clientSocket. getInputStream()));
        

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT be 
	 * overwritten
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param data	byte array representing arbitrary data object
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 */
	public int put(String key, byte[] data) throws IOException, InterruptedException{

		// Implement here
		
		//First part of protocol: send the request type (P:put) and then the key 
		
		boolean doLog=false;
		String log_prefix="put : "+ key+ "  :" ;
		if(doLog)
			System.out.println(log_prefix + "sending request type P");
		
		String req = new String("P\n");
		out.writeBytes(req);
		
		if(doLog)
			System.out.println(log_prefix+ "sent requestType. sending num bytes in Key");
		
		//Send the number of bytes the key would take
		byte[] key_array = key.getBytes();
		out.writeBytes(key_array.length + "\n");
		
		if(doLog)
			System.out.println(log_prefix+ "sent number of bytes in key.");
		
		//Read a line to confirm that the key could be sent
		String key_res = in.readLine();
		
		if(doLog)
			System.out.println(log_prefix+ "got the  ack for key size. sending key ");
		
		out.write(key_array, 0, key_array.length);
		
		if(doLog)
			System.out.println(log_prefix+ "sent key. sending data length ");
		
		
		//Then send the size of data
		int size = data.length;
		//System.out.println("Size of data: " + size);
		out.writeInt(size);
		//System.out.println("data: " + new String(data));
		
		//TimeUnit.SECONDS.sleep(2);
		//receive ack that this size of data is permitted
		if(doLog)
			System.out.println(log_prefix+ "sent data length. " + size);
		
		String s = in.readLine();
		
		if(doLog)
			System.out.println(log_prefix+ "received ack for data size  " + s);
		
		//Now just write the bytes of the array into the socket
		out.write(data, 0, data.length);
		//System.out.println("reached here in the Client");
		//Read the return value from the server
		//According to the protocol, the response is an integer
		if(doLog)
			System.out.println(log_prefix+ "sent data");
		
		int output = Integer.parseInt(in.readLine());
		//System.out.println("reached output in the Client: " + output);
		
		if(doLog)
			System.out.println(log_prefix+ "received " + output);
		return output;
	

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT 
	 * be overwritten.
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param file_path	path of file data to transfer
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws InterruptedException 
	 */
	public int put(String key, String file_path) throws IOException, InterruptedException {

		// Implement here
		
		File f = new File(file_path);
		byte data[] = Files.readAllBytes(f.toPath());
		
		//reuse the above put function to store these bytes in the server
		/*
		byte data2[] = new byte[1024];
		
		for(int i = 0; i < 1024; i++) {
			data2[i] = data[i];
		}
		*/
		//System.out.println("Size of data2: " + data2.length);
		int result = put(key, data);
		
		return result;

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server.
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		object data as a byte array, null if key doesn't exist.
	 *        		Throw an exception if any other issues occur.
	 */
	public byte[] get(String key) throws IOException {

		// Implement here
		
		//send the type of request
		out.writeBytes("G\n");
		
		//Send the number of bytes the key would take
		byte[] key_array = key.getBytes();
		out.writeBytes(key_array.length + "\n");
				
		//Read a line to confirm that the key could be sent
		String key_res = in.readLine();
				
		out.write(key_array, 0, key_array.length);
		
		//receive the number of bytes of the value from the server
		//The server will send -1 if the key does not exist
		
		String line = in.readLine();
		int size = Integer.parseInt(line);
		
		//System.out.println("Size of GET Request data: " + size);
		
		if (size == -1) {
			return null;
		}
		
		//create an array of size length
		byte[] result = new byte[size];
		
		//send a line (say the size) to specify that the server could start sending all the bytes
		
		out.writeBytes(size + "\n");
		
		//collect the data
		//input.read(result, 0, size);
		
		boolean readDone = false;
		int numberOfBytesRead = 0;
		
		//System.out.println("Get: starting the read Loop: " + size);
		while(readDone == false)
		{
			int numberOfBytesToRead = size - numberOfBytesRead;
			//System.out.println("Get: Loop:  numberOfBytesToRead" + numberOfBytesToRead);
			int bytesRead = input.read(result,numberOfBytesRead,numberOfBytesToRead);
			numberOfBytesRead += bytesRead;
			//System.out.println("Get: Loop:  bytesRead" + bytesRead + ": numberOfBytesRead" +numberOfBytesRead);
			if(numberOfBytesRead == size)
				readDone = true;
			
		}
		
		
		return result;
		
		

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server and places it in a file. 
	 * 
	 * @param key	key associated with the object
	 * @param	file_path	output file path
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int get(String key, String file_path) throws IOException {

		
		// Implement here
		
		//reuse everything from previous function to get the bytes in the file
		
		byte [] file_data = get(key);
		
		if(file_data == null) {
			//key does not exist
			return 1;
		}
		
		//Write this byte array to a file
		File file = new File(file_path);
		FileOutputStream stream = new FileOutputStream(file);
		stream.write(file_data);
		stream.close();
		

		return 0;

	}

	/**
	 * Removes data object associated with a given key 
	 * from the object store server. Note: No need to download the data object, 
	 * simply invoke the object store server to remove object on server side
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int remove(String key) throws IOException {

		// Implement here
		
		//Send request type
		out.writeBytes("R\n");
		
		//Send the number of bytes the key would take
		byte[] key_array = key.getBytes();
		out.writeBytes(key_array.length + "\n");
						
		//Read a line to confirm that the key could be sent
		String key_res = in.readLine();
						
		out.write(key_array, 0, key_array.length);
		
		int result = Integer.parseInt(in.readLine());
		
		return result;

	}

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return		List of keys as string array, null if there are no keys.
	 *        		Throw an exception if any other issues occur.
	 * @throws IOException 
	 */
	public String[] list() throws IOException {

		// Implement here
		
		//Send request type
		out.writeBytes("L\n");
		
		int num_keys = Integer.parseInt(in.readLine());
		String[] keys = new String[num_keys];
		//System.out.println("List: Received number of keys: " + num_keys);
		
		if(num_keys == 0) {
			return null;
		}
		
		for(int i = 0; i < num_keys; i++) {
			//read the length of the key
			//System.out.println("List: Started an iteration");
			int key_size = Integer.parseInt(in.readLine());
			//System.out.println("List: Length of key: " + key_size);
			//Ack that you received size
			out.writeBytes(key_size + "\n");
			//System.out.println("List: ACKED Length of key");
			byte[] key = new byte[key_size];
			int read_size = input.read(key, 0, key_size);
			//System.out.println("List: Read bytes of key: " + read_size);
			String element = new String(key);
			//System.out.println("List: Element Recieved: " + element);
			keys[i] = element;
			out.writeBytes("Recieved a key\n");
	
		}
		
		return keys;

	}

	/**
	 * Signals to server to close connection before closes 
	 * the client socket.
	 * 
	 * @return		n/a, however throw an exception if any issues occur
	 */
	public void disconnect() throws IOException{

		// Implement here
		out.writeBytes("D\n");
		in.close();
		input.close();
        out.close();
        clientSocket.close();
		

	}

}
