package com.RUStore;

/* any necessary Java packages here */
import java.net.*;
import java.io.*;
import java.util.*;

//HashMap<String, String> storage = new HashMap<String, String>(100, 2.0f);

public class RUStoreServer {

	/* any necessary class members here */
	
	private ServerSocket serverSocket;
	HashMap<String, byte[]> storage = new HashMap<String, byte[]>(100, 2.0f);

	/* any necessary helper methods here */
	public void start(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        while (true) {
        	
        	//This main iteration is for each client connection
            	
        	//accept connection and initialize input and output variables
        	//corresponding to the accepting socket
        	Socket clientSocket = serverSocket.accept();
        	DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
        	
        	//To read the input bytes
        	DataInputStream in = new DataInputStream(new BufferedInputStream(clientSocket. getInputStream()));
        	
        	//To read input lines
        	BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        	
        	//start a loop that communicates through this socket
        	//The loop ends when the client wants to end the communication
        	
        	while(true) {
        		
    			String req = input.readLine();
    			char c_req = req.charAt(0);
    			
    			//System.out.println("Request Code Received:" + c_req);
    			
    			
    			if (c_req == 'D') {
    				break;
    			}
    			
    			
    			switch(c_req) {
    			
    			
    				case 'P':
    				
    					
    					//System.out.println("Server: Received PUT Request");
    					
    					//Get the length of the string key
    					int key_size = Integer.parseInt(input.readLine());
    					//Confirm you got this length
    					out.writeBytes(key_size + "\n");
    					
    					
    					//Now, read in the value of the key
    					byte[] key_array = new byte[key_size];
    					in.read(key_array, 0, key_size);
    					String p_key = new String(key_array);
    					
    					
    					//System.out.println("Server: Received key: "+ p_key);
    					
    					
    					//Now take note of the size of the data
    					int size = in.readInt();
    					//System.out.println("Server: Size of data for put = " + size + "bytes");
    					
    					out.writeBytes(size + "\n");
    					
    					
    					//Read size number of bytes into a byte array
    					byte[] data = new byte[size];
    					//int bytesRead = in.read(data, 0, size);
    					//int a = in.read(data);
    					//System.out.println("Sever: Read the data from socket: bytes: "+bytesRead);
    					
    					//sample code
    					
    					boolean readDone = false;
    					int numberOfBytesRead = 0;
    					
    					//System.out.println("Server: starting the read Loop: " + size);
    					while(readDone == false)
    					{
    						int numberOfBytesToRead = size - numberOfBytesRead;
    						//System.out.println("Server: Loop:  numberOfBytesToRead" + numberOfBytesToRead);
    						int bytesRead = in.read(data,numberOfBytesRead,numberOfBytesToRead);
    						numberOfBytesRead += bytesRead;
    						//System.out.println("Server: Loop:  bytesRead" + bytesRead + ": numberOfBytesRead" +numberOfBytesRead);
    						if(numberOfBytesRead == size)
    							readDone = true;
    						
    					}
    					
    					//
    					
    					
    					
    					//System.out.println("reached here in the Server");
    					
    					
    					//Debugging: making sure the data is right
    					//System.out.println("Server: Received data:" + new String(data));
    					
    					//Default value of response: key already exists
    					int response = 1;
    					
    					//if key does not already exists insert it, and modify response
    					//Store (key, data) as (key, value) in the hash map
    					if(!(storage.containsKey(p_key))) {
    						storage.put(p_key, data);
    						response = 0;
    					}
    					
    					//System.out.println("Respose:" + response);
    					
    					//return your response to the client
    					out.writeBytes(response + "\n");
    					
    					break;
        			
    				case 'G':
    					
    					//System.out.println("Server: Received GET Request");

    					///Get the length of the string
    					key_size = Integer.parseInt(input.readLine());
    					//Confirm you got this length
    					out.writeBytes(key_size + "\n");
    					
    					
    					//Now, read in the value of the key
    					key_array = new byte[key_size];
    					in.read(key_array, 0, key_size);
    					String g_key = new String(key_array);
    					
    					//System.out.println("Server: Received key: " + g_key);
    					
    					byte[] value;
    					
    					
    					//Check if the key is contained in storage and return corresponding size
    					if(storage.containsKey(g_key)) {
    						value = storage.get(g_key);
    						//System.out.println("Server: Size of data for get: " + value.length);
    						out.writeBytes(value.length + "\n");
    					}else {
    						out.writeBytes(-1 + "\n");
    						//Nothing to do
    						break;
    					}
    					
    					//read in a character after which you can start sending the data
    					
    					String s = input.readLine();
    					//char c_s = s.charAt(0);
    					
    					//Write the data in value to the output stream
    					out.write(value, 0, value.length);
    					
    					break;
    				
    				
    				case 'R':
    				
    					//System.out.println("Server: Received Remove Request");
    					
    					///Get the length of the string
    					key_size = Integer.parseInt(input.readLine());
    					//Confirm you got this length
    					out.writeBytes(key_size + "\n");
    					
    					
    					//Now, read in the value of the key
    					key_array = new byte[key_size];
    					in.read(key_array, 0, key_size);
    					String r_key = new String(key_array);
    					
    					//System.out.println("Server: Received key: " + r_key);
    					
    					if(storage.containsKey(r_key)) {
    						storage.remove(r_key);
    						out.writeBytes(0 + "\n");
    					}else {
    						out.writeBytes(1 + "\n");
    					}
    					
    					break;
    				
    				case 'L':
    					
    					//System.out.println("Server: Received List Request");
    					
    					//send the number of keys
    					out.writeBytes(storage.size() + "\n");
    					//System.out.println("Server: Sent the number of keys: " + storage.size());

    					//for each key in storage write it to output
    					for(String e: storage.keySet()) {
    						//Send the length of the key first
    						//System.out.println("Server: List: key being dealt with: " + e);
    						byte[] key = e.getBytes();
    						//System.out.println("Server: List: " + e + ": length" + key.length + "bytes");
    						out.writeBytes(key.length + "\n");
    						
    						
    						//System.out.println("Server: List: " + e + ": sent the key length");
    						String key_res = input.readLine();
    						//System.out.println("Server: List: " + e + ": got ack for key length :" + key_res);
    						out.write(key, 0, key.length);
    						//System.out.println("Server: List: " + e + ": wrote the key into the socket");
    						key_res = input.readLine();
    						
    					}
    					
 
    			}

    		}
        	
        	//Since this connection is over, close the accepting socket and the input/output variables
        	in.close();
        	input.close();
    		out.close();
    		clientSocket.close();
     	
        	
        }
            
    }
	
	
	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 */
	public static void main(String args[]){

		// Check if at least one argument that is potentially a port number
		if(args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}

		// Try and parse port # from argument
		int port = Integer.parseInt(args[0]);


		// Implement here //
		
		RUStoreServer server = new RUStoreServer();
		try {
			server.start(port);
		}catch(IOException e) {
			System.out.println("Failed to create a listening port/accepting a connection");
		}
		
		

	}

}
