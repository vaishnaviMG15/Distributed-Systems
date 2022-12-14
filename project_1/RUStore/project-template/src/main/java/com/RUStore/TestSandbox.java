package com.RUStore;

/**
 * This TestSandbox is meant for you to implement and extend to 
 * test your object store as you slowly implement both the client and server.
 * 
 * If you need more information on how an RUStorageClient is used
 * take a look at the RUStoreClient.java source as well as 
 * TestSample.java which includes sample usages of the client.
 */
public class TestSandbox{

	public static void main(String[] args) {

		// Create a new RUStoreClient
		RUStoreClient client = new RUStoreClient("localhost", 12345);

		// Open a connection to a remote service
		System.out.println("Connecting to object server...");
		try {
			client.connect();
			System.out.println("Established connection to server.");
			int a = client.put("key1", "Hello World".getBytes());
			//System.out.println("Result of the put:" + a);
			byte[] a2 = client.get("key1");
			System.out.println("Result of the get:" + new String(a2));
			//int a3 = client.remove("key1");
			//System.out.println("Result of the remove:" + a3);
			//a2 = client.get("key1");
			//if(a2 == null) {
			//	System.out.println("Successfully removed key1");
			//}
			client.put("ke\ny2", "1234".getBytes());
			client.put("key3\n", "Vaishnavi".getBytes());
			String[] a3 = client.list();
			for(int i = 0; i<a3.length; i++) {
				System.out.println(a3[i]);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to connect to server.");
		}
		
		//code I added
		try {
			client.disconnect();
			System.out.println("Broke connection to server.");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed to disconnect from server.");
		}

	}

}
