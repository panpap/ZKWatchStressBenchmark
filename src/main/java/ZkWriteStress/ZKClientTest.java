package ZkWriteStress;

import java.io.IOException;

public class ZKClientTest {

	
	
	
	
	public static void main(String[] args) {
		try {
			Thread tmp = new Thread(new SyncBenchmarkClient("109.231.85.43", "/zkTest", 10000, 750));
			tmp.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
