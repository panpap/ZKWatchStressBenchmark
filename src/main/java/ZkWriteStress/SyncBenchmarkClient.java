package ZkWriteStress;
import java.io.IOException;
import org.apache.log4j.Logger;



public class SyncBenchmarkClient extends BenchmarkClient {

	public static long StartTime;
	private boolean done;
	static int TimeToLast;

	private static final Logger LOG = Logger.getLogger(SyncBenchmarkClient.class);

	
	public SyncBenchmarkClient(String host, String namespace,
			int time, int id) throws IOException {
		super(host, namespace, time, id);
	}
	
	@Override
	protected void submit(int n) {
		StartTime = System.currentTimeMillis();
		try {
			submitWrapped(n);
		} catch (Exception e) {
			// What can you do? for some reason
			// com.netflix.curator.framework.api.Pathable.forPath() throws Exception
			LOG.error("Error while submitting requests", e);
			System.err.println("Error while submitting requests");
		}
	}
		
	protected void submitWrapped(int n) throws Exception {
		TimeToLast = n;
		done = false;
		byte data[];
		long i = 0; 
		System.out.print("~");
		while(((System.currentTimeMillis() - StartTime)/1000) <TimeToLast ) {
			data = new String("pgaref AcaZoo DATAAAAA"+i++).getBytes();
			_client.create().forPath(_path + "/" + _count, data);
			System.out.print("~");
			_count++;

			if (done)
				break;
		}
		
	}
	
	@Override
	protected void finish() {
		done = true;
	}
}
