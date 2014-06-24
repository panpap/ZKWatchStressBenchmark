package ZkWriteStress;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Timer;

import main.java.zkstress.ZkWatchStress;

import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;


public abstract class BenchmarkClient implements Runnable {
	protected String _host; // the host this client is connecting to
	protected CuratorFramework _client; // the actual client
	protected String _path;
	protected int _id;
	protected long _count;
	protected int _countTime;
	protected int _time;
	protected int _highestN;
	protected int _highestDeleted;
	
	protected BufferedWriter _latenciesFile;
	
	private static final Logger LOG = Logger.getLogger(BenchmarkClient.class);


	public BenchmarkClient( String host, String namespace,
			int time, int id) throws IOException {
		_time = time;
		_host = host;
		_client = CuratorFrameworkFactory.builder()
			.connectString(_host).namespace(namespace)
			.retryPolicy(new RetryNTimes(Integer.MAX_VALUE,1000))
			.connectionTimeoutMs(5000).build();
		_id = id;
		_path = "/client"+id;
		_highestN = 0;
		_highestDeleted = 0;
	}
	
	@Override
	public void run() {
		if (!_client.isStarted())
			_client.start();		
		
		_count = 0;
		_countTime = 0;
		
		// Create a directory to work in

		try {
			Stat stat = _client.checkExists().forPath(_path);
			if (stat == null) {
				_client.create().forPath(_path, "this Is test DAta ONly!!!".getBytes());
			}
		} catch (Exception e) {
			LOG.error("Error while creating working directory", e);
		}
		
		//run in seconds.
		submit(_time);
		try {
			deleteChildren();
		} catch (Exception e) {
			System.out.println("Could not delete Children");
		}
		
		System.out.println("Zk -> Write CLient Done!");
		ZkWatchStress.done = true;
		ZkWatchStress.executorPool.shutdownNow();
		return;
		
	}

	/* Delete all the child znodes created by this client */
	void deleteChildren() throws Exception {
		List<String> children;

		do {
			children = _client.getChildren().forPath(_path);
			for (String child : children) {
				_client.delete().inBackground().forPath(_path + "/" + child);
			}
			Thread.sleep(2000);
		} while (children.size() != 0);
	}





	int getTimeCount() {
		return _countTime;
	}
	
	long getOpsCount(){
		return _count;
	}
	

	abstract protected void submit(int n);

	
	abstract protected void finish();
}
