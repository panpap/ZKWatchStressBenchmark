package main.java.zkstress;


/**
 * A simple class that monitors the data and existence of a ZooKeeper
 * node. It uses asynchronous ZooKeeper APIs.
 */
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ChildrenMonitor implements Watcher, AsyncCallback.ChildrenCallback {

    ZooKeeper zk;

    String znode;

    Watcher chainedWatcher;

    boolean dead;

    ChildrenMonitorListener listener;
    
    long LastEvent;

    List<String> prevData;
    
    
    public ChildrenMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher,
    		ChildrenMonitorListener listener) throws KeeperException, InterruptedException {
        this.zk = zk;
        this.znode = znode;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
        this.LastEvent = -1;
        // Get things started by checking if the node exists. We are going
        // to be completely event driven
        zk.getChildren(znode, true, this, null);
    }

    /**
     * Other classes use the DataMonitor by implementing this method
     */
    public interface ChildrenMonitorListener {
        /**
         * The existence status of the node has changed.
         */
        void exists(List<String> data);

        /**
         * The ZooKeeper session is no longer valid.
         *
         * @param rc
         *                the ZooKeeper reason code
         */
        void closing(int rc);
    }

    public void process(WatchedEvent event) {
        String path = event.getPath();
        System.out.println("Event Type: " + event.getType()  + " Path: "+ path  + "State: "+ event.getState());
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            case SyncConnected:
                // In this particular example we don't need to do anything
                // here - watches are automatically re-registered with 
                // server and any watches triggered while the client was 
                // disconnected will be delivered (in order of course)
            	zk.getChildren(znode, true, this, null);
                break;
            case Expired:
                // It's all over
                dead = true;
                listener.closing(KeeperException.Code.SessionExpired);
                break;
            default:
            	zk.getChildren(znode, true, this, null);
            	break;
            }
        } else {
            if (path != null && path.equals(znode)) {
                // Something has changed on the node, let's find out
                zk.getChildren(znode, true, this, null);
				
            }
        }
        if (chainedWatcher != null) {
            chainedWatcher.process(event);
        }
    }
    /*
    public void processResult(int rc, String path, Object ctx, Stat stat) {
    	System.out.println("ProcessResult: "+ path);
        boolean exists;
        switch (rc) {
        case Code.Ok:
            exists = true;
            break;
        case Code.NoNode:
            exists = false;
            break;
        case Code.SessionExpired:
        case Code.NoAuth:
            dead = true;
            listener.closing(rc);
            return;
        default:
            // Retry errors
            zk.exists(znode, true, this, null);
            return;
        }

        byte b[] = null;
        if (exists) {
            try {
                b = zk.getData(znode, false, null);
            } catch (Exception e) {
                // We don't need to worry about recovering now. The watch
                // callbacks will kick off any exception handling
                e.printStackTrace();
                return;
            }
        }
        if ((b == null && b != prevData)
                || (b != null && !Arrays.equals(prevData, b))) {
            listener.exists(b);
            prevData = b;
            String ringstate = null;
			try {
				ringstate = new String(prevData, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            System.out.println("New Data: "+  ringstate);

        }
    }*/

	@Override
	public void processResult(int rc, String path, Object ctx,
			List<String> children) {
		long now = System.currentTimeMillis();
		if((this.LastEvent != -1))
			ZkWatchStress._measurements.measure("RespTime", (int)(now-this.LastEvent));
		this.LastEvent = now;
		
		//System.out.println("New Watcher Data: "+  children.toString());
		System.out.print(".");
		
		//set again
		boolean exists;
        switch (rc) {
        case Code.Ok:
            exists = true;
            break;
        case Code.NoNode:
            exists = false;
            break;
        case Code.SessionExpired:
        case Code.NoAuth:
            dead = true;
            listener.closing(rc);
            return;
        default:
            // Retry errors
        	zk.getChildren(znode, true, this, null);
            return;
        }
        List<String> b = null;
        if (exists) {
            try {
                b = zk.getChildren(znode, false, null);
            } catch (Exception e) {
                // We don't need to worry about recovering now. The watch
                // callbacks will kick off any exception handling
                e.printStackTrace();
                return;
            }
        }
        if ((b == null && b != prevData)
                || (b != null && !(prevData.containsAll(b) && b.containsAll(prevData)))) {
            listener.exists(b);
            prevData = b;
            System.out.println("New Data: "+  b.toString());

        }
		zk.getChildren(znode, true, this, null);
		
	}
}