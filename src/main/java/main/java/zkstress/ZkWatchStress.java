package main.java.zkstress;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import measurements.Measurements;
import measurements.MeasurementsExporter;
import measurements.TextMeasurementsExporter;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.zookeeper.KeeperException;

import ZkWriteStress.SyncBenchmarkClient;

public class ZkWatchStress {

private static int ThreadNo=0;
private String Znode= null;
private String ZkServer;
public static boolean done = false;
private static long StartTime;
private static long TotalTime;
static Measurements _measurements;
public static int opcount;
public static ThreadPoolExecutor executorPool;
private static final Object lock = new Object();
private boolean LoadBalance;
private static Thread stresser =null;

private static String ZKServ;
private static int time;





public ZkWatchStress(String ip,String node, int threads, int time, boolean lb){
	opcount=0;
	ThreadNo = threads;
	this.Znode = node;
	this.ZkServer = ip;
	TotalTime = time;
	this.LoadBalance = lb;
	_measurements = Measurements.getMeasurements();
}
	
public void RunAll() throws KeeperException, IOException, InterruptedException{
	//RejectedExecutionHandler implementation
    RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();
    //Get the ThreadFactory implementation to use
    ThreadFactory threadFactory = Executors.defaultThreadFactory();
    //creating the ThreadPoolExecutor
     executorPool = new ThreadPoolExecutor(ThreadNo, ZkWatchStress.ThreadNo, 10,
				TimeUnit.SECONDS, new  LinkedBlockingQueue<Runnable>(),
				threadFactory, rejectionHandler);
    //start the monitoring thread
    
    MyMonitorThread monitor = new MyMonitorThread(executorPool, 3);
    Thread monitorThread = new Thread(monitor);
    monitorThread.start();
    
    
    if(this.LoadBalance){
    	//for Load balanced
    	for(int i=0; i<(ThreadNo/3); i++){
    		
    		Executor tmp = new Executor("10.254.1.2", this.Znode);
            executorPool.execute(tmp);
            if(i ==1 ){
            	ChildrenMonitor.myID = tmp.chm.hashCode();
        	}
            executorPool.execute(new Executor("10.254.1.4", this.Znode));
            executorPool.execute(new Executor("10.254.1.5", this.Znode));
    	}
    	
    	
    }
    else{
    	//submit work to the thread pool Not Balanced
        for(int i=0; i<this.ThreadNo; i++){
        	
        	Executor tmp = new Executor(this.ZkServer, this.Znode);
            executorPool.execute(tmp);
            
            if(i ==1 ){
            	ChildrenMonitor.myID = tmp.chm.hashCode();;
        	}
        }
    }
    
    System.out.println("All Watch Threads Started!");
    
    /*
	 * Start Stresser!!!
	 */
	
	
	try {
		stresser = new Thread(new SyncBenchmarkClient(ZKServ, "/zkTest", time, 750));
		stresser.setPriority(Thread.MAX_PRIORITY);
		stresser.start();
		System.out.println("Stresser Started!");
	} catch (IOException e) {
		System.out.println("ZK currator error "+ e);
	}
	
	
    
    
    
    
    
    SyncBenchmarkClient.StartTime = System.currentTimeMillis();
    
    
    StartTime = System.currentTimeMillis();
    
    while ( (((System.currentTimeMillis() - StartTime)/1000) < TotalTime) && !done ){
    	//Thread.sleep(100); 
    	
    }
    //shut down the pool
    
    executorPool.shutdown();
    //shut down the monitor thread
    //Thread.sleep(2000);
    monitor.shutdown();

    System.out.println("Finished all Watch - threads");
    this.done= true;
    return;

}


public class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        System.out.println(r.toString() + " is rejected");
    }
};



/**
 * Exports the measurements to either sysout or a file using the exporter
 * loaded from conf.
 * @throws IOException Either failed to write to output stream or failed to close it.
 */
private static synchronized void exportMeasurements()
		throws IOException
{
	MeasurementsExporter exporter = null;
	try
	{
		// if no destination file is provided the results will be written to stdout
		OutputStream out;
		String exportFile = "AcaZooStats.txt";
		/*
		 * pgaref Should change here!
		 */
		
		if (exportFile == null)
		{
			out = System.out;
		} else
		{
			out = new FileOutputStream(exportFile);
		}

		exporter = new TextMeasurementsExporter(out);

		exporter.write("OVERALL", "RunTime(ms)", (System.currentTimeMillis()- StartTime));
		double throughput = ((double) opcount) / ((double) TotalTime);
		exporter.write("OVERALL", "Throughput(watch notifications/sec)", throughput);
		exporter.write("OVERALL", "Threads No", ThreadNo);

		Measurements.getMeasurements().exportMeasurements(exporter);
	} finally
	{
		if (exporter != null)
		{
			exporter.close();
		}
	}
	return;
}


public static void main(String [] args ){
	Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
	
	ArgumentParser parser = ArgumentParsers.newArgumentParser("run")
	        .defaultHelp(true);
	    parser.addArgument("--host")
	        .setDefault("host","10.254.1.2")
	     //   .action(Arguments.append())
	        .help("-host <IP>");
	    parser.addArgument("--threads")
	        .setDefault("threads","64")
	        //.action(Arguments.append())
	        .help("-threads <Number>");
	    parser.addArgument("--time")
        .setDefault("time","10")
        //.action(Arguments.append())
        .help("--time <Seconds>");
	    parser.addArgument("--LB")
	    .action(Arguments.storeTrue())
	    .help("--LB for LoadBalancing");
	    parser.printHelp();
	
	
	
	Namespace res =parser.parseArgsOrFail(args); 
	System.out.println("Starting ACaZoo Stresser...");
	
	ZKServ = res.getString("host");
	int threads = Integer.parseInt(res.getString("threads"));
	time = Integer.parseInt(res.getString("time"));
	boolean gotlb = res.getBoolean("LB");
	
	
	/*
	 * Now Start Watcher
	 * 
	 */
	String root = new String("/zkTest/client750");
	ZkWatchStress myStres = new ZkWatchStress(ZKServ,root, threads,time, gotlb);
	
	/*
	 * First Start Watchers and then Stresser!!!!!!
	 * 
	 */
	
	try {
		myStres.RunAll();
	} catch (KeeperException e) {
		System.out.println("Zk Exception Running multiple Threads!");
	} catch (IOException e) {
		System.out.println("IO Exception Runnign multiple Threads!"+e.getMessage());
	} catch(InterruptedException e){
		System.out.println("InterruptedException - thread - Sleep!");
	}
	System.out.println("Watcher Threads Done! ");

	
	
	/*
	 * Force shutdown!
	 */
	stresser.stop();
	executorPool.shutdown();
	
	try {
		Thread.sleep(2000);
	} catch (InterruptedException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	
	try
	{
		synchronized(lock){
			exportMeasurements();
		}
	} catch (IOException e)
	{
		System.err.println("Could not export measurements, error: " + e.getMessage());
		e.printStackTrace();
		System.exit(-1);
	}
	System.out.println("All done!");
	System.exit(0);
	
}


}
