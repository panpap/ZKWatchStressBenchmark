package main.java.zkstress;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import measurements.Measurements;
import measurements.MeasurementsExporter;
import measurements.TextMeasurementsExporter;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.zookeeper.KeeperException;

public class ZkWatchStress {

private static int ThreadNo=0;
private String Znode= null;
private String ZkServer;
private static long StartTime;
private static long TotalTime;
static Measurements _measurements;
public static int opcount;

public ZkWatchStress(String ip,String node, int threads, int time){
	opcount=0;
	this.ThreadNo = threads;
	this.Znode = node;
	this.ZkServer = ip;
	this.TotalTime = time;
	_measurements = Measurements.getMeasurements();
}
	
public void RunAll() throws KeeperException, IOException, InterruptedException{
	//RejectedExecutionHandler implementation
    RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();
    //Get the ThreadFactory implementation to use
    ThreadFactory threadFactory = Executors.defaultThreadFactory();
    //creating the ThreadPoolExecutor
    ThreadPoolExecutor executorPool = new ThreadPoolExecutor(this.ThreadNo/2, this.ThreadNo, 10,
				TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(8),
				threadFactory, rejectionHandler);
    //start the monitoring thread
    
    MyMonitorThread monitor = new MyMonitorThread(executorPool, 3);
    Thread monitorThread = new Thread(monitor);
    monitorThread.start();
    
    //submit work to the thread pool
    for(int i=0; i<this.ThreadNo; i++){
    	System.out.println("Running: "+ i);
        executorPool.execute(new Executor(this.ZkServer, this.Znode));
    }
    StartTime = System.currentTimeMillis();
    
    while ( ((System.currentTimeMillis() - StartTime)/1000) < TotalTime ){
    	//Thread.sleep(5000); 
    }
    //shut down the pool
    
    executorPool.shutdown();
    //shut down the monitor thread
    //Thread.sleep(2000);
    monitor.shutdown();

    System.out.println("Finished all threads");
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
private static void exportMeasurements()
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
	
	ArgumentParser parser = ArgumentParsers.newArgumentParser("run")
	        .defaultHelp(true);
	    parser.addArgument("--host")
	        .setDefault("host","109.231.85.43")
	     //   .action(Arguments.append())
	        .help("-host <IP>");
	    parser.addArgument("--threads")
	        .setDefault("threads","64")
	        //.action(Arguments.append())
	        .help("-threads <Number>");
	    parser.addArgument("--time")
        .setDefault("time","10")
        //.action(Arguments.append())
        .help("-time <Seconds>");
	    parser.printHelp();
	
	
	
	Namespace res =parser.parseArgsOrFail(args); 
	System.out.println("Starting ACaZoo Stresser...");
	
	String ZKServ = res.getString("host");
	int threads = Integer.parseInt(res.getString("threads"));
	int time = Integer.parseInt(res.getString("time"));
	
	
	
	String root = new String("/");
	ZkWatchStress myStres = new ZkWatchStress(ZKServ,root, threads,time);
	long now = System.currentTimeMillis();
	_measurements.measure("RespTime",(int)now);
	
	try {
		myStres.RunAll();
	} catch (KeeperException e) {
		System.out.println("Zk Exception Running multiple Threads!");
	} catch (IOException e) {
		System.out.println("IO Exception Runnign multiple Threads!");
	} catch(InterruptedException e){
		System.out.println("InterruptedException - thread - Sleep!");
	}
	

	try
	{
		exportMeasurements();
	} catch (IOException e)
	{
		System.err.println("Could not export measurements, error: " + e.getMessage());
		e.printStackTrace();
		System.exit(-1);
	}
	System.exit(0);
	
}


}
