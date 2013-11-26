package main.java.zkstress;


import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.zookeeper.KeeperException;

public class ZkWatchStress {

private int ThreadNo=0;
private String Znode= null;
private String ZkServer;

public ZkWatchStress(String ip,String node, int threads){
	
	this.ThreadNo = threads;
	this.Znode = node;
	this.ZkServer = ip;
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
    
    while ( executorPool.getActiveCount() >0 ){
    	//Thread.sleep(5000); 
    }
    //shut down the pool
    executorPool.shutdown();
    //shut down the monitor thread
    Thread.sleep(2000);
    monitor.shutdown();

    System.out.println("Finished all threads");

}


public class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        System.out.println(r.toString() + " is rejected");
    }
};


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
	        .help("-threads <Integer>");
	    parser.printHelp();
	
	
	
	Namespace res =parser.parseArgsOrFail(args); 
	System.out.println("Starting ACaZoo Stresser...");
	
	String ZKServ = res.getString("host");
	int threads = Integer.parseInt(res.getString("threads"));
	
	String root = new String("/");
	ZkWatchStress myStres = new ZkWatchStress(ZKServ,root, threads);
	try {
		myStres.RunAll();
	} catch (KeeperException e) {
		System.out.println("Zk Exception Running multiple Threads!");
	} catch (IOException e) {
		System.out.println("IO Exception Runnign multiple Threads!");
	} catch(InterruptedException e){
		System.out.println("InterruptedException - thread - Sleep!");
	}
	

	
	
}


}
