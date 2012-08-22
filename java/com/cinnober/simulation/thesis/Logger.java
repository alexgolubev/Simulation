package com.cinnober.simulation.thesis;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.cinnober.simulation.thesis.Simulation.Request;
import com.cinnober.simulation.thesis.Simulation.ServerBase;
import com.cinnober.simulation.thesis.Simulation.Request.Track;


/**
 *
 * @author alexander.golubev, Cinnober Financial Technology
 */
public class Logger {

    private static volatile Logger cLogger; 
    
    Date mDate = new Date();
    long mRealStartTime;

    SimpleDateFormat mDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    DecimalFormat mDecimalFormat  = new DecimalFormat();
    
    private Logger(long pSimulationStartTime) {
        
        mRealStartTime = pSimulationStartTime;
        
        mDecimalFormat.setMaximumFractionDigits(9);
        mDecimalFormat.setMinimumFractionDigits(9);

        mDecimalFormat.setMaximumIntegerDigits(100);
        mDecimalFormat.setMinimumIntegerDigits(1);
        mDecimalFormat.setGroupingSize(0);
    }
    
    public static Logger getSingleton() {
        if (cLogger == null) {
            synchronized (Logger.class) {
                if (cLogger == null) {
                    cLogger = new Logger(System.currentTimeMillis());
                }
            }
        }
        return cLogger;
    }
    
    public void logTransactionMonitorHeader() {
        
        System.out.println(
            "time;eligible;duration;excuses;result;transid;ip:port;user;req;rsp;partition;obid;conn;osize;meta");
        System.out.println(
            "-------------------------------------------------------------------------------------------------");
    }   
    
    /**
     * Prints a transaction in transaction monitor style.
     * @param pRequest
     */
    public void logTransaction(Request pRequest)
    {
        long tMilliTime = mRealStartTime + (long) (pRequest.mTimeOut * 0.001);
        int tMicroTime  = 1000 + (int) pRequest.mTimeOut % 1000;
        String tMicro = Integer.toString(tMicroTime).substring(1, 4);
        
        // time;eligible;duration;excuses;result;transid;ip:port;user;req;rsp;partition;obid;conn;osize;meta
        mDate.setTime(tMilliTime);
        String tTime = mDateFormat.format(mDate) + tMicro;
        int tTaxTime = (int) (pRequest.mTimeOut -  pRequest.mTimeIn);
        System.out.println(tTime + ";y;" + tTaxTime + ";;3001;" + pRequest.mRequestNum + ";;" 
            + pRequest.mClient + ";;;1;;" + pRequest.mClient + ";;");
    }
    
    public void logTrack(Track pTrack) {
        
        System.out.println(mDecimalFormat.format(pTrack.getTime()) + " " + pTrack.getMsg());
    }
    
    public void logTrackWithReqNum(int pReqNum, Track pTrack) {
        
        System.out.println(mDecimalFormat.format(pTrack.getTime()) + " " + pReqNum + " " + pTrack.getMsg());
    }
    
    public void logRequestInAndOut(Request pRequest, int pReqNum) {
        
        System.out.println("\n" + pRequest.mClient + ", Request " + pReqNum 
            + ", which is request #" + pRequest.mRequestNum + " in all incoming transactions");
        System.out.println("Request creation time " + mDecimalFormat.format(pRequest.mCreateTime));
        System.out.println("TimeIn to TAX " + mDecimalFormat.format(pRequest.mTimeIn));
        System.out.println("TimeOut from TAX " + mDecimalFormat.format(pRequest.mTimeOut) + "\n");
    }   
    
    /**
     * Prints the results of simulation run.
     * 
     * @param pSimTime - Simulation time in microseconds
     * @param pSentRequestsNum - total number of requests sent
     * @param pTaxTime - Total time requests spent in the TAX server
     * @param pClientTime - Total time from sending the requests till all the responses come back to the clients
     */
    public void logSimulationFinished(double pSimTime, int pSentRequestsNum, double pTaxTime, double pClientTime) {
        
        System.out.println("\nSimulation time = " + pSimTime / (1000.0 * 1000.0) + " sec.");
        System.out.println("Sent requests = " + pSentRequestsNum);
        System.out.println("Average TAX time = " + pTaxTime / pSentRequestsNum + " micros");
        System.out.println("Average client time = " + pClientTime / pSentRequestsNum + " micros");
        System.out.println("Execution time: " 
            + ((System.currentTimeMillis() -  mRealStartTime) / 1000.0) + " secs.\n"); 
    }

    public void logSimTimeTooShort(double pNewSimTime) {
        
        System.out.println("\nSimulation time is too short! It has been reset to " 
            + pNewSimTime + " micro\n");
    }
    
    public void logMaxNumOfRequests(int pMaxNumOfRequests) {
        
        System.out.println("Maximum number of requests to send is " + pMaxNumOfRequests);
    }
    
    public void logMaxNumOfRequestsPerClient(int pMaxNumOfRequests) {
        
        System.out.println("Maximum number of requests to send for a client is " + pMaxNumOfRequests);
    }
    
    public void logGcBlock(ServerBase pService, double pTime, double pBlockTime) {
        
        System.out.println(mDecimalFormat.format(pTime) + " " + pService.mName + " was blocked for " + pBlockTime 
            + " micros");
    }
}
