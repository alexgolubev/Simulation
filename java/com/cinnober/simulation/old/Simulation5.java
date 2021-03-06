 
package com.cinnober.simulation.old;

import java.util.ArrayList;

import dk.ruc.javaSimulation.*;
import dk.ruc.javaSimulation.Process;


/*
* Simulation of a queuing system.
*
* This version models is based on passive customers 
* and active servers.
*/

public class Simulation5 extends Process {
    static boolean cTrace = false;
    static String cBlanks = "                   z                     ";

    double mSum;
    double mQueuSum;
    int mRequests = 0;

    int mTaxes = 10;
    int mMes = 10;
    Random mRandom = new Random(7);
    
    Head mTaxServers = new Head();
    Head mMeServers = new Head();
    
    Head mTaxRequestQueue = new Head();
    Head mMeRequestQueue = new Head();

    long mStartTime = System.currentTimeMillis();
    int mRequestsToSend = 5*60*1000;
    boolean mFixedServiceTimed = false;
    ArrayList<GcService> mGcList = new ArrayList<GcService>();

    public void actions() {
        double tGcTime = 20;
        Service tService;
        
        for (int i = 1; i <= mTaxes; i++)  {
            tService = new Service("Tax"+i, 0.25, 0.25, mTaxRequestQueue, mTaxServers, mMeRequestQueue, mMeServers);
            tService.into(mTaxServers);
            tService.mTakeTime = true;
            if (tGcTime != 0) {
                GcService tTmp = new GcService(tService, 1000, tGcTime); 
                mGcList.add(tTmp);
                activate(tTmp);
            }
        }
        
        for (int i = 1; i <= mMes; i++) { 
            tService = new Service("Me"+i, 0.5, 0.5, mMeRequestQueue, mMeServers, null, null);
            tService.into(mMeServers);
            if (tGcTime != 0) {
                GcService tTmp = new GcService(tService, 1000, tGcTime); 
                mGcList.add(tTmp);
                activate(tTmp);
            }
        }
        activate(new ClientRequestGenerator());
        hold(1000*1000);
        System.out.println("Requests = " + mRequests);
        System.out.println("Av.elapsed time = " + mSum/mRequests);
        System.out.println("Av.queue time = " + mQueuSum/mRequests);
        System.out.println("\nExecution time: " + ((System.currentTimeMillis() - mStartTime)/1000.0) + " secs.");
    }

    // Generate message, send into queue eaten from any tax
    class ClientRequestGenerator extends Process {
        public void actions() {
            for (int i = 0; i < mRequestsToSend; ++i ) {
                new Request().into(mTaxRequestQueue);
                if (!mTaxServers.empty()) 
                    activate((Service) mTaxServers.first());
                hold(mRandom.negexp(1));
//                hold(mRandom.uniform(10, 11));
            }
            for (GcService tGcService : mGcList) {
                cancel(tGcService);
            }
        }
    }

    class Request extends Link {
        int mRequest = ++mRequests;
        double mCreateTime = time();
        double mTimeIn;
        double mTimeOut;
        // mWaitList contains the waiting servers.
        Head mWaitList = new Head();
        int mIndent = 0;
    }

    class GcService extends Process {
        Service mService;
        double mGcInterval;
        double mBlockTime;
        
        public GcService(Service pService, double pGcInterval, double pBlockTime)
        {
            super();
            mService = pService;
            mGcInterval = pGcInterval;
            mBlockTime = pBlockTime;
        }

        public void actions() {
            double tEndOfBreak;
            while (true) {
//                hold(mRandom.negexp(1.0/mGcInterval));
                hold(mGcInterval);
                // if idle, set an earliest start time.
                // if not idle, updated with new time if past current schedule
                tEndOfBreak = mBlockTime + time();
                if (mService.idle()) {
                    mService.mBlockedTime = tEndOfBreak;
                    if (cTrace)
                        System.out.println(mService.mName + " Gc block " + time() + " " + tEndOfBreak);
                }
                else {
                    if (tEndOfBreak <= mService.evTime()) {
                        tEndOfBreak = mService.evTime();
                    }
                    if (cTrace)
                        System.out.println(mService.mName + " Gc extend " + time() + " " + mService.evTime() + " -> "+ tEndOfBreak);
                    reactivate(mService, at, tEndOfBreak);
                }

            }
        } 
        
    }
    public static String ind(int pLen) {
        return cBlanks.substring(0, pLen);
    }
    
    class Service extends Process {
        String mName;
        Head mInServerQ;
        Head mInQ;
        Head mOutServerQ;
        Head mOutQ;
        double mServiceTime1;
        double mStdev1;
        double mServiceTime2;
        double mStdev2;
        double mBlockedTime;
        boolean mTakeTime;
        
        public Service(String pName, double pServiceTime1, double pServiceTime2, Head pInQ, Head pInServerQ, Head pOutQ, Head pOutServerQ)
        {
            super();
            mServiceTime1 = pServiceTime1;
            mStdev1 = pServiceTime1 * 0.3;
            mServiceTime2 = pServiceTime2;
            mStdev2 = pServiceTime2 * 0.3;
            mName = pName;
            mInServerQ = pInServerQ;
            mInQ = pInQ;
            mOutQ = pOutQ;
            mOutServerQ = pOutServerQ;
        }

        public void actions() {
            
            while (true) {
                out();

                while (!mInQ.empty()) {
                    // If we were in GC-block while idle await GC completion.
                    if (mBlockedTime != 0) {
                        double tTmp = mBlockedTime;
                        mBlockedTime = 0;
                        hold(tTmp - time());
                        continue;
                    }

                    // Get request and remove it from queue.
                    Request tReq = (Request) mInQ.first();
                    tReq.out();

                    // Save time for roundtrip
                    if (mTakeTime) {
                        tReq.mTimeIn = time();
                    }

                    if (cTrace)
                        System.out.println(ind(tReq.mIndent++) + mName + " Req " + tReq.mRequest + " " + time());

                    // Wait pre service time
                    if (mFixedServiceTimed)
                        hold(mServiceTime1);
                    else
                        hold(mRandom.normal(mServiceTime1, mStdev1));
                    
                    if (cTrace)
                        System.out.println(ind(tReq.mIndent++) + mName + " Sv1 " + tReq.mRequest + " " + time());

                    // If requested, send message to next server 
                    if (mOutQ != null) {
                        tReq.into(mOutQ);
                        // Reqactivate calling server
                        if (!mOutServerQ.empty()) { 
                            activate((Service) mOutServerQ.first());
                        }

                        // Wait for completion of request, i.e, activated by called server and handling GC delay
                        wait(tReq.mWaitList);

                        if (cTrace)
                            System.out.println(ind(tReq.mIndent++) + mName + " Rsp " + tReq.mRequest + " " + time());

                        
                        // Wait post service time
                        if (mFixedServiceTimed)
                            hold(mServiceTime2);
                        else
                            hold(mRandom.normal(mServiceTime2, mStdev2));

                        if (cTrace)
                            System.out.println(ind(tReq.mIndent++) + mName + " Sv2 " + tReq.mRequest + " " + time());

                        // Collect roundtrip statistics
                        if (mTakeTime) {
                            tReq.mTimeOut = time();
                            double tTime = tReq.mTimeOut - tReq.mTimeIn;
                            mSum += tTime;
                            double tQtime = tReq.mTimeOut - tReq.mCreateTime;
                            mQueuSum += tQtime;
                            if (cTrace)
                                System.out.println(ind(tReq.mIndent++) + mName + " Time " + tReq.mRequest +
                                        " " + tTime + " q: " + tQtime);
                        } 
                    }
                    
                    // Reqactivate calling server with potential gc delay
                    if (!tReq.mWaitList.empty()) { 
                        Service tService = (Service)tReq.mWaitList.first();

                        // Just activate if no GC occurred while idle
                        // else calculate earliest time when the server can be activated and activate.
                        if (tService.mBlockedTime == 0) {
                            activate(tService);
                        }
                        else {
                            double tBlockedTime = tService.mBlockedTime;
                            tService.mBlockedTime = 0;
                            activate(tService, at, tBlockedTime);
                        }
                    }

//                    if (!mInServerQ.empty()) { 
//                        activate((Service) mInServerQ.first());
//                    }
                }
                wait(mInServerQ);
            }
        } 
    }

    
    public static void main(String args[]) {
        activate(new Simulation5());
    } 
}