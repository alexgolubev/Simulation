package com.cinnober.simulation.old;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import dk.ruc.javaSimulation.*;
import dk.ruc.javaSimulation.Process;


/*
* Simulation of a queuing system.
*
* This version models is based on passive customers 
* and active servers.
*/

public class Simulation7 extends Process
{
    static boolean cTrace = false;
    static String cBlanks = "                                        ";
    static DecimalFormat cDecimalFormat = new DecimalFormat();

    double mSum;
    double mQueuSum;
    int mRequests = 0;

    int mTaxes = 1;
    int mMes = 1;
    int mClients = 10;
    Random mRandom = new Random(7);
    
    Head mTaxServers = new Head();
    Head mMeServers = new Head();
    
    Head mTaxRequestQueue = new Head();
    Head mMeRequestQueue = new Head();

    long mStartTime = System.currentTimeMillis();
    int mRequestsToSend = 10;
    boolean mFixedServiceTimed = false;
    ArrayList<GcService> mGcList = new ArrayList<GcService>();


    public static void main(String args[])
    {
        cDecimalFormat.setMaximumFractionDigits(15);
        cDecimalFormat.setMinimumFractionDigits(15);

        cDecimalFormat.setMaximumIntegerDigits(100);
        cDecimalFormat.setMinimumIntegerDigits(1);
        cDecimalFormat.setGroupingSize(0);
        
//        DecimalFormatSymbols tFormatSymb = cDecimalFormat.getDecimalFormatSymbols();
//        tFormatSymb.setGroupingSeparator(' ');
//        tFormatSymb.setDecimalSeparator('.');
//
//        cDecimalFormat.setDecimalFormatSymbols(tFormatSymb);

        activate(new Simulation7());
    }
    
    // All times in micro seconds.
    public void actions()
    {
        double tGcTime = 123;
        Service tService;
        
        for (int i = 1; i <= mTaxes; i++)
        {
            tService = new Service("Tax"+i, 50, 50, mTaxRequestQueue, mTaxServers, mMeRequestQueue, mMeServers);
            tService.into(mTaxServers);
            tService.mTakeTime = true;
            if (tGcTime != 0)
            {
                GcService tTmp = new GcService(tService, 300*1000, 12*1000); 
                mGcList.add(tTmp);
                activate(tTmp);
            }
        }
        
        for (int i = 1; i <= mMes; i++)
        { 
            tService = new Service("Me"+i, 125, 125, mMeRequestQueue, mMeServers, null, null);
            tService.into(mMeServers);
            if (tGcTime != 0)
            {
                GcService tTmp = new GcService(tService, 200*1000, 23*1000); 
                mGcList.add(tTmp);
                activate(tTmp);
            }
        }
        
        for (int i = 1; i <= mClients; ++i)
        {
            activate(new ClientRequestGenerator(i), at, 10*i);
        }
        hold(1000*1000);
        System.out.println("Requests = " + mRequests);
        System.out.println("Av.elapsed time = " + mSum/mRequests);
        System.out.println("Av.queue time = " + mQueuSum/mRequests);
        System.out.println("\nExecution time: " + ((System.currentTimeMillis() - mStartTime)/1000.0) + " secs.");
    }

    // Generate message, send into queue eaten from any tax
    // We should implement this as a closed queue!
    class ClientRequestGenerator extends Service
    {
        public ClientRequestGenerator(int pClient)
        {
            super("Client " + pClient);
            mTakeTime = true;
        }

        public void actions()
        {
            double tStart;
            for (int i = 0; i < mRequestsToSend; ++i )
            {
                tStart = time();
                Request tRequest = new Request(mName);

                hold(mRandom.uniform(45,65));
                tRequest.mTrack.add(new Track(time(), "Network send done "));
                
                // Add this process to we waked when completed
                tRequest.into(mTaxRequestQueue);
                this.into(tRequest.mWaitList);
                if (!mTaxServers.empty()) 
                    activate((Service) mTaxServers.first());

                // We should wait here for completion
                passivate();
                hold(mRandom.uniform(45,65));
                tRequest.mTrack.add(new Track(time(), "Network receive done"));

                // Collect round trip statistics
                if (mTakeTime)
                {
                    tRequest.mTimeOut = time();
                    double tTime = tRequest.mTimeOut - tRequest.mTimeIn;
                    mSum += tTime;
                    double tQtime = tRequest.mTimeOut - tRequest.mCreateTime;
                    mQueuSum += tQtime;
                    String tInfo = mName + " TaxTime: " + tTime + " ClientTime: " + tQtime;
                    
                    if (cTrace)
                    {
                        System.out.println(time() + ": "  + ind(tRequest.mIndent++) + tInfo);
                    }
                    
                    tRequest.mTrack.add(new Track(time(), tInfo));
                    tRequest.dumpTrack();
                } 

                //hold(mRandom.uniform(1, 2));

                // Calculate delay according to rate
                double tDelta = 1000000 / mClients - (time() - tStart);
                hold(tDelta);
            }

            for (GcService tGcService : mGcList)
            {
                cancel(tGcService);
            }
        }
    }

    class Track
    {
        double mTime;
        String mMsg;
        public Track(double pTime, String pMsg)
        {
            super();
            mTime = pTime;
            mMsg = pMsg;
        }
        
    }
    
    class Request extends Link
    {
        int mRequest = ++mRequests;
        double mCreateTime = time();
        double mTimeIn;
        double mTimeOut;
        // mWaitList contains the waiting servers.
        Head mWaitList = new Head();
        int mIndent = 0;
        ConcurrentLinkedQueue<Track> mTrack = new ConcurrentLinkedQueue<Track>();

        Request(String pClient)
        {
            mTrack.add(new Track(time(), pClient));
        }

        public void dumpTrack()
        {
            for (Track tTrack : mTrack)
            {
                System.out.println(cDecimalFormat.format(tTrack.mTime) + " " + mRequest + " " + tTrack.mMsg);
            }
        }
    }

    class GcService extends Process
    {
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

        public void actions()
        {
            double tEndOfBreak;
            while (true)
            {
                //hold(mRandom.negexp(1.0/mGcInterval));
                hold(mRandom.uniform(mGcInterval, mGcInterval * 1.5));
                
                // if idle, set an earliest start time.
                // if not idle, updated with new time if past current schedule
                double tBlock = mRandom.uniform(mBlockTime, mBlockTime * 1.5);
                if (mService.idle())
                {
                    mService.mBlockedTime = time() + tBlock;
                    if (cTrace)
                    {
                        System.out.println(mService.mName + " Gc block " + time() + " " + tBlock);
                    }
                }
                else
                {
                    tEndOfBreak = mService.evTime() +  tBlock;

                    Request tRequest = mService.mActiveRequest;
                    String tMessage = mService.mName + " GC delay " + + mService.evTime() + " to " + tEndOfBreak; 
                    if (tRequest != null)
                    {
                        tRequest.mTrack.add(new Track(time(), tMessage));
                    }
                    // If execution, execution will be extended
                    if (cTrace)
                    {
                        System.out.println(tMessage);
                    }
                    reactivate(mService, at, tEndOfBreak);
                }

            }
        } 
        
    }
    public static String ind(int pLen)
    {
        return cBlanks.substring(0, pLen);
    }
    
    class Service extends Process
    {
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
        Request mActiveRequest;

        public Service(String pName)
        {
            mName = pName;
        }

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

        public void actions()
        {
            ArrayList<String> tBlocked = new ArrayList<String>();

            while (true) {

                // As long as we have work to do
                while (!mInQ.empty())
                {
                    // Remove myself from any list I might be in.
                    out();

                    // If we were in GC-block while idle await GC completion until earliest non-block time
                    if (mBlockedTime != 0)
                    {
                        double tBlockDelay = mBlockedTime - time();
                        mBlockedTime = 0;
                        if (tBlockDelay > 0)
                        {
                            tBlocked.add(mName + " GC blocked from " + time() + " + until " + (time() + tBlockDelay));
                            hold(tBlockDelay);
                            continue;
                        }
                    }

                    // Get request and take it out from queue.
                    Request tReq = (Request) mInQ.first();
                    tReq.out();
                    mActiveRequest = tReq;
                    if (tBlocked.size() != 0)
                    {
                        for (String tMsg : tBlocked)
                        {
                            tReq.mTrack.add(new Track(time(), tMsg));
                        }
                        tBlocked.clear();
                    }
                    
                    // Save time for round trip
                    if (mTakeTime)
                    {
                        tReq.mTimeIn = time();
                    }

                    tReq.mTrack.add(new Track(time(), mName + " Arrived"));

                    // Wait pre service time
                    if (mFixedServiceTimed)
                    {
                        hold(mServiceTime1);
                    }
                    else
                    {
                        //hold(mRandom.normal(mServiceTime1, mStdev1));
                        hold(mRandom.poisson(mServiceTime1));
                    }
                    
                    tReq.mTrack.add(new Track(time(), mName + " Pre Service done"));

                    // If requested, send message to next server 
                    if (mOutQ != null)
                    {
                        tReq.into(mOutQ);
                        
                        // Reactivate calling server if none active
                        this.into(tReq.mWaitList);
                        if (!mOutServerQ.empty())
                        { 
                            activate((Process) mOutServerQ.first());
                        }

                        // Wait for completion of request, i.e, activated by called server and handling GC delay
                        passivate();

                        // If we were in GC-block while idle await GC completion until earliest non-block time
                        if (mBlockedTime != 0)
                        {
                            double tBlockDelay = mBlockedTime - time();
                            mBlockedTime = 0;
                            if (tBlockDelay > 0)
                            {
                                String tMsg = mName + " GC blocked2 from " + time() + " + until " + (time() + tBlockDelay);
                                tReq.mTrack.add(new Track(time(), tMsg));
                                hold(tBlockDelay);
                            }
                        }
                        
                        tReq.mTrack.add(new Track(time(), mName + " Call done"));
                        
                        // Wait post service time
                        if (mFixedServiceTimed)
                        { 
                            hold(mServiceTime2);
                        }
                        else
                        { 
                            //hold(mRandom.normal(mServiceTime2, mStdev2));
                            hold(mRandom.poisson(mServiceTime2));
                        }

                        tReq.mTrack.add(new Track(time(), mName + " Post Service done"));
                    }
                    
                    // Re-activate calling server with potential gc delay
                    if (!tReq.mWaitList.empty())
                    { 
                        Service tService = (Service)tReq.mWaitList.last();
                        tService.out();
                        activate(tService);
                    }
                }
                mActiveRequest = null;
                wait(mInServerQ);
            }
        } 
    }

    
 
}