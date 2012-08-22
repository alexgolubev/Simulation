package com.cinnober.simulation.old;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
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

public class Simulation8 extends Process
{
    static boolean cTrace = false;
    static boolean cDumpTracks = false;
    static String cBlanks = "                                        ";
    static DecimalFormat cDecimalFormat = new DecimalFormat();

    static double cTaxSum = 0.0;
    static double cClientSum = 0.0;
    static int cRequests = 0;
    public static Simulation8 cInstance; 

    static Random cRandom = new Random(7);
    static boolean cFixedServiceTimed = false;

    // Run for 5 minutes expressed in microseconds
    //static int cRunTimeMicro =  5 * 60 * 1000 * 1000;
    
    // Run for 10 seconds expressed in microseconds
    static int cRunTimeMicro =  10 * 1000 * 1000;

    static int cRequestsToSend = Integer.MAX_VALUE;
    
    // Transactions per micro
    static double cClientTransPerMicro = 1000.0 / (1000.0 * 1000.0);
    static int cClientCount = 100;

    long mStartTime = System.currentTimeMillis();
    
    ArrayList<GcService> mGcList = new ArrayList<GcService>();
    
    public static void main(String args[])
    {
        cDecimalFormat.setMaximumFractionDigits(15);
        cDecimalFormat.setMinimumFractionDigits(15);

        cDecimalFormat.setMaximumIntegerDigits(100);
        cDecimalFormat.setMinimumIntegerDigits(1);
        cDecimalFormat.setGroupingSize(0);

        cInstance = new Simulation8(); 
        cInstance.init();
        activate(cInstance);
    }
    
    // A ME Primary contains the following threads
    ProcQueue mClient = new ProcQueue(Client.class);
    ProcQueue mClientTaxNwDelay = new ProcQueue();
    ProcQueue mTaxServer = new ProcQueue(TaxServer.class);
    ProcQueue mTaxMeNwDelay = new ProcQueue();
    ProcQueue mMatchingEngine = new ProcQueue(MatchingEngine.class);
    ProcQueue mMatchingEngineStandby = new ProcQueue(MatchingEngineStandby.class);
    ProcQueue mMeMesNwDelay = new ProcQueue();
    ProcQueue mMesMeNwDelay = new ProcQueue();
    ProcQueue mMeTaxNwDelay = new ProcQueue();
    ProcQueue mTaxClientNwDelay = new ProcQueue();
    
    GcService mTaxJvm = new GcService(50*1000, 500); // 500ms, 20 ms gc
    GcService mMeJvm = new GcService(50*1000, 500); // 250ms, 50 ms gc


    public Simulation8()
    {
    }
    
    public void init()
    {
        //GcService tTaxJvm = new GcService(500*1000, 20*1000);
        //GcService tMeJvm = new GcService(250*1000, 50*1000);

        mClient.init(new ServerConfig(mClient, mClientTaxNwDelay, "Client", cClientCount, cClientCount, 0));
        
        mClientTaxNwDelay.init(new ServerConfig(mClientTaxNwDelay, mTaxServer, "ClientTaxNwDelay", 1, 1, 50));
        
        mTaxServer.init(new ServerConfig(mTaxJvm, mTaxServer, mTaxMeNwDelay, mTaxClientNwDelay, "TaxServer", 1, 1, 0));

        mTaxMeNwDelay.init(new ServerConfig(mTaxMeNwDelay, mMatchingEngine, "TaxMeNwDelay", 1, 1, 50));
        
        mMatchingEngineStandby.init(new ServerConfig(mMatchingEngineStandby, mMesMeNwDelay, "MatchingEngineStandby", 1, 1, 60));

        mMatchingEngine.init(new ServerConfig(mMeJvm, mMatchingEngine, mMeTaxNwDelay, "MatchingEngine", 1, 1, 0));

        mMeMesNwDelay.init(new ServerConfig(mMeMesNwDelay, mMatchingEngineStandby, "MeMesNwDelay", 1, 1, 50));

        mMesMeNwDelay.init(new ServerConfig(mMesMeNwDelay, null, "MesMeNwDelay", 1, 1,50));
        
        mMeTaxNwDelay.init(new ServerConfig(mMeTaxNwDelay, null, "MeTaxNwDelay", 1, 1, 50));
        
        mTaxClientNwDelay.init(new ServerConfig(mTaxClientNwDelay, null, "TaxClientNwDelay", 5, 25, 50));
    }
    
    public static <T> T[] headToArray(Head pHead, T[] pArray)
    {
        return headToArray(pHead, pArray, pHead.cardinal());
    }

    public static <T> T[] headToArray(Head pHead, T[] pArray, int pSize) {
        T[] tArray = (T[]) Array.newInstance(pArray.getClass().getComponentType(), pSize);
        Link tLink = (Link) pHead.first();
        for (int i = 0; i < pSize; ++i)
        {
            tArray[i] = (T) tLink;
            tLink = (Link) tLink.suc();
        }
        return tArray;
    }

    
    // All times in ms.
    public void actions()
    {
        activate(mMeJvm);
        activate(mTaxJvm);
        double tGcTime = 20;
        Process[] tClientList = headToArray(mClient.mProcess, new Process[0]);
        for (int i = 0; i < tClientList.length; ++i)
        {
            activate((Process) tClientList[i]);
            if ( (i % 10) == 0)
            {
                hold(1000);
            }
            else
            {
                hold(1000);
            }
        }
        
        hold(cRunTimeMicro);

        System.out.println("Time = " + time());
        System.out.println("Requests = " + cRequests);
        System.out.println("Av.tax time = " + cTaxSum/cRequests);
        System.out.println("Av.client time = " + cClientSum/cRequests);
        System.out.println("\nExecution time: " + ((System.currentTimeMillis() - mStartTime)/1000.0) + " secs.");
    }

    static class Track
    {
        double mTime;
        String mMsg;
        public Track(double pTime, String pMsg)
        {
            super();
            mTime = pTime;
            mMsg = pMsg;
            if (cTrace)
            {
                System.out.println(cDecimalFormat.format(pTime) + " " + pMsg);
            }
        }
        
    }
    
    // Generate message, send into queue eaten from any tax
    // We should implement this as a closed queue!
    public static class Client extends ServerBase
    {
        boolean mTakeTime = true;
        public Client(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }

        public void actions()
        {

            double tMicroDelayPerClient = cClientCount / cClientTransPerMicro;
            double tStart;
            int tRequests = cRequestsToSend / cClientCount;
            for (int i = 0; i < tRequests; ++i )
            {
                tStart = time();
                mRequest = new Request(mName);

                // Add return.
                this.into(mRequest.mWaitList);
                mServerConfig.mOut.queue(mRequest);
                // Queue
                passivate();
                
                // Collect round trip statistics
                if (mTakeTime)
                {
                    double tTime = mRequest.mTimeOut - mRequest.mTimeIn;
                    cTaxSum += tTime;
                    double tClientTime = time() - mRequest.mCreateTime;
                    cClientSum += tClientTime;
                    String tInfo = mName + " TaxTime: " + tTime + " ClientTime: " + tClientTime;
                    
                    if (cTrace)
                    {
                        //System.out.println(time() + ": "  + ind(mRequest.mIndent++) + tInfo);
                    }
                    
                    mRequest.addTrack(new Track(time(), tInfo));
                    if (cDumpTracks)
                    {
                        mRequest.dumpTrack();
                    }
                } 

                // Calculate delay according to rate
                double tDelta = tMicroDelayPerClient - (time() - tStart);
                hold(tDelta);
            }
        }
    }

    static class Request extends Link
    {
        Request mActualRequest;
        int mRequest;
        double mCreateTime;
        double mTimeIn;
        double mTimeOut;
        // mWaitList contains the waiting servers.
        Head mWaitList;
        int mMeResponses;
        int mIndent;
        ConcurrentLinkedQueue<Track> mTrack;

        public Request()
        {
        }

        Request(Request pOriginal)
        {
            mActualRequest = pOriginal;
        }

        Request(String pClient)
        {
            mCreateTime = time();
            mRequest = ++cRequests;
            mWaitList = new Head();
            mTrack = new ConcurrentLinkedQueue<Track>();
            mTrack.add(new Track(time(), pClient));
        }

        public Request getRequest()
        {
            if (mActualRequest != null)
            {
                return mActualRequest.getRequest();
            }
            return this; 
        }
        
        public void addTrack(Track pTrack)
        {
            getRequest().mTrack.add(pTrack);
        }
        
        public void dumpTrack()
        {
            for (Track tTrack : mTrack)
            {
                System.out.println(cDecimalFormat.format(tTrack.mTime) + " " + mRequest + " " + tTrack.mMsg);
            }
        }
    }

    
    
    static class GcService extends Process
    {
        ArrayList<ServerBase> mThreadList = new ArrayList<ServerBase>();
        double mGcInterval;
        double mBlockTime;
        
        public GcService(double pGcInterval, double pBlockTime)
        {
            super();
            mGcInterval = pGcInterval;
            mBlockTime = pBlockTime;
        }

        // Add threads to be affected by this JVM
        public void addThread(ServerBase pServerBase)
        {
            mThreadList.add(pServerBase);
        }
        
        public void actions()
        {
            double tEndOfBreak;
            while (true)
            {
                //hold(mRandom.negexp(1.0/mGcInterval));
                hold(cRandom.uniform(mGcInterval, mGcInterval * 1.5));
                
                // if idle, set an earliest start time.
                // if not idle, updated with new time if past current schedule
                double tBlock = cRandom.uniform(mBlockTime, mBlockTime * 1.5);
                
                for (ServerBase tService : mThreadList)
                {
                    if (tService.idle())
                    {
                        tService.mBlockedTime = time() + tBlock;
                        if (cTrace)
                        {
                            //System.out.println(tService.mName + " Gc block " + time() + " " + tBlock);
                        }
                    }
                    else
                    {
                        tEndOfBreak = tService.evTime() +  tBlock;

                        Request tRequest = tService.mRequest.getRequest();
                        String tMessage = tService.mName + " GC delay " + + tService.evTime() + " to " + tEndOfBreak; 
                        if (tRequest != null)
                        {
                            tRequest.mTrack.add(new Track(time(), tMessage));
                        }
                        // If execution, execution will be extended
                        if (cTrace)
                        {
                            //System.out.println(tMessage);
                        }
                        reactivate(tService, at, tEndOfBreak);
                    }
                }
            }
        } 
    }

    public static String ind(int pLen)
    {
        return cBlanks.substring(0, pLen);
    }

    public static class ServerBase extends Process
    {
        ServerConfig mServerConfig;
        String mName;
        int mId;
        Request mRequest;
        double mBlockedTime;
        boolean mTakeTimeIn;
        boolean mTakeTimeOut;
        
        ArrayList<String> mBlockedMessage = new ArrayList<String>();
        
        public ServerBase(ServerConfig pServerConfig, int pId)
        {
            mServerConfig = pServerConfig;
            mId = pId;
            mName = mServerConfig.mName + "_" + mId;
        }

        public boolean checkBlock()
        {
            // If we were in GC-block while idle await GC completion until earliest non-block time
            if (mBlockedTime == 0)
            {
                return false;
            }

            double tBlockDelay = mBlockedTime - time();
            mBlockedTime = 0;
            if (tBlockDelay <= 0)
            {
                return false;
            }
            mBlockedMessage.add(mName + " GC blocked from " + time() + " + until " + (time() + tBlockDelay));
            hold(tBlockDelay);
            out();
            return true;
        }
        
        public void logBlock()
        {
            if (mBlockedMessage.size() != 0)
            {
                for (String tMsg : mBlockedMessage)
                {
                    mRequest.addTrack(new Track(time(), tMsg));
                }
                mBlockedMessage.clear();
            }
        }

        public void logBlock(BatchRequest pBatch)
        {
            if (mBlockedMessage.size() != 0)
            {
                for (int i = 0; i < pBatch.mCount; ++i)
                {
                    for (String tMsg : mBlockedMessage)
                    {
                        pBatch.mRequests[i].mTrack.add(new Track(time(), tMsg));
                    }
                }
                mBlockedMessage.clear();
            }
        }

        public void actions()
        {
            while (true)
            {
                out();
                
                // As long as we have work to do
                while (!mServerConfig.mIn.mQueue.empty())
                {
                    // If we were in GC-block while idle await GC completion until earliest non-block time
                    if (checkBlock())
                    {
                        continue;
                    }
                    
                    // Get request and take it out from queue.
                    Request tRequest = mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    //mRequest = mRequest.getRequest();
                    logBlock();

                    if (mTakeTimeIn)
                    {
                        mRequest.mTimeIn = time();
                    }
                    mRequest.addTrack(new Track(time(), mName + " Arrived"));

                    // Wait pre service time
                    if (cFixedServiceTimed)
                    {
                        hold(mServerConfig.mServiceTime);
                    }
                    else
                    {
                        hold(cRandom.normal(mServerConfig.mServiceTime, mServerConfig.mServiceTime * 0.3));
                    }
                    out();
                    mRequest.addTrack(new Track(time(), mName + " After Service"));
                    
                    if (mServerConfig.mCall != null)
                    {
                        this.into(mRequest.mWaitList);
                        mServerConfig.mCall.queue(tRequest);
                        passivate();
                        mRequest.addTrack(new Track(time(), mName + " Return after call"));
                    }

                    if (checkBlock())
                    {
                        logBlock();
                    }
                        
                    if (mTakeTimeOut)
                    {
                        mRequest.mTimeOut = time();
                    }

                    // Queue to next server or return
                    if (mServerConfig.mOut != null)
                    {
                        mRequest.addTrack(new Track(time(), mName + " Queue next"));
                        mServerConfig.mOut.queue(tRequest);
                    }
                    else
                    {
                        // Re-activate calling server with potential gc delay
                        if (!mRequest.mWaitList.empty())
                        { 
                            mRequest.addTrack(new Track(time(), mName + " return"));
                            ServerBase tService = (ServerBase) mRequest.mWaitList.last();
                            tService.out();
                            activate(tService);
                        }
                    }
                }
                mRequest.addTrack(new Track(time(), mName + " Waiting for work"));
                wait(mServerConfig.mIn.mProcess);
            }
        }
    }
    
    public static class ServerConfig
    {
        String mName;
        int mMinPoolSize;
        int mMaxPoolSize;
        ProcQueue mIn;
        ProcQueue mCall;
        ProcQueue mOut;
        Request mRequest;
        GcService mJvm;
        double mServiceTime;
        double mBlockedTime;
        
        public ServerConfig(ProcQueue pIn, ProcQueue pOut, String pName, int pMinPoolSize, int pMaxPoolSize, double pServiceTime)
        {
            mName = pName;
            mIn = pIn;
            mOut = pOut;
            mServiceTime = pServiceTime;
            mMinPoolSize = pMinPoolSize;
            mMaxPoolSize = pMaxPoolSize;
        }
        
        public ServerConfig(ProcQueue pIn, ProcQueue pCall, ProcQueue pOut, String pName, int pMinPoolSize, int pMaxPoolSize, double pServiceTime)
        {
            mName = pName;
            mIn = pIn;
            mCall = pCall;
            mOut = pOut;
            mServiceTime = pServiceTime;
            mMinPoolSize = pMinPoolSize;
            mMaxPoolSize = pMaxPoolSize;
        }
        
        public ServerConfig(GcService pJvm, ProcQueue pIn, ProcQueue pOut, String pName, int pMinPoolSize, int pMaxPoolSize, double pServiceTime)
        {
            mName = pName;
            mIn = pIn;
            mOut = pOut;
            mServiceTime = pServiceTime;
            mMinPoolSize = pMinPoolSize;
            mMaxPoolSize = pMaxPoolSize;
            mJvm = pJvm;
        }
        
        public ServerConfig(GcService pJvm, ProcQueue pIn, ProcQueue pCall, ProcQueue pOut, String pName, int pMinPoolSize, int pMaxPoolSize, double pServiceTime)
        {
            mName = pName;
            mIn = pIn;
            mCall = pCall;
            mOut = pOut;
            mServiceTime = pServiceTime;
            mMinPoolSize = pMinPoolSize;
            mMaxPoolSize = pMaxPoolSize;
            mJvm = pJvm;
        }
        
        
    }

   
    public static class ProcQueue
    {
        Head mQueue = new Head();
        Head mProcess = new Head();
        ServerConfig mServerConfig;
        Constructor mConstructor;
        int mPoolSize;
        
        public ProcQueue()
        {
        }

        public ProcQueue(Class pClass)
        {
            try
            {
                //Constructor[] mConstructors = pClass.getConstructors();
                mConstructor = pClass.getConstructor(ServerConfig.class, int.class);
            }
            catch (NoSuchMethodException e)
            {
                // By design
            }
        }

        public void init(ServerConfig pServerConfig)
        {
            mServerConfig = pServerConfig;
            for (int i = 0; i < mServerConfig.mMinPoolSize; ++i)
            {
                createProcess(i);
            }
        }
        
        public void createProcess(int pId)
        {
            if (mConstructor == null)
            {
                addProcess(new ServerBase(mServerConfig, pId));
            }
            else
            {
                try
                {
                    addProcess((ServerBase) mConstructor.newInstance(mServerConfig, pId));
                }
                catch (InstantiationException e)
                {
                    // By design
                }
                catch (InvocationTargetException e)
                {
                    // By design
                }
                catch (IllegalAccessException e)
                {
                    // By design
                }
            }
            
        }
        
        public void addProcess(ServerBase pServerBase)
        {
            pServerBase.into(mProcess);
            if (mServerConfig.mJvm != null)
            {
                mServerConfig.mJvm.addThread(pServerBase);
            }
            ++mPoolSize;
        }

        public void queue(Request pRequest)
        {
            // Activate server if inactive
            pRequest.into(mQueue);
            
            // Add thread to pool
            if (mPoolSize < mServerConfig.mMaxPoolSize && mProcess.empty())
            {
                createProcess(mPoolSize);
            }
            
            if (!mProcess.empty())
            { 
                Process.activate((Process) mProcess.first());
            }
        }

    }
    
    public static class MatchingEngine extends ServerBase
    {
        // A ME Primary contains the following threads
        ProcQueue mMePrimaryTcp = new ProcQueue();
        ProcQueue mMePrimaryServerPool = new ProcQueue();
        ProcQueue mMePrimarySorter = new ProcQueue(MeSorter.class);
        ProcQueue mMePrimaryChainUnit0 = new ProcQueue();
        ProcQueue mMePrimaryBatchSender = new ProcQueue(MeBatchSender.class);
        ProcQueue mMePrimaryRecoveryLog = new ProcQueue();
        ProcQueue mMePrimaryPostChain = new ProcQueue(MePostChain.class);
        ProcQueue mMePrimaryResponsePool = new ProcQueue();

        public MatchingEngine(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
            GcService tJvm = pServerConfig.mJvm;

            // Put medan values here
            mMePrimaryTcp.init(new ServerConfig(tJvm, mMePrimaryTcp, mMePrimaryServerPool, mName + "Tcp", 1, 1, 9));
            mMePrimaryServerPool.init(new ServerConfig(tJvm, mMePrimaryServerPool, mMePrimarySorter, mName + "_ServerPool", 5, 150, 27));
            mMePrimarySorter.init(new ServerConfig(tJvm, mMePrimarySorter, mMePrimaryChainUnit0, mName + "Sorter", 1, 1, 15));
            mMePrimaryChainUnit0.init(new ServerConfig(tJvm, mMePrimaryChainUnit0, mMePrimaryPostChain, mName + "_ChainUnit0", 1, 1, 52));
            mMePrimaryBatchSender.init(new ServerConfig(tJvm, mMePrimaryBatchSender, cInstance.mMeMesNwDelay, mMePrimaryPostChain, mName + "_BatchSender", 10, 10, 1));
            mMePrimaryRecoveryLog.init(new ServerConfig(tJvm, mMePrimaryRecoveryLog, mMePrimaryPostChain, mName + "_RecoveryLog", 1, 1, 50));
            mMePrimaryPostChain.init(new ServerConfig(tJvm, mMePrimaryPostChain, mMePrimaryResponsePool, mName + "_PostChain", 1, 1, 26));
            mMePrimaryResponsePool.init(new ServerConfig(tJvm, mMePrimaryResponsePool, this.mServerConfig.mOut, mName + "ResponsePool", 5, 25, 11));

            MeSorter[] tMeSorterList = headToArray(mMePrimarySorter.mProcess, new MeSorter[0]);
            for (int i = 0; i < tMeSorterList.length; ++i)
            {
                tMeSorterList[i].mMatchingEngine = this;
            }
        }
        
        public void actions()
        {
            while (true)
            {

                out();

                // As long as we have work to do
                while (!mServerConfig.mIn.mQueue.empty())
                {
                    // Get request and take it out from queue.
                    mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    mRequest = mRequest.getRequest();

                    mRequest.addTrack(new Track(time(), mName + " Arrived"));

                    mMePrimaryTcp.queue(mRequest);
                }
                mRequest.addTrack(new Track(time(), mName + " Waiting for work"));
                wait(mServerConfig.mIn.mProcess);
            }
        }
    }
    
    public static class MeSorter extends ServerBase
    {
        MatchingEngine mMatchingEngine;
        public MeSorter(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }
        
        public void actions()
        {
            while (true)
            {
                out();

                // As long as we have work to do
                while (!mServerConfig.mIn.mQueue.empty())
                {
                    // If we were in GC-block while idle await GC completion until earliest non-block time
                    if (checkBlock())
                    {
                        continue;
                    }

                    // Get request and take it out from queue.
                    mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    //mRequest = mRequest.getRequest();
                    logBlock();
                    
                    mRequest.addTrack(new Track(time(), mName + " Arrived"));

                    // Wait pre service time
                    if (cFixedServiceTimed)
                    {
                        hold(mServerConfig.mServiceTime);
                    }
                    else
                    {
                        hold(cRandom.normal(mServerConfig.mServiceTime, mServerConfig.mServiceTime * 0.3));
                    }
                    out();
                    mRequest.addTrack(new Track(time(), mName + " After Service"));

                    // Need to create request clones for these calls!
                    mRequest.mMeResponses = 3;
                    mMatchingEngine.mMePrimaryBatchSender.queue(new Request(mRequest));
                    mMatchingEngine.mMePrimaryRecoveryLog.queue(new Request(mRequest));
                    mServerConfig.mOut.queue(mRequest);
                }
                mRequest.addTrack(new Track(time(), mName + " Waiting for work"));
                wait(mServerConfig.mIn.mProcess);
            }
        }
    }

    public static class BatchRequest extends Request
    {
        Request[] mRequests;
        int mCount;
        
        public BatchRequest(int pSize)
        {
            mWaitList = new Head();
            mRequests = new Request[pSize];
            mCount = 0;
        }
        public void addTrack(Track pTrack)
        {
            for (int i = 0; i < mCount; ++i)
            {
                mRequests[i].getRequest().mTrack.add(pTrack);
            }
        }

    }
    

    public static class MeBatchSender extends ServerBase
    {
        public MeBatchSender(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }
        
        public void actions()
        {
            int tMaxBatch = 40;
            BatchRequest tBatch = new BatchRequest(tMaxBatch);
            while (true)
            {
                out();

                // As long as we have work to do
                tBatch.mCount = 0;
                while (!mServerConfig.mIn.mQueue.empty())
                {
                    // If we were in GC-block while idle await GC completion until earliest non-block time
                    if (checkBlock())
                    {
                        continue;
                    }

                    // Get request and take it out from queue.
                    mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    //mRequest = mRequest.getRequest();
                    logBlock();
                    
                    mRequest.addTrack(new Track(time(), mName + " Arrived"));

                    tBatch.mRequests[tBatch.mCount++] = mRequest;
                    if ( tBatch.mCount < tMaxBatch && !mServerConfig.mIn.mQueue.empty())
                    {
                        continue;
                    }

                    // Wait pre service time
                    if (cFixedServiceTimed)
                    {
                        hold(mServerConfig.mServiceTime);
                    }
                    else
                    {
                        hold(cRandom.normal(mServerConfig.mServiceTime, mServerConfig.mServiceTime * 0.3));
                    }
                    out();
                    // Log in all requests.

                    tBatch.addTrack(new Track(time(), mName + " After Service"));

                    // Do a call first
                    this.into(tBatch.mWaitList);
                    mServerConfig.mCall.queue(tBatch);
                    passivate();

                    if (checkBlock())
                    {
                        logBlock(tBatch);
                    }
                    
                    tBatch.addTrack(new Track(time(), mName + " Return after batch call"));
                    // Handle situation when batches are out of order
                    for (int i = 0; i < tBatch.mCount; ++i)
                    {
                        // Queue to next server or return
                        if (mServerConfig.mOut != null)
                        {
                            tBatch.mRequests[i].addTrack(new Track(time(), mName + " Queue next"));
                            mServerConfig.mOut.queue(tBatch.mRequests[i]);
                        }
                        else
                        {
                            // Re-activate calling server with potential gc delay
                            if (!mRequest.mWaitList.empty())
                            { 
                                tBatch.mRequests[i].addTrack(new Track(time(), mName + " return"));
                                ServerBase tService = (ServerBase) mRequest.mWaitList.last();
                                tService.out();
                                activate(tService);
                            }
                        }
                    }
                }
                tBatch.addTrack(new Track(time(), mName + " Waiting for work"));
                wait(mServerConfig.mIn.mProcess);
            }
        }
    }

    
    public static class MePostChain extends ServerBase
    {
        public MePostChain(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }
        
        public void actions()
        {
            while (true)
            {
                out();

                // As long as we have work to do
                while (!mServerConfig.mIn.mQueue.empty())
                {
                    // If we were in GC-block while idle await GC completion until earliest non-block time
                    if (checkBlock())
                    {
                        continue;
                    }

                    // Get request and take it out from queue.
                    mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    mRequest = mRequest.getRequest();
                    logBlock();

                    mRequest.addTrack(new Track(time(), mName + " Arrived"));

                    // Wait pre service time
                    if (cFixedServiceTimed)
                    {
                        hold(mServerConfig.mServiceTime);
                    }
                    else
                    {
                        hold(cRandom.normal(mServerConfig.mServiceTime, mServerConfig.mServiceTime * 0.3));
                    }
                    out();
                    mRequest.addTrack(new Track(time(), mName + " After Service"));

                    mRequest = mRequest.getRequest();
                    // Wait for all three request
                    // Then activate response thread
                    if (--mRequest.mMeResponses == 0)
                    {
                        mServerConfig.mOut.queue(mRequest);
                    }
                }
                mRequest.addTrack(new Track(time(), mName + " Waiting for work"));
                wait(mServerConfig.mIn.mProcess);
            }
        }
    }

    public static class MatchingEngineStandby extends ServerBase
    {
        // A ME Primary contains the following threads
        ProcQueue mMeStandbyTcp = new ProcQueue();
        ProcQueue mMeStandbyServerPool = new ProcQueue();

        public MatchingEngineStandby(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
            GcService tJvm = pServerConfig.mJvm;
            // Put medan values here
            mMeStandbyTcp.init(new ServerConfig(tJvm, mMeStandbyTcp, mMeStandbyServerPool, mName + "Tcp", 1, 1, 9));
            mMeStandbyServerPool.init(new ServerConfig(tJvm, mMeStandbyServerPool, this.mServerConfig.mOut, mName + "_ServerPool", 5, 150, 10)); // Estimate
        }
        
        public void actions()
        {
            while (true)
            {

                out();

                // As long as we have work to do
                while (!mServerConfig.mIn.mQueue.empty())
                {
                    // Get request and take it out from queue.
                    mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    mRequest = mRequest.getRequest();

                    mRequest.addTrack(new Track(time(), mName + " Arrived"));

                    mMeStandbyTcp.queue(mRequest);
                }
                mRequest.addTrack(new Track(time(), mName + " Waiting for work"));
                wait(mServerConfig.mIn.mProcess);
            }
        }
    }


    public static class TaxServerTcp extends ServerBase
    {
        public TaxServerTcp(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
            mTakeTimeIn = true;
        }
    }

    public static class TaxServerPool extends ServerBase
    {
        public TaxServerPool(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
            mTakeTimeOut = true;
        }
    }
    
    public static class TaxServer extends ServerBase
    {
        // A ME Primary contains the following threads
        ProcQueue mTaxTcp = new ProcQueue(TaxServerTcp.class);
        ProcQueue mTaxPool = new ProcQueue(TaxServerPool.class);
        
        public TaxServer(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
            GcService tJvm = pServerConfig.mJvm;

            mTaxTcp.init(new ServerConfig(tJvm, mTaxTcp, mTaxPool, mName + "Tcp", 1, 1, 20));
            // Destination is an external tcp-link, with process-queue
            mTaxPool.init(new ServerConfig(tJvm, mTaxPool, this.mServerConfig.mCall, this.mServerConfig.mOut, mName + "ServerPool", 5, 150, 20));
        }

        public void actions()
        {
            while (true) {

                out();

                // As long as we have work to do
                while (!mServerConfig.mIn.mQueue.empty())
                {
                    // Get request and take it out from queue.
                    mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    mRequest = mRequest.getRequest();

                    mRequest.addTrack(new Track(time(), mName + " Arrived"));

                    mTaxTcp.queue(mRequest);
                }
                
                mRequest.addTrack(new Track(time(), mName + " Waiting for work"));
                wait(mServerConfig.mIn.mProcess);
            }
        }
    }    
}

