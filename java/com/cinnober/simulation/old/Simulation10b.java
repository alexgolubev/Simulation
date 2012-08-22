package com.cinnober.simulation.old;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.cinnober.simulation.Process;

import dk.ruc.javaSimulation.*;


/*
* Simulation of a queuing system.
*
* This version models is based on passive customers 
* and active servers.
*/

public class Simulation10b extends Process
{
    public static final String cTransMonHeader = "time;eligable;duration;excuses;result;transid;ip:port;user;req;rsp;partition;obid;conn;osize;meta";
    static SimpleDateFormat cDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    static Date cDate = new Date();

    static boolean cTrace = true;  //true;
    static boolean cDumpTracks = false;
    static boolean cTransLogMon = true;
    static boolean cTick = true; //false; 
    static boolean cSaveTracks = false;

    static String cBlanks = "                                        ";
    static DecimalFormat cDecimalFormat = new DecimalFormat();
    static double cTaxSum = 0.0;
    static double cClientSum = 0.0;
    static int cRequests = 0;
    public static Simulation10b cInstance; 
    static double tClientStartTime;

    static Random cRandom = new Random(7);
    static boolean cFixedServiceTimed = false;

    // Run for 20 minutes expressed in microseconds
    static double cSimTimeMicro =  20 * 1000 * 1000;
    //static double cSimTimeMicro =  10*1000*1000;     // > cClientCount/cClientTransPerMicro
    static double cActualRunTimeMicro;     
    
    // Transactions per micro
    static double cClientTransPerMicro = 1000.0 / (1000.0 * 1000.0); //Total transactions from all clients per micro second
    static int cClientCount = 100;
    static int cMaxRequestsToSend; 
    
    static long cStartTime = System.currentTimeMillis();
    
    ArrayList<GcService> mGcList = new ArrayList<GcService>();
    
    public static void main(String args[])
    {
        cDecimalFormat.setMaximumFractionDigits(15);
        cDecimalFormat.setMinimumFractionDigits(15);

        cDecimalFormat.setMaximumIntegerDigits(100);
        cDecimalFormat.setMinimumIntegerDigits(1);
        cDecimalFormat.setGroupingSize(0);

        cInstance = new Simulation10b(); 
        cInstance.init();
        activate(cInstance);
    }
    
    ProcQueue mClient = new ProcQueue(Client.class);
    ProcQueue mClientTaxNwDelay = new ProcQueue();
    ProcQueue mTaxMeNwDelay = new ProcQueue();
    ProcQueue mMeMesNwDelay = new ProcQueue();
    ProcQueue mMesMeNwDelay = new ProcQueue();
    ProcQueue mMeTaxNwDelay = new ProcQueue();
    ProcQueue mTaxClientNwDelay = new ProcQueue();
    
    // A ME Primary contains the following threads
    ProcQueue mMePrimaryTcp = new ProcQueue();
    ProcQueue mMePrimaryServerPool = new ProcQueue();
    ProcQueue mMePrimarySorter = new ProcQueue(MeSorter.class);
    ProcQueue mMePrimaryChainUnit0 = new ProcQueue();
    ProcQueue mMePrimaryBatchSender = new ProcQueue(MeBatchSender.class);
    ProcQueue mMePrimaryRecoveryLog = new ProcQueue();
    ProcQueue mMePrimaryPostChain = new ProcQueue(MePostChain.class);
    ProcQueue mMePrimaryResponsePool = new ProcQueue();

    // A ME Standby contains the following threads
    ProcQueue mMeStandbyTcp = new ProcQueue();
    ProcQueue mMeStandbyServerPool = new ProcQueue();

    // A TAX contains the following threads
    ProcQueue mTaxTcp = new ProcQueue(TaxServerTcp.class);
    ProcQueue mTaxPool = new ProcQueue();
    ProcQueue mTaxCollector = new ProcQueue(TaxCollector.class);
    
    GcService mTaxJvm;
    GcService mMeJvm;
    GcService mMeStandbyJvm;

    public Simulation10b()
    {
    }

    // Do a simple solution with multiple clients, one tax, one me and one standby
    public void init()
    {
        boolean tHotspot = true;
        // Figures below valid for 1000TPS
        double tGcRateFactor =  1000.0D / ((1000.0D * 1000.0) * cClientTransPerMicro) ;
        if (tHotspot)
        {
            // tax average 6ms, max 65, every 20 seconds
            // me1 27ms, max 83ms, every 20 seconds
            // me1s 27ms, max 87, every 22seconds
            mTaxJvm = new GcService(9908.0D*1000 * tGcRateFactor, 9.0D*1000); 
            mMeJvm = new GcService(2509.0D*1000 * tGcRateFactor,  17.0D*1000); 
            mMeStandbyJvm = new GcService(2257.0D*1000 * tGcRateFactor, 15.0D*1000);
        }
        else
        {
            // Metronome 
            mTaxJvm = new GcService(100*1000, 500);
            mMeJvm = new GcService(100*1000, 500);
            mMeStandbyJvm = new GcService(100*1000, 500);
        }

        String tName;
        mClient.init(new ServerConfig(mClient, mClientTaxNwDelay, "Client", cClientCount, cClientCount, 0.1));
        {
            mClientTaxNwDelay.init(new ServerConfig(mClientTaxNwDelay, mTaxTcp, "ClientTaxNwDelay", 1, 1, 0.1)); 
            {
                tName = "TaxServer";
                mTaxTcp.init(new ServerConfig(mTaxJvm, mTaxTcp, mTaxPool, tName + "_Tcp", 1, 1, 22)); 
                mTaxPool.init(new ServerConfig(mTaxJvm, mTaxPool, mTaxMeNwDelay, tName + "_ServerPool", 5, 150, 11));        

                {
                    mTaxMeNwDelay.init(new ServerConfig(mTaxMeNwDelay, mMePrimaryTcp, "TaxMeNwDelay", 1, 1, 100)); 
                    {
                         tName="MatchingEngine";
                         mMePrimaryTcp.init(new ServerConfig(mMeJvm, mMePrimaryTcp, mMePrimaryServerPool, tName + "Tcp", 1, 1, 14));
                         mMePrimaryServerPool.init(new ServerConfig(mMeJvm, mMePrimaryServerPool, mMePrimarySorter, tName + "_ServerPool", 5, 150, 12)); 
                         mMePrimarySorter.init(new ServerConfig(mMeJvm, mMePrimarySorter, mMePrimaryChainUnit0, tName + "_Sorter", 1, 1, 13));
                         mMePrimaryChainUnit0.init(new ServerConfig(mMeJvm, mMePrimaryChainUnit0, mMePrimaryPostChain, tName + "_ChainUnit0", 1, 1, 59)); 
                         mMePrimaryBatchSender.init(new ServerConfig(mMeJvm, mMePrimaryBatchSender, mMeMesNwDelay, tName + "_BatchSender", 10, 10, 1));                          
                         mMePrimaryRecoveryLog.init(new ServerConfig(mMeJvm, mMePrimaryRecoveryLog, mMePrimaryPostChain, tName + "_RecoveryLog", 1, 1, 50)); 
                         mMePrimaryPostChain.init(new ServerConfig(mMeJvm, mMePrimaryPostChain, mMePrimaryResponsePool, tName + "_PostChain", 1, 1, 46)); 
                         mMePrimaryResponsePool.init(new ServerConfig(mMeJvm, mMePrimaryResponsePool, mMeTaxNwDelay, tName + "_ResponsePool", 5, 25, 16)); 

                         {
                             mMeMesNwDelay.init(new ServerConfig(mMeMesNwDelay, mMeStandbyTcp, "MeMesNwDelay", 1, 1, 90)); 
                             {
                                 tName="MatchingEngineStandby";
                                 mMeStandbyTcp.init(new ServerConfig(mMeStandbyJvm, mMeStandbyTcp, mMeStandbyServerPool, tName + "_Tcp", 1, 1, 13)); 
                                 mMeStandbyServerPool.init(new ServerConfig(mMeStandbyJvm, mMeStandbyServerPool, mMesMeNwDelay, tName + "_ServerPool", 5, 150, 18)); 
                             }
                             mMesMeNwDelay.init(new ServerConfig(mMesMeNwDelay, mMePrimaryPostChain, "MesMeNwDelay", 1, 1, 90));                             
                        }
                    }
                    mMeTaxNwDelay.init(new ServerConfig(mMeTaxNwDelay, mTaxCollector, "MeTaxNwDelay", 1, 1, 100));
                }
            }    
            mTaxCollector.init(new ServerConfig(mTaxJvm, mTaxCollector, mTaxClientNwDelay, tName + "_Collector", 1, 1, 0.1));               
            mTaxClientNwDelay.init(new ServerConfig(mTaxClientNwDelay, null, "TaxClientNwDelay", 1, 1, 0.1));
        }
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

    
    private static final int cMeJvm = 1;
    private static final int cMeStandbyJvm =2;
    private static final int cForNext = 3;
    private static final int cNewClient = 4;
    private static final int cForEnd = 5;
    private static final int cHoldRunTimeMicro = 6;
    private static final int cHoldRunTime = 7;
    private static final int cJvmActivated = 8;

    Process[] mClientList;
    double mHoldTimeBetweenClients;
    int mFor_i;
    
    // All times in ms.
    public int actions()
    {
        switch (mState) 
        {
            case cProcessInit:
                out();
                if (cSimTimeMicro < cClientCount/cClientTransPerMicro) {
                    cSimTimeMicro = cClientCount/cClientTransPerMicro;
                    System.out.println("\nSimulation time is too short! It has been reset to " + cSimTimeMicro + " micro\n");
                }
                cMaxRequestsToSend = (int) (cClientTransPerMicro * cSimTimeMicro);
                System.out.println("cMaxRequestsToSend = " + cMaxRequestsToSend);
                
                if (cTransLogMon)
                {
                    System.out.println(cTransMonHeader);
                    System.out.println("----------------------------------------------------------------------------------------");
                }

//              if (cTick)
//                {
//                    activate(new Tick());
//                }
                activate(mTaxJvm);
                return cMeJvm;
                
            case cMeJvm:
                activate(mMeJvm);
                return cMeStandbyJvm;

            case cMeStandbyJvm:
                activate(mMeStandbyJvm);
                return cJvmActivated;

            case cJvmActivated:
                mClientList = headToArray(mClient.mProcess, new Process[0]);
                
                //double tInterval = 500/tClientList.length;  //500 is the minimum response time
                //double tInterval = 1.0/(cClientCount*cClientTransPerMicro);  
                
                double tInterval = 1.0D*1000*1000/(cClientCount);   //evenly distributed client over 1 second interval
                
                //hold until connect next client to the system 
                mHoldTimeBetweenClients = cRandom.normal(tInterval, tInterval*0.3);
                mFor_i = -1;
                // Flow into loop

            case cForNext:
                ++mFor_i;
                //for (int i = 0; i < tClientList.length; ++i)    //For every client
                if (mFor_i >= mClientList.length)
                {
                    return cForEnd;
                }

                tClientStartTime = time();
                activate((Process) mClientList[mFor_i]); 
                return cNewClient;
                
            case cNewClient:
                hold(mHoldTimeBetweenClients);
                return cForNext;

            case cForEnd:
                //hold(cRunTimeMicro);
                //return cHoldRunTimeMicro;

            case cHoldRunTimeMicro:
                cActualRunTimeMicro = cSimTimeMicro * 2;
                hold(cActualRunTimeMicro);
                return cHoldRunTime;
                    
            case cHoldRunTime:
                if (cTick)
                {
                    System.out.println("\nSimulation time = " + time()/(1000.0*1000.0) + " sec.");
                    System.out.println("Requests = " + cRequests);
                    System.out.println("cRequestsToSend = " + cMaxRequestsToSend);
                    System.out.println("Av.tax time = " + cTaxSum/cRequests);
                    System.out.println("Av.client time = " + cClientSum/cRequests);
                    System.out.println("\nExecution time: " + ((System.currentTimeMillis() - cStartTime)/1000.0) + " secs.");         
                }
                return cProcessDone;
        }
        return cProcessError;
    }

//    public static class Tick extends Process
//    {
//
//        public void actions()
//        {
//            while (true)
//            {
//                hold(1.0D*1000*1000);
//                System.out.println(((System.currentTimeMillis() - cStartTime)/1000.0) + " " + 
//                        cDecimalFormat.format(time()) + " " + cRequests + " " + cTaxSum/cRequests +
//                        " " + cClientSum/cRequests);
//
//            }
//        }
//    }
    
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
        double tNextRequestStartTime;
        
        public Client(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }

        static boolean cInit;
        static double cMicroDelayPerClient;
        static int cMaxRequestsPerClient;
        int mFor_i;
        double mStart;

        private static final int cForNext = 1;
        private static final int cResumed = 2;
        private static final int cForEnd = 3;
        private static final int cQueueActivate = 4;
        
        public int actions()
        {
            if (!cInit)
            {
                cInit = true;
                cMicroDelayPerClient = cClientCount / cClientTransPerMicro;
                cMaxRequestsPerClient = cMaxRequestsToSend / cClientCount;
            }

            switch (mState)
            {
                case cProcessInit:
                    System.out.println("tMaxRequestsPerClient = " + cMaxRequestsPerClient);    
                    mFor_i = -1;
                    // Flow into loop

                case cForNext:
                    ++mFor_i;
                    // for (int i = 0; i < tMaxRequestsPerClient; ++i )
                    if (mFor_i >= cMaxRequestsPerClient)
                    {
                        return cForEnd;
                    }
                    mStart = time();
                    mRequest = new Request(mName);
                    if (mFor_i == 0)
                    {
                        mRequest.mCreateTime = tClientStartTime;
                    }
                    else
                    {
                        mRequest.mCreateTime = tNextRequestStartTime;
                    }
                
                    // Add return.
                    this.into(mRequest.mWaitList);
                    // Queue
                    if (mServerConfig.mOut.queue(mRequest))
                    {
                        return cQueueActivate;
                    }
                    // Fall thru

                case cQueueActivate:
                    passivate();
                    return cResumed;
                    
                case cResumed:
                    // Collect round trip statistics
                    if (mTakeTime)
                    {
                        double tTaxTime = mRequest.mTimeOut - mRequest.mTimeIn;
                        cTaxSum += tTaxTime;
                        double tClientTime = time() - mRequest.mCreateTime;
                        cClientSum += tClientTime;
                        String tInfo = mName + " TaxTime: " + tTaxTime + " ClientTime: " + tClientTime;
                     
                        if (cTrace)
                        {
                            //System.out.println(time() + ": "  + ind(mRequest.mIndent++) + tInfo);
                            System.out.println(mRequest.mClient + ", Request " + mFor_i + ", which is request #" + mRequest.mRequest + " in all incoming transactions");  
                            System.out.println("Request creation time " + mRequest.mCreateTime);                      
                            System.out.println("TimeIn to TAX " + mRequest.mTimeIn);
                            System.out.println("TimeOut from TAX " + mRequest.mTimeOut);                  
                        }

                        if (cTransLogMon)
                        {
                            logTransMon(mRequest);
                        }
                    
                        if (cSaveTracks)
                        {
                            mRequest.addTrack(new Track(time(), tInfo));
                            if (cDumpTracks)
                            {
                                mRequest.dumpTrack();
                            }
                        } 
                    } 
                
                    //hold until send in next transaction from this client
                    double tHoldTime = cRandom.normal(cMicroDelayPerClient, cMicroDelayPerClient/1000);  //Variation has to be small enough here!
                    //double tHoldTime = tMicroDelayPerClient;  // evenly distributed according to insertion rate
                    //double tHoldTime = cRandom.negexp(1.0/tMicroDelayPerClient);
                	
                    tNextRequestStartTime = mRequest.mCreateTime + tHoldTime;
                
                    // Calculate delay according to rate                	
                    double tDelta = tHoldTime - (time() - mStart);
    
                    if (tDelta > 0)
                    {
                        hold(tDelta);
                    }
                    return cForNext;

                case cForEnd:
                    return cProcessDone;
            }
            return cProcessError;
        }
    }

    static class Request extends Link
    {
        Request mActualRequest;
        String mClient;
        int mRequest;
        double mCreateTime;
        double mTimeIn;
        double mTimeOut;
        // mWaitList contains the waiting servers.
        Head mWaitList;
        int mMeResponses;
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
            mClient = pClient;
            //mCreateTime = time();   //Set when receiving a request at the client
            mRequest = ++cRequests;   //Sorted for all requests from all clients 
            mWaitList = new Head();
            mTrack = new ConcurrentLinkedQueue<Track>();
            if (cSaveTracks) { addTrack(new Track(time(), pClient)); }
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
        double mIntervalBase;
        double mInvIntervalDelta;
        double mBlockBase;
        double mInvBlockDelta;
        
        public GcService(double pGcInterval, double pBlockTime)
        {
            super();
            mGcInterval = pGcInterval;
            mBlockTime = pBlockTime;
            mIntervalBase = mGcInterval * 0.75;
            mInvIntervalDelta = 1.0 / (mGcInterval * 0.50);
            mBlockBase = mBlockTime * 0.75;
            mInvBlockDelta = 1.0 / (mBlockTime * 0.50);
        }

        // Add threads to be affected by this JVM
        public void addThread(ServerBase pServerBase)
        {
            mThreadList.add(pServerBase);
        }
        
        private final static int cHoldGcDelta = 1;
        private static final int cForNext = 2;
        private static final int cForEnd = 3;

        int mFor_i;
        
        double mBlock;
        public int actions()
        {
            switch (mState)
            {
                case cProcessInit:
                    //hold(cRandom.uniform(mGcInterval, mGcInterval * 1.5));
                    hold( mIntervalBase + cRandom.negexp(mInvIntervalDelta));
                    return cHoldGcDelta;

                case cHoldGcDelta:
                    // if idle, set an earliest start time.
                    // if not idle, updated with new time if past current schedule
                    //double tBlock = cRandom.uniform(mBlockTime, mBlockTime * 1.5);
                    mBlock = mBlockBase + cRandom.negexp(mInvBlockDelta);
                    mFor_i = -1;
                    // Flow into loop

                case cForNext:
                    ++mFor_i;
                    // Handle situation when batches are out of order
                    //for (ServerBase tService : mThreadList)
                    if (mFor_i >= mThreadList.size())
                    {
                        return cForEnd;
                    }

                    ServerBase tService = mThreadList.get(mFor_i); 
                    if (tService.idle())
                    {
                        tService.mBlockedTime = time() + mBlock;
                        if (cTrace)
                        {
                            //System.out.println(tService.mName + " Gc block " + time() + " " + tBlock);
                        }
                    }
                    else
                    {
                        double tEndOfBreak = tService.evTime() +  mBlock;

                        Request tRequest = tService.mRequest; //getRequest();
                        String tMessage = tService.mName + " GC delay " + + tService.evTime() + " to " + tEndOfBreak; 
                        if (tRequest != null)
                        {
                            if (cSaveTracks) { tRequest.addTrack(new Track(time(), tMessage)); }
                        }
                        // If execution, execution will be extended
                        if (cTrace)
                        {
                            //System.out.println(tMessage);
                        }
                        reactivate(tService, at, tEndOfBreak);
                        // We should use for loop case but reactivation is in the future and
                        // current should not change.
                    }
                    return cForNext;
                    
                case cForEnd:
                    return cProcessInit;
            }
            return cProcessError;
        } 
    }

    public static String ind(int pLen)
    {
        return cBlanks.substring(0, pLen);
    }

    private static final int cHoldNormal = 0;
    private static final int cHoldNegexp = 1;
    private static final int cHoldPoisson = 2;
    private static final int cHoldUniform = 3;
    
    public static class ServerBase extends Process
    {
        ServerConfig mServerConfig;
        String mName;
        int mId;
        Request mRequest;
        double mBlockedTime;
        boolean mTakeTimeIn;
        boolean mTakeTimeOut;
        //int mHoldType = cHoldPoisson;
        int mHoldType = cHoldNormal;
        
        ArrayList<String> mBlockedMessage = new ArrayList<String>();
        
        public ServerBase(ServerConfig pServerConfig, int pId)
        {
            mServerConfig = pServerConfig;
            mId = pId;
            mName = mServerConfig.mName + "_" + mId;
        }

        public boolean checkBlockX()
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

        public double getBlockDelay()
        {
            // If we were in GC-block while idle await GC completion until earliest non-block time
            if (mBlockedTime == 0)
            {
                return 0.0;
            }

            double tBlockDelay = mBlockedTime - time();
            mBlockedTime = 0;
            if (tBlockDelay <= 0)
            {
                return 0.0;
            }
            mBlockedMessage.add(mName + " GC blocked from " + time() + " + until " + (time() + tBlockDelay));
            return tBlockDelay;
        }

        
        public void logBlock()
        {
            if (mBlockedMessage.size() != 0)
            {
                for (String tMsg : mBlockedMessage)
                {
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), tMsg)); }
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
                        if (cSaveTracks) { pBatch.mRequests[i].addTrack(new Track(time(), tMsg)); }
                    }
                }
                mBlockedMessage.clear();
            }
        }

        public void holdForWorkXX()
        {
            // Wait pre service time
            if (cFixedServiceTimed)
            {
                hold(mServerConfig.mServiceTime);
                out();
                return;
            }
            double tBase;
            double tDelta;
            switch (mHoldType)
            {
                case cHoldNormal:
                    hold(cRandom.normal(mServerConfig.mServiceTime, mServerConfig.mServiceTime * 0.1)); //0.3
                    break;

                case cHoldNegexp:
                    tBase = mServerConfig.mServiceTime * 0.75;
                    tDelta = 1.0 / (mServerConfig.mServiceTime * 0.50);
                    hold( tBase + cRandom.negexp(tDelta));
                    break;

                case cHoldPoisson:
                    tBase = mServerConfig.mServiceTime * 0.75;
                    tDelta = mServerConfig.mServiceTime * 0.50;
                    hold( tBase + cRandom.poisson(tDelta));
                    break;

                case cHoldUniform:
                    hold(cRandom.uniform(mServerConfig.mServiceTime * 0.75, mServerConfig.mServiceTime * 1.5));
                    break;
            }
            out();
        }

        public double getWorkTime()
        {
            // Wait pre service time
            if (cFixedServiceTimed)
            {
                return (mServerConfig.mServiceTime);
            }
            double tBase;
            double tDelta;
            switch (mHoldType)
            {
                case cHoldNormal:
                    return (cRandom.normal(mServerConfig.mServiceTime, mServerConfig.mServiceTime * 0.1)); //0.3

                case cHoldNegexp:
                    tBase = mServerConfig.mServiceTime * 0.75;
                    tDelta = 1.0 / (mServerConfig.mServiceTime * 0.50);
                    return (tBase + cRandom.negexp(tDelta));

                case cHoldPoisson:
                    tBase = mServerConfig.mServiceTime * 0.75;
                    tDelta = mServerConfig.mServiceTime * 0.50;
                    return (tBase + cRandom.poisson(tDelta));

                case cHoldUniform:
                    return (cRandom.uniform(mServerConfig.mServiceTime * 0.75, mServerConfig.mServiceTime * 1.5));
            }
            return 0.0;
        }

        private static final int cWaitForGc = 1;
        private static final int cNoGcBlock = 2;
        private static final int cHoldForWork = 3;
        private static final int cCallDone = 4;
        private static final int cNoCall = 5;
        private static final int cWaitForGcB = 6;
        private static final int cNoGcBlockB = 7;
        private static final int cWaitForWork = 8;
        private static final int cQueueActivate = 9;
        private static final int cCheckForWork = 10;

        Request mCurrentRequest;
        public int actions()
        {
            switch (mState) 
            {
                case cProcessInit:
                    out();

                case cCheckForWork:
                    if (mServerConfig.mIn.mQueue.empty())
                    {
                        if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Waiting for work")); }
                        wait(mServerConfig.mIn.mProcess);
                        return cProcessInit;
                    }
                    // Flow into as long as we have work to do
                    
                    // If we were in GC-block while idle await GC completion until earliest non-block time
                    double tBlockDelay = getBlockDelay();
                    if (tBlockDelay == 0.0)
                    {
                        return cNoGcBlock;
                    }
                    hold(tBlockDelay);
                    return cWaitForGc;

                case cWaitForGc:
                    out();
                    return cProcessInit;

                case cNoGcBlock:
                    // Get request and take it out from queue.
                    mCurrentRequest = mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    //mRequest = mRequest.getRequest();
                    logBlock();

                    if (mTakeTimeIn)
                    {
                        mRequest.mTimeIn = time();
                    }
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Arrived")); }


                    hold(getWorkTime());
                    return cHoldForWork;
                    
                case cHoldForWork:
                    out();
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " After Service")); }
                    
                    if (mServerConfig.mCall == null)
                    {
                        return cNoCall;
                    }
                    this.into(mRequest.mWaitList);
                    if (mServerConfig.mCall.queue(mCurrentRequest))
                    {
                        return cQueueActivate;
                    }
                    // Fall thru

                case cQueueActivate:
                    passivate();
                    return cCallDone;
                    
                case cCallDone:
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Return after call")); }
                    mState = cCallDone;

                case cNoCall:
                    tBlockDelay = getBlockDelay();
                    if (tBlockDelay == 0.0)
                    {
                        return cNoGcBlockB;
                    }
                    hold(tBlockDelay);
                    return cWaitForGcB;

                case cWaitForGcB:
                    out();
                    logBlock();

                case cNoGcBlockB:
                    if (mTakeTimeOut)
                    {
                        mRequest.mTimeOut = time();
                    }

                    // Queue to next server or return
                    if (mServerConfig.mOut != null)
                    {
                        if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Queue next")); }
                        if (mServerConfig.mOut.queue(mCurrentRequest))
                        {
                            return cCheckForWork;
                        }
                    }
                    else
                    {
                        // Re-activate calling server with potential gc delay
                        if (!mRequest.mWaitList.empty())
                        { 
                            if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " return")); }
                            ServerBase tService = (ServerBase) mRequest.mWaitList.last();
                            tService.out();
                            activate(tService);
                            return cCheckForWork;
                        }
                    }
                    return cCheckForWork;
                    
            }
            return cProcessError;
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
        
//        public ServerConfig(ProcQueue pIn, ProcQueue pCall, ProcQueue pOut, String pName, int pMinPoolSize, int pMaxPoolSize, double pServiceTime)
//        {
//            mName = pName;
//            mIn = pIn;
//            mCall = pCall;
//            mOut = pOut;
//            mServiceTime = pServiceTime;
//            mMinPoolSize = pMinPoolSize;
//            mMaxPoolSize = pMaxPoolSize;
//        }
        
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
        
//        public ServerConfig(GcService pJvm, ProcQueue pIn, ProcQueue pCall, ProcQueue pOut, String pName, int pMinPoolSize, int pMaxPoolSize, double pServiceTime)
//        {
//            mName = pName;
//            mIn = pIn;
//            mCall = pCall;
//            mOut = pOut;
//            mServiceTime = pServiceTime;
//            mMinPoolSize = pMinPoolSize;
//            mMaxPoolSize = pMaxPoolSize;
//            mJvm = pJvm;
//        }
        
        
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
                //System.out.println("add Process " + pServerBase.mName + " into Head" + mProcess);
            }
            ++mPoolSize;
        }

        public boolean queue(Request pRequest)
        {
            // Activate server if inactive
            pRequest.into(mQueue);
            
            // Add thread to pool
            if (mPoolSize < mServerConfig.mMaxPoolSize && mProcess.empty())
            {
                createProcess(mPoolSize);
                //System.out.println("add #" + mPoolSize + " Process " + pRequest + " into Queue" + mQueue);
            }
            
            if (!mProcess.empty())
            { 
            	//System.out.println("Activate the first process in queue " + mProcess.first());
                Process.activate((Process) mProcess.first());
                return true;
            }
            return false;
        }

    }
    
    
    public static class MeSorter extends ServerBase
    {
        public MeSorter(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }

        private static final int cWaitForGc = 1; 
        private static final int cNoGcBlock = 2;
        private static final int cHoldForWork = 3;
        private static final int cWaitForWork = 4;
        private static final int cBatchQueued = 5;
        private static final int cRecoveryQueued = 6;
        private static final int cCheckForWork = 7;

        Request mCurrentRequest;
        public int actions()
        {
            switch (mState) 
            {
                case cProcessInit:
                    out();

                case cCheckForWork:
                    if (mServerConfig.mIn.mQueue.empty())
                    {
                        if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Waiting for work")); }
                        wait(mServerConfig.mIn.mProcess);
                        return cProcessInit;
                    }
                    
                    // If we were in GC-block while idle await GC completion until earliest non-block time
                    double tBlockDelay = getBlockDelay();
                    if (tBlockDelay == 0.0)
                    {
                        return cNoGcBlock;
                    }
                    hold(tBlockDelay);
                    return cWaitForGc;

                case cWaitForGc:
                    out();
                    return cProcessInit;

                case cNoGcBlock:
                    // Get request and take it out from queue.
                    mCurrentRequest = mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    //mRequest = mRequest.getRequest();
                    logBlock();
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Arrived")); }

                    hold(getWorkTime());
                    return cHoldForWork;
                    
                case cHoldForWork:
                    out();
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " After Service")); }

                    // Need to create request clones for these calls!
                    mRequest.mMeResponses = 3;
                    if (cInstance.mMePrimaryBatchSender.queue(new Request(mRequest)))
                    {
                        return cBatchQueued;
                    }
                    //return cBatchQueued;

                case cBatchQueued:
                    if (cInstance.mMePrimaryRecoveryLog.queue(new Request(mRequest)))
                    {
                        return cRecoveryQueued;
                    }
                    //return cRecoveryQueued;

                case cRecoveryQueued:
                    // Chain Unit 0 will get the original request
                    if (mServerConfig.mOut.queue(mRequest))
                    {
                        return cCheckForWork;
                    }
                    return cCheckForWork;
                    
            }
            return cProcessError;
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
                mRequests[i].addTrack(pTrack);
            }
        }

    }
    

    public static class MeBatchSender extends ServerBase
    {
        public MeBatchSender(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }
        
        private final static int cWaitForGc = 1;
        private final static int cNoGcBlock = 2;
        private final static int cHoldForWork = 3;
        private final static int cCallDone = 4;
        private final static int cNoCall = 5;
        private final static int cWaitForGcB = 6;
        private final static int cNoGcBlockB = 7;
        private final static int cForNext = 8;
        private final static int cForEnd = 9;
        private final static int cWaitForWork = 10;
        private static final int cCheckForWork = 11;

        
        int mMaxBatch = 40;
        BatchRequest mBatch = new BatchRequest(mMaxBatch);
        Request mCurrentRequest;
        int mFor_i;
        
        public int actions()
        {
            switch (mState) 
            {
                case cProcessInit:
                    out();
                    mBatch.mCount = 0;

                case cCheckForWork:
                    if (mServerConfig.mIn.mQueue.empty())
                    {
                        if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Waiting for work")); }
                        wait(mServerConfig.mIn.mProcess);
                        return cProcessInit;
                    }
                    // Flow into as long as we have work to do
                    
                    // If we were in GC-block while idle await GC completion until earliest non-block time
                    double tBlockDelay = getBlockDelay();
                    if (tBlockDelay == 0.0)
                    {
                        return cNoGcBlock;
                    }
                    hold(tBlockDelay);
                    return cWaitForGc;

                case cWaitForGc:
                    out();
                    return cProcessInit;

                case cNoGcBlock:
                    // Get request and take it out from queue.
                    mCurrentRequest = mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    //mRequest = mRequest.getRequest();
                    logBlock();

                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Arrived")); }
                    mBatch.mRequests[mBatch.mCount++] = mRequest;
                    if ( mBatch.mCount < mMaxBatch && !mServerConfig.mIn.mQueue.empty())
                    {
                        return cProcessInit;
                    }

                    hold(getWorkTime());
                    return cHoldForWork;
                    
                case cHoldForWork:
                    out();
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " After Service")); }
                    
//                    if (mServerConfig.mCall == null)
//                    {
//                        return cNoCall;
//                    }
//                    this.into(mBatch.mWaitList);
//                    mServerConfig.mCall.queue(mBatch);
//                    passivate();
//                    return cCallDone;
                    
                case cCallDone:
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Return after call")); }
                    mState = cNoCall;

                case cNoCall:
                    tBlockDelay = getBlockDelay();
                    if (tBlockDelay == 0.0)
                    {
                        return cNoGcBlockB;
                    }
                    hold(tBlockDelay);
                    return cWaitForGcB;

                case cWaitForGcB:
                    out();
                    logBlock();

                case cNoGcBlockB:
                    if (cSaveTracks) { mBatch.addTrack(new Track(time(), mName + " Return after batch call")); }
                    mFor_i = -1;
                    // Flow into loop

                case cForNext:
                    ++mFor_i;
                    // Handle situation when batches are out of order
                    // for (int i = 0; i < tBatch.mCount; ++i)
                    if (mFor_i >= mBatch.mCount)
                    {
                        return cForEnd;
                    }

                    // Queue to next server or return
                    if (mServerConfig.mOut != null)
                    {
                        if (cSaveTracks) { mBatch.mRequests[mFor_i].addTrack(new Track(time(), mName + " Queue next")); }
                        if (mServerConfig.mOut.queue(mBatch.mRequests[mFor_i]))
                        {
                            return cForNext;
                        }
                    }
                    else
                    {
                        // Re-activate calling server with potential gc delay
                        if (!mRequest.mWaitList.empty())
                        { 
                            if (cSaveTracks) { mBatch.mRequests[mFor_i].addTrack(new Track(time(), mName + " return")); }
                            ServerBase tService = (ServerBase) mRequest.mWaitList.last();
                            tService.out();
                            activate(tService);
                            return cForNext;
                        }
                    }
                    return cForNext;

                case cForEnd:
                    return cCheckForWork;

            }
            return cProcessError;
        }
    }

    
    public static class MePostChain extends ServerBase
    {
        public MePostChain(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }
        
        private static final int cWaitForGc = 1; 
        private static final int cNoGcBlock = 2;
        private static final int cHoldForWork = 3;
        private static final int cWaitForWork = 4;
        private static final int cCheckForWork = 5;

        public int actions()
        {
            switch (mState) 
            {
                case cProcessInit:
                    out();

                case cCheckForWork:
                    if (mServerConfig.mIn.mQueue.empty())
                    {
                        if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Waiting for work")); }
                        wait(mServerConfig.mIn.mProcess);
                        return cProcessInit;
                    }
                    // Flow into as long as we have work to do
                    
                    // If we were in GC-block while idle await GC completion until earliest non-block time
                    double tBlockDelay = getBlockDelay();
                    if (tBlockDelay == 0.0)
                    {
                        return cNoGcBlock;
                    }
                    hold(tBlockDelay);
                    return cWaitForGc;

                case cWaitForGc:
                    out();
                    return cProcessInit;

                case cNoGcBlock:
                    // Get request and take it out from queue.
                    mRequest = (Request) mServerConfig.mIn.mQueue.first();
                    mRequest.out();
                    logBlock();
                    
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " Arrived")); }

//                    hold(getWorkTime());
//                    return cHoldForWork;
//                    
//                case cHoldForWork:
//                    out();
//                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " After Service")); }
                    

                    mRequest = mRequest.getRequest();
                    // Wait for all three request
                    // Then activate response thread
                    if (--mRequest.mMeResponses != 0)
                    {
                        return cCheckForWork;
                    }

                    hold(getWorkTime());
                    return cHoldForWork;
                    
                case cHoldForWork:
                    out();
                    if (cSaveTracks) { mRequest.addTrack(new Track(time(), mName + " After Service")); }

                    if (mServerConfig.mOut.queue(mRequest))
                    {
                        return cCheckForWork;
                    }
                    return cCheckForWork;
            }
            return cProcessError;
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

    public static class TaxCollector extends ServerBase
    {
        public TaxCollector(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
            mTakeTimeOut = true;
        }
    }
    
    public static void logTransMon(Request pRequest)
    {
        long tMilliTime = cStartTime + (long) (pRequest.mTimeOut * 0.001);
        int tMicroTime  = 1000 + (int) pRequest.mTimeOut % 1000;
        String tMicro = Integer.toString(tMicroTime).substring(1,4);
        
        // ";time;eligable;duration;excuses;result;transid;ip:port;user;req;rsp;partition;obid;conn;osize;meta";
        cDate.setTime(tMilliTime);
        String tTime = cDateFormat.format(cDate) + tMicro;
        int tTaxTime = (int) (pRequest.mTimeOut - pRequest.mTimeIn);
        System.out.println(tTime + ";y;" + tTaxTime + ";;3001;" + pRequest.mRequest + ";;" + pRequest.mClient + ";;;1;;" + pRequest.mClient + ";;");
    }
    
}
