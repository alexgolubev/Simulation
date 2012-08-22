package com.cinnober.simulation.thesis;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.cinnober.simulation.Process;

import dk.ruc.javaSimulation.Head;
import dk.ruc.javaSimulation.Link;
import dk.ruc.javaSimulation.Random;


/**
* Simulation of a queuing system. One TAX server, one ME primary, and one ME standby 
*
* This version models is based on passive customers and active servers.
*/

public class Simulation extends Process {
    
    public static Simulation cInstance; 
    protected static final String cConfigName = "StationParameters.cfg";
    protected static Properties cConfiguration;
    
    // Following attributes can be set to true if additional info is needed while running simulation
    static boolean cTrace;
    static boolean cDumpTracks;
    static boolean cTransLogMon;
    static boolean cSaveTracks;

    static double cTaxSum;
    static double cClientSum;
    static int cRequestNums;
    
    static double cClientStartTime;

    static Random cRandom = new Random();
    
    static final int MICROS_IN_SEC = 1000 * 1000;
    static final int MICROS_IN_MILLI = 1000;
    
    // Run for 20 minutes expressed in microseconds
    static double cSimTimeMicro = 20 * 60 * MICROS_IN_SEC;
    static int cClientCount = 100;     
    static double cTps = 1400.0D;
    
    // Total transactions from all clients per microsecond
    static double cClientTransPerMicro = cTps / MICROS_IN_SEC; 
    
    static double cActualRunTimeMicro;
    static int cMaxRequestsToSend; 
    
    private static final int cMeJvm = 1;
    private static final int cMeStandbyJvm = 2;
    private static final int cForNext = 3;
    private static final int cNewClient = 4;
    private static final int cForEnd = 5;
    private static final int cHoldRunTimeMicro = 6;
    private static final int cHoldRunTime = 7;
    private static final int cJvmActivated = 8;
    
    private static final boolean cFixedServiceTime = false;
    private static final int cHoldNormal = 0;
    private static final int cHoldNegexp = 1;
    private static final int cHoldPoisson = 2;
    private static final int cHoldUniform = 3;
    
    private static SimulationUtils cUtils = new SimulationUtils(); 
    private static Logger cLogger; 
    
    Process[] mClientList;
    double mHoldTimeBetweenClients;
    int mForI;
        
    ArrayList<GcService> mGcList = new ArrayList<GcService>();
    
    // Service Stations and their configurations read from file
    ProcQueue mClient = new ProcQueue(Client.class);
    ProcQueue mClientTaxNwDelay = new ProcQueue();
    ProcQueue mTaxMeNwDelay = new ProcQueue();
    ProcQueue mMeMesNwDelay = new ProcQueue();
    ProcQueue mMesMeNwDelay = new ProcQueue();
    ProcQueue mMeTaxNwDelay = new ProcQueue();
    ProcQueue mTaxClientNwDelay = new ProcQueue();
    
    ServerConfig mClientConfig;
    ServerConfig mClientTaxNwDelayConfig;
    ServerConfig mTaxMeNwDelayConfig;
    ServerConfig mMeMesNwDelayConfig;
    ServerConfig mMesMeNwDelayConfig;
    ServerConfig mMeTaxNwDelayConfig;
    ServerConfig mTaxClientNwDelayConfig;
    
    // A ME Primary contains the following threads
    ProcQueue mMePrimaryTcp = new ProcQueue();
    ProcQueue mMePrimaryServerPool = new ProcQueue();
    ProcQueue mMePrimarySorter = new ProcQueue(MeSorter.class);
    ProcQueue mMePrimaryChainUnit0 = new ProcQueue();
    ProcQueue mMePrimaryBatchSender = new ProcQueue(MeBatchSender.class);
    ProcQueue mMePrimaryRecoveryLog = new ProcQueue();
    ProcQueue mMePrimaryPostChain = new ProcQueue(MePostChain.class);
    ProcQueue mMePrimaryResponsePool = new ProcQueue();
    
    ServerConfig mMePrimaryTcpConfig;
    ServerConfig mMePrimaryServerPoolConfig;
    ServerConfig mMePrimarySorterConfig;
    ServerConfig mMePrimaryChainUnit0Config;
    ServerConfig mMePrimaryBatchSenderConfig;
    ServerConfig mMePrimaryRecoveryLogConfig;
    ServerConfig mMePrimaryPostChainConfig;
    ServerConfig mMePrimaryResponsePoolConfig;

    // A ME Standby contains the following threads
    ProcQueue mMeStandbyTcp = new ProcQueue();
    ProcQueue mMeStandbyServerPool = new ProcQueue();
    
    ServerConfig mMeStandbyTcpConfig;
    ServerConfig mMeStandbyServerPoolConfig;

    // A TAX contains the following threads
    ProcQueue mTaxTcp = new ProcQueue(TaxServerTcp.class);
    ProcQueue mTaxPool = new ProcQueue();
    ProcQueue mTaxCollector = new ProcQueue(TaxCollector.class);
    
    ServerConfig mTaxTcpConfig;
    ServerConfig mTaxPoolConfig;
    ServerConfig mTaxCollectorConfig;
    
    GcService mTaxJvm;
    GcService mMeJvm;
    GcService mMeStandbyJvm;
   
    public enum JvmType { HOTSPOT, METRONOME }
    
    JvmType mJvmType = JvmType.HOTSPOT;
    
    public Simulation() {
        
    }
    
    public static void main(String[] pArgs) {
        
        cInstance = new Simulation(); 
        cInstance.init();
        cLogger = Logger.getSingleton();
        activate(cInstance);
    }

    public void init() {
        
        loadConfig();
        
        try {
            initiateJvms();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        initiateServerConfigs();
        initiateStations();
    }
    
    private void loadConfig() {

        cConfiguration = cUtils.loadConfiguration(new Properties(), cConfigName);
    }
    
    private void initiateJvms() throws Exception {
        
        // Figures below valid for 1000TPS
        double tGcRateFactor =  1000.0D / cTps;
        switch (mJvmType) {
            
            case HOTSPOT:
                // tax average 6ms, max 65, every 20 seconds
                // me1 27ms, max 83ms, every 20 seconds
                // me1s 27ms, max 87, every 22 seconds
                mTaxJvm = new GcService(0.8D * MICROS_IN_SEC * tGcRateFactor, 6.0D * MICROS_IN_MILLI); 
                mMeJvm = new GcService(1.2D * MICROS_IN_SEC * tGcRateFactor, 12.0D * MICROS_IN_MILLI); 
                mMeStandbyJvm = new GcService(1.2D * MICROS_IN_SEC * tGcRateFactor, 12.0D * MICROS_IN_MILLI);
                break;
            
            case METRONOME:
                mTaxJvm = new GcService(100 * 1000, 500);
                mMeJvm = new GcService(100 * 1000, 500);
                mMeStandbyJvm = new GcService(100 * 1000, 500);
                break;
            
            default :
                throw new Exception("No Jvm set for simulation!");
        }
    }
    
    private void initiateServerConfigs() {
    
        mClientConfig = new ServerConfig(
            mClient, mClientTaxNwDelay, SimulationUtils.CLIENT, cClientCount, cClientCount);
        mClientTaxNwDelayConfig = new ServerConfig(
            mClientTaxNwDelay, mTaxTcp, SimulationUtils.CLIENT_TAX_NW, 1, 1);
        mTaxTcpConfig = new ServerConfig(
            mTaxJvm, mTaxTcp, mTaxPool, SimulationUtils.TAX_TCP, 1, 1);
        mTaxPoolConfig = new ServerConfig(
            mTaxJvm, mTaxPool, mTaxMeNwDelay, SimulationUtils.TAX_SERVER_POOL, 80, 80);
        mTaxMeNwDelayConfig = new ServerConfig(
            mTaxMeNwDelay, mMePrimaryTcp, SimulationUtils.TAX_ME_NW, 1, 1);
        mMePrimaryTcpConfig = new ServerConfig(
            mMeJvm, mMePrimaryTcp, mMePrimaryServerPool, SimulationUtils.ME_TCP, 1, 1);
        mMePrimaryServerPoolConfig = new ServerConfig(
            mMeJvm, mMePrimaryServerPool, mMePrimarySorter, SimulationUtils.ME_SERVER_POOL, 5, 150);
        mMePrimarySorterConfig = new ServerConfig(
            mMeJvm, mMePrimarySorter, mMePrimaryChainUnit0, SimulationUtils.ME_SORTER, 1, 1); 
        
        // copies of message sent to 3 different processes
        {
            // Main flow
            mMePrimaryChainUnit0Config = new ServerConfig(
                mMeJvm, mMePrimaryChainUnit0, mMePrimaryPostChain, SimulationUtils.ME_CHAIN_UNIT0, 1, 1);
            
            // Replication flow
            mMePrimaryBatchSenderConfig = new ServerConfig(
                mMeJvm, mMePrimaryBatchSender, mMeMesNwDelay, SimulationUtils.ME_BATCH_SENDER, 10, 10);
            mMeMesNwDelayConfig = new ServerConfig(
                mMeMesNwDelay, mMeStandbyTcp, SimulationUtils.ME_MES_NW, 1, 1);
            mMeStandbyTcpConfig = new ServerConfig(
                mMeStandbyJvm, mMeStandbyTcp, mMeStandbyServerPool, SimulationUtils.MES_TCP, 1, 1);
            mMeStandbyServerPoolConfig = new ServerConfig(
                mMeStandbyJvm, mMeStandbyServerPool, mMesMeNwDelay, SimulationUtils.MES_SERVER_POOL, 5, 150);
            mMesMeNwDelayConfig = new ServerConfig(
                mMesMeNwDelay, mMePrimaryPostChain, SimulationUtils.MES_ME_NW, 1, 1);
            
            // Recovery flow
            mMePrimaryRecoveryLogConfig = new ServerConfig(
                mMeJvm, mMePrimaryRecoveryLog, mMePrimaryPostChain, SimulationUtils.ME_RECOVERY, 1, 1);
        }
            
        mMePrimaryPostChainConfig = new ServerConfig(
            mMeJvm, mMePrimaryPostChain, mMePrimaryResponsePool, SimulationUtils.ME_POST_CHAIN, 1, 1);
        mMePrimaryResponsePoolConfig = new ServerConfig(
            mMeJvm, mMePrimaryResponsePool, mMeTaxNwDelay, SimulationUtils.ME_RESPONSE_POOL, 5, 25);
        mMeTaxNwDelayConfig = new ServerConfig(
            mMeTaxNwDelay, mTaxCollector, SimulationUtils.ME_TAX_NW, 1, 1);
        mTaxCollectorConfig = new ServerConfig(
            mTaxJvm, mTaxCollector, mTaxClientNwDelay, SimulationUtils.TAX_COLLECTOR, 1, 1);
        mTaxClientNwDelayConfig = new ServerConfig(
            mTaxClientNwDelay, null, SimulationUtils.TAX_CLIENT_NW, 1, 1);
    }

    private void initiateStations() {
        
        mClient.init(mClientConfig);
        mClientTaxNwDelay.init(mClientTaxNwDelayConfig); 
        mTaxTcp.init(mTaxTcpConfig); 
        mTaxPool.init(mTaxPoolConfig);
        mTaxMeNwDelay.init(mTaxMeNwDelayConfig); 
        mMePrimaryTcp.init(mMePrimaryTcpConfig);
        mMePrimaryServerPool.init(mMePrimaryServerPoolConfig); 
        mMePrimarySorter.init(mMePrimarySorterConfig);
        mMePrimaryChainUnit0.init(mMePrimaryChainUnit0Config); 
        mMePrimaryBatchSender.init(mMePrimaryBatchSenderConfig);
        mMePrimaryRecoveryLog.init(mMePrimaryRecoveryLogConfig); 
        mMePrimaryPostChain.init(mMePrimaryPostChainConfig); 
        mMePrimaryResponsePool.init(mMePrimaryResponsePoolConfig); 
        mMeMesNwDelay.init(mMeMesNwDelayConfig);
        mMeStandbyTcp.init(mMeStandbyTcpConfig); 
        mMeStandbyServerPool.init(mMeStandbyServerPoolConfig);
        mMesMeNwDelay.init(mMesMeNwDelayConfig);
        mMeTaxNwDelay.init(mMeTaxNwDelayConfig);
        mTaxCollector.init(mTaxCollectorConfig);
        mTaxClientNwDelay.init(mTaxClientNwDelayConfig);
    }

    // All times in ms.
    public int actions() {
        switch (mState) 
        {
            case cProcessInit:
                out();
                if (cSimTimeMicro < cClientCount / cClientTransPerMicro) {
                    cSimTimeMicro = cClientCount / cClientTransPerMicro;
                    cLogger.logSimTimeTooShort(cSimTimeMicro);
                }
                cMaxRequestsToSend = (int) (cClientTransPerMicro * cSimTimeMicro);
                cLogger.logMaxNumOfRequests(cMaxRequestsToSend);
                
                if (cTransLogMon)
                {
                    cLogger.logTransactionMonitorHeader();
                }

                activate(mTaxJvm);
                return cMeJvm;
                
            case cMeJvm:
                activate(mMeJvm);
                return cMeStandbyJvm;

            case cMeStandbyJvm:
                activate(mMeStandbyJvm);
                return cJvmActivated;

            case cJvmActivated:
                mClientList = cUtils.headToArray(mClient.mProcess, new Process[0]);
               
                //evenly distributed client over 1 second interval
                double tInterval = 1.0D * MICROS_IN_SEC / (cClientCount);                
                
                //hold until connect next client to the system 
                mHoldTimeBetweenClients = cRandom.normal(tInterval, tInterval * 0.7);
                mForI = -1;
                // Flow into loop

            case cForNext:
                ++mForI;
                //for (int i = 0; i < tClientList.length; ++i)    //For every client
                if (mForI >= mClientList.length)
                {
                    return cForEnd;
                }

                cClientStartTime = time();
                activate((Process) mClientList[mForI]); 
                return cNewClient;
                
            case cNewClient:
                hold(mHoldTimeBetweenClients);
                return cForNext;

            case cForEnd:
 
            case cHoldRunTimeMicro:
                cActualRunTimeMicro = cSimTimeMicro * 2;
                hold(cActualRunTimeMicro);
                return cHoldRunTime;
                    
            case cHoldRunTime:
                cLogger.logSimulationFinished(cSimTimeMicro, cRequestNums, cTaxSum, cClientSum);        
                return cProcessDone;
            
            default :
                break;
        }
        return cProcessError;
    }
    
    /**
     * 
     */
    public static class ServerConfig {
        
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
        
        public ServerConfig(ProcQueue pIn, ProcQueue pOut, String pName, int pMinPoolSize, 
            int pMaxPoolSize) {
            
            mName = pName;
            mIn = pIn;
            mOut = pOut;
            mServiceTime = Double.valueOf(cConfiguration.getProperty(pName));
            mMinPoolSize = pMinPoolSize;
            mMaxPoolSize = pMaxPoolSize;
        }
        
       
        public ServerConfig(GcService pJvm, ProcQueue pIn, ProcQueue pOut, String pName, int pMinPoolSize, 
            int pMaxPoolSize) {
            
            mName = pName;
            mIn = pIn;
            mOut = pOut;
            mServiceTime = Double.valueOf(cConfiguration.getProperty(pName));
            mMinPoolSize = pMinPoolSize;
            mMaxPoolSize = pMaxPoolSize;
            mJvm = pJvm;
        }        
    }

    /**
     * 
     */
    public static class ProcQueue
    {
        Head mQueue = new Head();
        Head mProcess = new Head();
        ServerConfig mServerConfig;
        @SuppressWarnings("unchecked")
        Constructor mConstructor;
        int mPoolSize;
        
        public ProcQueue() {
        }
    
        @SuppressWarnings("unchecked")
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
        
        public void addProcess(ServerBase pServerBase) {
            pServerBase.into(mProcess);
            if (mServerConfig.mJvm != null) {
                mServerConfig.mJvm.addThread(pServerBase);
            }
            ++mPoolSize;
        }
    
        public boolean queue(Request pRequest) {
            // Activate server if inactive
            pRequest.into(mQueue);
            
            // Add thread to pool
            if (mPoolSize < mServerConfig.mMaxPoolSize && mProcess.empty()) {
                createProcess(mPoolSize);
            }
            
            if (!mProcess.empty()) { 
                Process.activate((Process) mProcess.first());
                return true;
            }
            return false;
        }
    
    }
    
    /**
     * 
     */
    public static class Request extends Link {
        
        Request mActualRequest;
        String mClient;
        int mRequestNum;
        double mCreateTime;
        double mTimeIn;
        double mTimeOut;
        // mWaitList contains the waiting servers.
        Head mWaitList;
        int mMeResponses;
        ConcurrentLinkedQueue<Track> mTrackQueue;
    
        public Request() {
            
        }
    
        Request(Request pOriginal) {
            
            mActualRequest = pOriginal;
        }
    
        Request(String pClient) {
            
            mClient = pClient;
            //mCreateTime = time();   //Set when receiving a request at the client
            mRequestNum = ++cRequestNums;   //Sorted for all requests from all clients 
            mWaitList = new Head();
            mTrackQueue = new ConcurrentLinkedQueue<Track>();
            if (cSaveTracks) { addTrack(time(), pClient, cTrace); }
        }
    
        public Request getRequest() {
            
            if (mActualRequest != null) {
                
                return mActualRequest.getRequest();
            }
            return this; 
        }
        
        public void addTrack(double pTime, String pClient, boolean pTrace) {
            
            getRequest().mTrackQueue.add(new Track(pTime, pClient, pTrace));
        }
        
        public void dumpTrack() {
            
            for (Track tTrack : mTrackQueue) {
                
                cLogger.logTrackWithReqNum(mRequestNum, tTrack);
            }
        }
        
        /**
         * 
         */
        public static class Track {

            private double mTime;
            private String mMsg;
            
            public Track(double pTime, String pMsg, boolean pTrace)
            {
                super();
                
                mTime = pTime;
                mMsg = pMsg;
                if (pTrace)
                {
                    Logger.getSingleton().logTrack(this);
                }
            }
            
            public double getTime() {
                return mTime;
            }
            
            public String getMsg() {
                return mMsg;
            }
        }
    }

    /**
     * 
     */
    public static class GcService extends Process {
        
        private static final int cHoldGcDelta = 1;
        private static final int cForNext = 2;
        private static final int cForEnd = 3;
        
        int mForI;
        double mBlock;
        
        ArrayList<ServerBase> mThreadList = new ArrayList<ServerBase>();
        double mGcInterval;
        double mBlockTime;
        double mIntervalBase;
        double mInvIntervalDelta;
        double mBlockBase;
        double mInvBlockDelta;
        
        public GcService(double pGcInterval, double pBlockTime) {
            
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
    
        public int actions() {
            switch (mState)
            {
                case cProcessInit:
                    //hold(cRandom.uniform(mGcInterval, mGcInterval * 1.5));
                    hold(mIntervalBase + cRandom.negexp(mInvIntervalDelta));
                    return cHoldGcDelta;
    
                case cHoldGcDelta:
                    // if idle, set an earliest start time.
                    // if not idle, updated with new time if past current schedule
                    //double tBlock = cRandom.uniform(mBlockTime, mBlockTime * 1.5);
                    mBlock = mBlockBase + cRandom.negexp(mInvBlockDelta);
                    mForI = -1;
                    // Flow into loop
    
                case cForNext:
                    ++mForI;
                    // Handle situation when batches are out of order
                    //for (ServerBase tService : mThreadList)
                    if (mForI >= mThreadList.size())
                    {
                        return cForEnd;
                    }
    
                    ServerBase tService = mThreadList.get(mForI); 
                    if (tService.idle())
                    {
                        tService.mBlockedTime = time() + mBlock;
                        if (cTrace) {
                            cLogger.logGcBlock(tService, time(), mBlock);
                        }
                    }
                    else
                    {
                        double tEndOfBreak = tService.evTime() +  mBlock;
    
                        Request tRequest = tService.mRequest; //getRequest();
                        String tMessage = tService.mName + " GC delay " + tService.evTime() + " to " + tEndOfBreak; 
                        if (tRequest != null)
                        {
                            if (cSaveTracks) { tRequest.addTrack(time(), tMessage, cTrace); }
                        }
                        // If execution, execution will be extended
                        reactivate(tService, at, tEndOfBreak);
                        // We should use for loop case but reactivation is in the future and
                        // current should not change.
                    }
                    return cForNext;
                    
                case cForEnd:
                    return cProcessInit;
                default :
                    break;
            }
            return cProcessError;
        } 
    }

    /**
     * 
     */
    public static class ServerBase extends Process {
        
        private static final int cWaitForGc = 1;
        private static final int cNoGcBlock = 2;
        private static final int cHoldForWork = 3;
        private static final int cCallDone = 4;
        private static final int cNoCall = 5;
        private static final int cWaitForGcB = 6;
        private static final int cNoGcBlockB = 7;
        @SuppressWarnings("unused")
        private static final int cWaitForWork = 8;
        private static final int cQueueActivate = 9;
        private static final int cCheckForWork = 10;
    
        Request mCurrentRequest;
        
        ServerConfig mServerConfig;
        String mName;
        int mId;
        Request mRequest;
        double mBlockedTime;
        boolean mTakeTimeIn;
        boolean mTakeTimeOut;
        int mHoldType = cHoldNormal;
        
        ArrayList<String> mBlockedMessage = new ArrayList<String>();
        
        public ServerBase(ServerConfig pServerConfig, int pId)
        {
            mServerConfig = pServerConfig;
            mId = pId;
            mName = mServerConfig.mName + "_" + mId;
        }
    
        public double getBlockDelay()
        {
            // If we were in GC- block while idle await GC completion until earliest non- block time
            if (mBlockedTime == 0)
            {
                return 0.0;
            }
    
            double tBlockDelay = mBlockedTime -  time();
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
                    if (cSaveTracks) { mRequest.addTrack(time(), tMsg, cTrace); }
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
                        if (cSaveTracks) { pBatch.mRequests[i].addTrack(time(), tMsg, cTrace); }
                    }
                }
                mBlockedMessage.clear();
            }
        }
    
        public double getWorkTime()
        {
            // Wait pre service time
            if (cFixedServiceTime)
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
                default :
                    break;
            }
            return mServerConfig.mServiceTime;
        }
    
        
        public int actions()
        {
            switch (mState) 
            {
                case cProcessInit:
                    out();
    
                case cCheckForWork:
                    if (mServerConfig.mIn.mQueue.empty())
                    {
                        //if (cSaveTracks) { mRequest.addTrack(time(), mName + " Waiting for work"); }
                        wait(mServerConfig.mIn.mProcess);
                        return cProcessInit;
                    }
                    // Flow into as long as we have work to do
                    
                    // If we were in GC- block while idle await GC completion until earliest non- block time
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
                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " Arrived", cTrace); }
    
    
                    hold(getWorkTime());
                    return cHoldForWork;
                    
                case cHoldForWork:
                    out();
                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " After Service", cTrace); }
                    
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
                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " Return after call", cTrace); }
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
                        if (cSaveTracks) { mRequest.addTrack(time(), mName + " Queue next", cTrace); }
                        if (mServerConfig.mOut.queue(mCurrentRequest))
                        {
                            return cCheckForWork;
                        }
                    }
                    else
                    {
                        // Re- activate calling server with potential gc delay
                        if (!mRequest.mWaitList.empty())
                        { 
                            if (cSaveTracks) { mRequest.addTrack(time(), mName + " return", cTrace); }
                            ServerBase tService = (ServerBase) mRequest.mWaitList.last();
                            tService.out();
                            activate(tService);
                            return cCheckForWork;
                        }
                    }
                    return cCheckForWork;
                default :
                    break;
                    
            }
            return cProcessError;
        }
    }

    /**
     * Generate message, send into queue eaten from any tax
     * We should implement this as a closed queue!
     */ 
    public static class Client extends ServerBase {
        
        static boolean cInit;
        static double cMicroDelayPerClient;
        static int cMaxRequestsPerClient;
        
        private static final int cForNext = 1;
        private static final int cResumed = 2;
        private static final int cForEnd = 3;
        private static final int cQueueActivate = 4;
        
        boolean mTakeTime = true;
        double mNextRequestStartTime;
        int mForI;
        double mStart;
        
        public Client(ServerConfig pServerConfig, int pId) {
            
            super(pServerConfig, pId);
        }

        
        
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
                    cLogger.logMaxNumOfRequestsPerClient(cMaxRequestsPerClient);    
                    mForI = -1;
                    // Flow into loop

                case cForNext:
                    ++mForI;
                    // for (int i = 0; i < tMaxRequestsPerClient; ++i )
                    if (mForI >= cMaxRequestsPerClient)
                    {
                        return cForEnd;
                    }
                    mStart = time();
                    mRequest = new Request(mName);
                    if (mForI == 0)
                    {
                        mRequest.mCreateTime = cClientStartTime;
                    }
                    else
                    {
                        mRequest.mCreateTime = mNextRequestStartTime;
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
                        double tTaxTime = mRequest.mTimeOut -  mRequest.mTimeIn;
                        cTaxSum += tTaxTime;
                        double tClientTime = time() -  mRequest.mCreateTime;
                        cClientSum += tClientTime;
                        String tInfo = mName + " TaxTime: " + tTaxTime + " ClientTime: " + tClientTime;
                     
                        if (cTrace) {
                            
                            cLogger.logRequestInAndOut(mRequest, mForI);                 
                        }

                        if (cTransLogMon)
                        {
                            cLogger.logTransaction(mRequest);
                        }
                    
                        if (cSaveTracks)
                        {
                            mRequest.addTrack(time(), tInfo, cTrace);
                            if (cDumpTracks)
                            {
                                mRequest.dumpTrack();
                            }
                        } 
                    } 
                
                    //hold until send in next transaction from this client
                    //Variation has to be small enough here!
                    double tHoldTime = cRandom.normal(cMicroDelayPerClient, cMicroDelayPerClient / 1000);  
                    //double tHoldTime = tMicroDelayPerClient;  // evenly distributed according to insertion rate
                    //double tHoldTime = cRandom.negexp(1.0/tMicroDelayPerClient);
                    mNextRequestStartTime = mRequest.mCreateTime + tHoldTime;
                
                    // Calculate delay according to rate
                    double tDelta = tHoldTime -  (time() -  mStart);
    
                    if (tDelta > 0) {
                        
                        hold(tDelta);
                    }
                    return cForNext;

                case cForEnd:
                    return cProcessDone;
                default :
                    break;
            }
            return cProcessError;
        }
    }

    /**
     * 
     */
    public static class MeSorter extends ServerBase {
        
        private static final int cWaitForGc = 1; 
        private static final int cNoGcBlock = 2;
        private static final int cHoldForWork = 3;
        @SuppressWarnings("unused")
        private static final int cWaitForWork = 4;
        private static final int cBatchQueued = 5;
        private static final int cRecoveryQueued = 6;
        private static final int cCheckForWork = 7;

        Request mCurrentRequest;
        public MeSorter(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }

        
        public int actions()
        {
            switch (mState) 
            {
                case cProcessInit:
                    out();

                case cCheckForWork:
                    if (mServerConfig.mIn.mQueue.empty())
                    {
                        if (cSaveTracks) { mRequest.addTrack(time(), mName + " Waiting for work", cTrace); }
                        wait(mServerConfig.mIn.mProcess);
                        return cProcessInit;
                    }
                    
                    // If we were in GC-block while idle await GC completion until earliest non- block time
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
                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " Arrived", cTrace); }

                    hold(getWorkTime());
                    return cHoldForWork;
                    
                case cHoldForWork:
                    out();
                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " After Service", cTrace); }

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
                default :
                    break;
                    
            }
            return cProcessError;
        }
    }

    /**
     * 
     */
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
        public void addTrack(double pTime, String pClient, boolean pTrace)
        {
            for (int i = 0; i < mCount; ++i)
            {
                mRequests[i].addTrack(pTime, pClient, pTrace);
            }
        }

    }
    
    /**
     * 
     */
    public static class MeBatchSender extends ServerBase
    {
               
        private static final int cWaitForGc = 1;
        private static final int cNoGcBlock = 2;
        private static final int cHoldForWork = 3;
        private static final int cCallDone = 4;
        private static final int cNoCall = 5;
        private static final int cWaitForGcB = 6;
        private static final int cNoGcBlockB = 7;
        private static final int cForNext = 8;
        private static final int cForEnd = 9;
        @SuppressWarnings("unused")
        private static final int cWaitForWork = 10;
        private static final int cCheckForWork = 11;

        int mMaxBatch = 40;
        BatchRequest mBatch = new BatchRequest(mMaxBatch);
        Request mCurrentRequest;
        int mForI;
        
        public MeBatchSender(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }
        
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
                        if (cSaveTracks) { mRequest.addTrack(time(), mName + " Waiting for work", cTrace); }
                        wait(mServerConfig.mIn.mProcess);
                        return cProcessInit;
                    }
                    // Flow into as long as we have work to do
                    
                    // If we were in GC- block while idle await GC completion until earliest non- block time
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

                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " Arrived", cTrace); }
                    mBatch.mRequests[mBatch.mCount++] = mRequest;
                    if (mBatch.mCount < mMaxBatch && !mServerConfig.mIn.mQueue.empty()) {
                        
                        return cProcessInit;
                    }

                    hold(getWorkTime());
                    return cHoldForWork;
                    
                case cHoldForWork:
                    out();
                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " After Service", cTrace); }
                                        
                case cCallDone:
                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " Return after call", cTrace); }
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
                    if (cSaveTracks) { mBatch.addTrack(time(), mName + " Return after batch call", cTrace); }
                    mForI = -1;
                    // Flow into loop

                case cForNext:
                    ++mForI;
                    // Handle situation when batches are out of order
                    // for (int i = 0; i < tBatch.mCount; ++i)
                    if (mForI >= mBatch.mCount)
                    {
                        return cForEnd;
                    }

                    // Queue to next server or return
                    if (mServerConfig.mOut != null)
                    {
                        if (cSaveTracks) { 
                            mBatch.mRequests[mForI].addTrack(time(), mName + " Queue next", cTrace); 
                        }
                        if (mServerConfig.mOut.queue(mBatch.mRequests[mForI]))
                        {
                            return cForNext;
                        }
                    }
                    else
                    {
                        // Re- activate calling server with potential gc delay
                        if (!mRequest.mWaitList.empty())
                        { 
                            if (cSaveTracks) { 
                                mBatch.mRequests[mForI].addTrack(time(), mName + " return", cTrace); 
                            }
                            ServerBase tService = (ServerBase) mRequest.mWaitList.last();
                            tService.out();
                            activate(tService);
                            return cForNext;
                        }
                    }
                    return cForNext;

                case cForEnd:
                    return cCheckForWork;
                default :
                    break;

            }
            return cProcessError;
        }
    }

    /**
     * 
     */
    public static class MePostChain extends ServerBase
    {
        
        private static final int cWaitForGc = 1; 
        private static final int cNoGcBlock = 2;
        private static final int cHoldForWork = 3;
        @SuppressWarnings("unused")
        private static final int cWaitForWork = 4;
        private static final int cCheckForWork = 5;

        public MePostChain(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
        }
        
        public int actions()
        {
            switch (mState) 
            {
                case cProcessInit:
                    out();

                case cCheckForWork:
                    if (mServerConfig.mIn.mQueue.empty())
                    {
                        if (cSaveTracks) { mRequest.addTrack(time(), mName + " Waiting for work", cTrace); }
                        wait(mServerConfig.mIn.mProcess);
                        return cProcessInit;
                    }
                    // Flow into as long as we have work to do
                    
                    // If we were in GC- block while idle await GC completion until earliest non- block time
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
                    
                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " Arrived", cTrace); }

//                    hold(getWorkTime());
//                    return cHoldForWork;
//                    
//                case cHoldForWork:
//                    out();
//                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " After Service")); }
                    

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
                    if (cSaveTracks) { mRequest.addTrack(time(), mName + " After Service", cTrace); }

                    if (mServerConfig.mOut.queue(mRequest))
                    {
                        return cCheckForWork;
                    }
                    return cCheckForWork;
                default :
                    break;
            }
            return cProcessError;
        }
        
    }

    /**
     * 
     */
    public static class TaxServerTcp extends ServerBase
    {
        public TaxServerTcp(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
            mTakeTimeIn = true;
        }
    }

    /**
     * 
     */
    public static class TaxServerPool extends ServerBase
    {
        public TaxServerPool(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
            mTakeTimeOut = true;
        }
    }

    /**
     * 
     */
    public static class TaxCollector extends ServerBase
    {
        public TaxCollector(ServerConfig pServerConfig, int pId)
        {
            super(pServerConfig, pId);
            mTakeTimeOut = true;
        }
    }
}
