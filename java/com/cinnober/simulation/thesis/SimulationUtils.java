package com.cinnober.simulation.thesis;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.Properties;

import dk.ruc.javaSimulation.Head;
import dk.ruc.javaSimulation.Link;

/**
 * The utility methods for sunning event driven simulations.
 *
 * @author alexander.golubev, Cinnober Financial Technology
 */
public class SimulationUtils {

    public static final String CLIENT = "Client";
    public static final String CLIENT_TAX_NW = "ClientTaxNwDelay";
    public static final String TAX_TCP = "TaxServer_Tcp";
    public static final String TAX_SERVER_POOL = "TaxServer_ServerPool";
    public static final String ME_NW = "MatchingEnginePlusNetworkTime";
    public static final String TAX_ME_NW = "TaxMeNwDelay";
    public static final String ME = "MatchingEngineTime";
    public static final String ME_TCP = "MatchingEngineTcp";
    public static final String ME_SERVER_POOL = "MatchingEngine_ServerPool";
    public static final String ME_SORTER = "MatchingEngine_Sorter";
    public static final String ME_CHAIN_UNIT0 = "MatchingEngine_ChainUnit0";
    public static final String ME_BATCH_SENDER = "MatchingEngine_BatchSender";
    public static final String MES_NW = "MatchingEngineStandbyPlusNetworkTime";
    public static final String ME_MES_NW = "MeMesNwDelay";
    public static final String MES = "MatchingEngineStandbyTime";
    public static final String MES_TCP = "MatchingEngineStandby_Tcp";
    public static final String MES_SERVER_POOL = "MatchingEngineStandby_ServerPool";
    public static final String MES_ME_NW = "MesMeNwDelay";
    public static final String ME_RECOVERY = "MatchingEngine_RecoveryLog";
    public static final String ME_POST_CHAIN = "MatchingEngine_PostChain";
    public static final String ME_RESPONSE_POOL = "MatchingEngine_ResponsePool";
    public static final String ME_TAX_NW = "MeTaxNwDelay";
    public static final String TAX_COLLECTOR = "TaxServer_Collector";
    public static final String TAX_CLIENT_NW = "TaxClientNwDelay";
    
    public Properties loadConfiguration(Properties pIncludeTo, String pIncludeThis) {
        InputStream tStream = ClassLoader.getSystemResourceAsStream(pIncludeThis.trim());
        if (tStream == null) {
            System.err.println("Couldn't find configuration named " + pIncludeThis);            
            return pIncludeTo;
        }
        Properties tConfig = new Properties();
        try {
            tConfig.load(tStream);
        }
        catch (IOException e) {
            System.err.println("Couldn't load configuration named " + pIncludeThis);
            return null;
        }
        pIncludeTo.putAll(tConfig);
        String tNewToInclude = pIncludeTo.getProperty("include");
        if (tNewToInclude != null) {
            pIncludeTo.remove("include");
            String[] tIncludeThis = tNewToInclude.split(",");
            for (String tI : tIncludeThis) {
                pIncludeTo = loadConfiguration(pIncludeTo, tI);
            }
        }
        return pIncludeTo;        
    }
    
    public <T> T[] headToArray(Head pHead, T[] pArray) {
        
        return headToArray(pHead, pArray, pHead.cardinal());
    }

    @SuppressWarnings("unchecked")
    public <T> T[] headToArray(Head pHead, T[] pArray, int pSize) {
        T[] tArray = (T[]) Array.newInstance(pArray.getClass().getComponentType(), pSize);
        Link tLink = (Link) pHead.first();
        for (int i = 0; i < pSize; ++i)
        {
            tArray[i] = (T) tLink;
            tLink = (Link) tLink.suc();
        }
        return tArray;
    }
}
