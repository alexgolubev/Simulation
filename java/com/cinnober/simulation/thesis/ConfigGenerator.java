package com.cinnober.simulation.thesis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author alexander.golubev, Cinnober Financial Technology
 */
public class ConfigGenerator {

    private static String cTaxAnalyzedLogPath;
    private static String cMeAnalyzedLogPath;
    private static String cMesAnalyzedLogPath;
    private static Map<String, String> cServiceTimes = new LinkedHashMap<String, String>();
    
    // Mapping contains simulation service station with the list of corresponding  stations in real system
    // That list is represented either by one station or by one beginning and one finishing station.
    private static Map<String, List<String>> cStationMappings = new HashMap<String, List<String>>();
    private static EntryParser cParser = new EntryParser();
    private static final String cConfigDir = System.getProperty("user.dir") + File.separator + "generatedConfigs";
    private static String cConfigName = "1400TPS.cfg";
    private static final String cNL = System.getProperty("line.separator");
    
    private static final String cPattern = "#,##0.0";
    private static DecimalFormat cLogDecimalFormat  = new DecimalFormat(cPattern);
    private static DecimalFormat cConfigDecimalFormat  = new DecimalFormat(cPattern,
        new DecimalFormatSymbols(Locale.US));

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] pArgs) throws Exception {
        
        init(pArgs);
        
        parseTrace("[TAX]", getOrderInsertTrace(cTaxAnalyzedLogPath));
        parseTrace("[ME]", getOrderInsertTrace(cMeAnalyzedLogPath));
        parseTrace("[MES]", getOrderInsertTrace(cMesAnalyzedLogPath));
        
        generateConfig();
    }

    private static void init(String[] pArgs) {

        try {
            parse(pArgs);
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
        
        addStationMapping(SimulationUtils.TAX_TCP, 
            "[TAX] XmapiReceiver_EmapiTagwir create Tap",
            "[TAX] XmapiReceiver_EmapiTagwir toTap [OrderInsertReq]");
        addStationMapping(SimulationUtils.TAX_SERVER_POOL, 
            "[TAX] EmapiTagwireConnector-req gatelet execute [pre]");
        addStationMapping(SimulationUtils.ME_NW, 
            "[TAX] EmapiTagwireConnector-req gatelet execute [post]");
        addStationMapping(SimulationUtils.ME, 
            "[ME] ServerPool SRI begin run()",
            "[ME] ResponseThreadPool SRI-callback: sent response");
        addStationMapping(SimulationUtils.ME_TCP,
            "[ME] ServerPool SRI begin run()");
        addStationMapping(SimulationUtils.ME_SERVER_POOL,
            "[ME] ServerPool SRI post sleep",
            "[ME] SorterBackend enter - executePreChain");
        addStationMapping(SimulationUtils.ME_SORTER,
            "[ME] SorterBackend post clone",
            "[ME] ChainUnit0 enter - executeChainUnit");
        addStationMapping(SimulationUtils.ME_CHAIN_UNIT0,
            "[ME] ChainUnit0 execute ChainUnit - 1",
            "[ME] PostChain enter - executePostChain");
        addStationMapping(SimulationUtils.MES_NW, 
            "[ME] ChainUnit0 execute ChainUnit - 1",
            "[ME] PostChain replicate done");
        addStationMapping(SimulationUtils.MES, 
            "[MES] ServerPool SRI begin run()",
            "[MES] ServerPool SRI-callback: write response to socket done");
        addStationMapping(SimulationUtils.MES_TCP,
            "[MES] ServerPool SRI begin run()");
        addStationMapping(SimulationUtils.MES_SERVER_POOL,
            "[MES] ServerPool SRI post sleep",
            "[MES] ServerPool SRI-callback: write response to socket done");
        addStationMapping(SimulationUtils.ME_RECOVERY, 
            "[ME] ChainUnit0 execute ChainUnit - 1",
            "[ME] PostChain recovery log done");
        addStationMapping(SimulationUtils.ME_POST_CHAIN, 
            "[ME] ResponseThreadPool SRI-callback: enter send response");
        addStationMapping(SimulationUtils.ME_RESPONSE_POOL, 
            "[ME] ResponseThreadPool SRI-callback: sending response",
            "[ME] ResponseThreadPool SRI-callback: sent response");
        addStationMapping(SimulationUtils.TAX_COLLECTOR,
            "[TAX] EmapiTagwireConnector-req toBytes [pre]",
            "[TAX] EmapiTagwireConnector-req message write to client done");
        
    }
    
    private static List<String> getOrderInsertTrace(String pFilePath) throws Exception {
        
        BufferedReader tReader = new BufferedReader(new FileReader(pFilePath));
        String tLine;
        boolean tFoundTrace = false;
        
        while ((tLine = tReader.readLine()) != null && !tFoundTrace) {
            
            // Checking if the line indicates trace start
            if (tLine.contains("No of occurrences")) {
                // The first 2 line contains non-used information.
                if (tReader.readLine() == null) {
                    break;
                } 
                
                List<String> tTrace = new ArrayList<String>();
            
                // Writing trace to tTrace and checking if it is actually OrderInsertReq trace 
                while ((tLine = tReader.readLine()) != null && tLine.length() != 0) {
                    
                    tTrace.add(tLine);
                    
                    if (tLine.contains("OrderInsertReq")) {
                        tFoundTrace = true;
                    }
                }
                
                if (tFoundTrace) {
                    
                    //Cut off the last line - it is not needed
                    tTrace.remove(tTrace.size() - 1);
                    return tTrace;
                }                
            } 
        }
        
        throw new Exception("No order insert trace found in " + pFilePath);
    }
    
    // Prefixes are needed since the ME and MES stations have the same names
    // Even stations within one trace can have the same name
    private static void parseTrace(String pPrefix, List<String> pTrace) throws ParseException {
    
        for (String tLine : pTrace) {
            
            cParser.parse(tLine);
            String tKey = cParser.getKey().trim(); // extra spaces at the end can happen
            String tStationName = pPrefix + " " + tKey;
            if (cServiceTimes.containsKey(tStationName)) {
                int i = 2;
                while (cServiceTimes.containsKey(tStationName + " " + i)) {
                    i++;
                }
                cServiceTimes.put(tStationName + " " + i, cParser.getValue());
            } else {
                cServiceTimes.put(tStationName, cParser.getValue());
            }
        }
    }

    private static void generateConfig() throws IOException {
        
        File tFile = new File(cConfigDir, cConfigName);
        if (!tFile.exists()) {
            BufferedWriter tWriter = new BufferedWriter(new FileWriter(tFile));
            
            addConfigLine(tWriter, SimulationUtils.CLIENT);
            addConfigLine(tWriter, SimulationUtils.CLIENT_TAX_NW);
            addConfigLine(tWriter, SimulationUtils.TAX_TCP);
            addConfigLine(tWriter, SimulationUtils.TAX_SERVER_POOL);
            addConfigLine(tWriter, SimulationUtils.TAX_ME_NW);
            addConfigLine(tWriter, SimulationUtils.ME_TCP);
            addConfigLine(tWriter, SimulationUtils.ME_SERVER_POOL);
            addConfigLine(tWriter, SimulationUtils.ME_SORTER);
            addConfigLine(tWriter, SimulationUtils.ME_CHAIN_UNIT0);
            addConfigLine(tWriter, SimulationUtils.ME_BATCH_SENDER);
            addConfigLine(tWriter, SimulationUtils.ME_MES_NW);
            addConfigLine(tWriter, SimulationUtils.MES_TCP);
            addConfigLine(tWriter, SimulationUtils.MES_SERVER_POOL);
            addConfigLine(tWriter, SimulationUtils.MES_ME_NW);
            addConfigLine(tWriter, SimulationUtils.ME_RECOVERY);
            addConfigLine(tWriter, SimulationUtils.ME_POST_CHAIN);
            addConfigLine(tWriter, SimulationUtils.ME_RESPONSE_POOL);
            addConfigLine(tWriter, SimulationUtils.ME_TAX_NW);
            addConfigLine(tWriter, SimulationUtils.TAX_COLLECTOR);
            addConfigLine(tWriter, SimulationUtils.TAX_CLIENT_NW);
            
            tWriter.close();
        } else {
            throw new IOException("File " + tFile + " already exists! It is better to specify some other name");
        }
        
    }

    private static void addConfigLine(BufferedWriter pWriter, String pStation) throws IOException {
        
        if (pStation.equals(SimulationUtils.TAX_ME_NW) || pStation.equals(SimulationUtils.ME_TAX_NW)) {
            pWriter.write(pStation + "=" + cConfigDecimalFormat.format(getTaxMeNwTime()) + cNL);
        } else if (pStation.equals(SimulationUtils.ME_MES_NW) || pStation.equals(SimulationUtils.MES_ME_NW)) {
            pWriter.write(pStation + "=" + cConfigDecimalFormat.format(getMeMesNwTime()) + cNL);
        } else {
            pWriter.write(pStation + "=" + cConfigDecimalFormat.format(getServiceTime(pStation)) + cNL);
        }
    }

    private static double getTaxMeNwTime() {
        
        double tMePlusNwTime = getServiceTime(SimulationUtils.ME_NW);
        double tPureMeTime = getServiceTime(SimulationUtils.ME);
        return ((tMePlusNwTime - tPureMeTime) / 2);
    }

    private static double getMeMesNwTime() {
        
        double tMesPlusNwTime = getServiceTime(SimulationUtils.MES_NW);
        double tPureMesTime = getServiceTime(SimulationUtils.MES);
        return ((tMesPlusNwTime - tPureMesTime) / 2);
    }

    private static double getServiceTime(String pStation) {
        
        double tTime = 0;
        List<String> tStationList = cStationMappings.get(pStation);
        if (tStationList != null) {
            
            String tFirstStation = tStationList.get(0);
            String tLastStation = tStationList.get(tStationList.size() - 1);
            
            boolean tFirstFound = false;
            for (String tRealStation : cServiceTimes.keySet()) {
                if (tFirstFound) {
                    tTime += getDoubleFromLogNumber(cServiceTimes.get(tRealStation));                   
                    if (tRealStation.equals(tLastStation)) {
                        break;
                    }         
                } else if (tRealStation.equals(tFirstStation)) {
                    tTime += getDoubleFromLogNumber(cServiceTimes.get(tRealStation));
                    
                    if (tFirstStation.equals(tLastStation)) {
                        break;
                    } else {
                        tFirstFound = true;
                    }
                }
            }
        }
        
        return tTime;
    }
    
    private static double getDoubleFromLogNumber(String pNumber) {
        try {
            return cLogDecimalFormat.parse(pNumber).doubleValue();
        }
        catch (ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }

    private static void addStationMapping(String pSimStation, String... pRealStations) {
        
        List<String> tList = new ArrayList<String>();
        for (String tRealStation : pRealStations) {
            tList.add(tRealStation);
        }
        cStationMappings.put(pSimStation, tList);
    }

    /**
     * First argument is a directory analyzed logs are in.
     * @param pArgs
     * @throws ParseException
     */
    private static void parse(String[] pArgs) throws ParseException {
        
        if (pArgs.length == 0 || pArgs[0] == null || pArgs[0].equals("")) {
            throw new ParseException("No directory is specified", 0);
        } else {
            cTaxAnalyzedLogPath = pArgs[0] + File.separator + "Analyzed_TAX.log";
            cMeAnalyzedLogPath = pArgs[0] + File.separator + "Analyzed_ME.log";
            cMesAnalyzedLogPath = pArgs[0] + File.separator + "Analyzed_MES.log";
        }
    }

    private static class EntryParser {

        private final Pattern mPattern = Pattern.compile(
            "\\t(?:(\\d{1,3}(?:[^\\t]\\d{3})*(?:,\\d)?)?\\t){2,3}(?:-?\\d{1,3}(?:[^\\t]\\d{3})*(?:,\\d)?\\t){8}(.+)");
        private String mKey;
        private String mValue;

        
        public EntryParser() {
            
        }
        
        /**
         * Parses the {@link FwTracker} line for the information provided by {@link #getKey()} and {@link #getValue()}.
         * @param pLine {@link FwTracker} line.
         */
        private void parse(String pLine) throws ParseException {
            Matcher tMatcher = mPattern.matcher(pLine);
            if (!tMatcher.matches()) {
                throw new RuntimeException("The regular expression seems to be incorrect"
                    + " - following line should have matched: \"" + pLine + "\"");
            }

            mValue = tMatcher.group(1);
            mKey = tMatcher.group(2);
        }

        /**
         * @return The time (in microseconds) spent is the {@link FwTracker} line.
         */
        private String getValue() {
            return mValue;
        }

        /**
         * @return The identifier for the {@link FwTracker} entry/line
         */
        private String getKey() {
            return mKey;
        }
    }
}
