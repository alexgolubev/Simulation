 
package com.cinnober.simulation.old;

import dk.ruc.javaSimulation.*;
import dk.ruc.javaSimulation.Process;

/*
* Simulation of a queuing system.
*
* This version models is based on passive customers 
* and active servers.
*/

public class Simulation4 extends Process {
    int mTaxes = 2;
    int mMes = 2;
    Random random = new Random(7);

    Head mTaxInServers = new Head();
    Head mMeInServers = new Head();
    Head mMeOutServers = new Head();
    Head mTaxOutServers = new Head();
    
    Head mClientTaxRequestQueue = new Head();
    Head mTaxMeRequestQueue = new Head();
    Head mMeTaxRequestQueue = new Head();
    Head mTaxClientRequestQueue = new Head();

    int busyServers;
    double throughTime;
    int customers;
    long startTime = System.currentTimeMillis();
  
    public void actions() {
        for (int i = 1; i <= mTaxes; i++)  {
            new TaxInServer().into(mTaxInServers);
            new TaxOutServer().into(mTaxOutServers);
        }
        for (int i = 1; i <= mMes; i++) { 
            new MeInServer().into(mMeInServers);
//            new MeOutServer().into(mMeOutServers);
        }
        activate(new ClientRequestGenerator());
        hold(600);
        System.out.println("Customers = " + customers);
        System.out.println("Av.elapsed time = " + 
                           throughTime/customers);
        System.out.println("\nExecution time: " +
                            ((System.currentTimeMillis() 
                             - startTime)/1000.0) +            
                           " secs.\n");
    }

    class ClientRequestGenerator extends Process {
        public void actions() {
            while (true) {
                new Request().into(mClientTaxRequestQueue);
                if (!mTaxInServers.empty()) 
                    activate((TaxInServer) mTaxInServers.first());
                hold(random.uniform(1, 3));
            }
        }
    }

    static int cRequests = 0;
    class Request extends Link {
        int mRequest = ++cRequests;
        double mCreateTime = time();
        double mTaxInTime;
        double mMeInTime;
        double mMeOutTime;
        double mTaxOutTime;
    }

    class Service extends Process {
        String mName;
        Head mServerQ;
        Head mInQ;
        Head mOutQ;
        double mServiceTime;
        
        public Service(String pName, double pServiceTime, Head pServerQ, Head pInQ, Head pOutQ)
        {
            super();
            mServiceTime = pServiceTime;
            mName = pName;
            mServerQ = pServerQ;
            mInQ = pInQ;
            mOutQ = pOutQ;
        }

        public void actions() {
            while (true) {
                out();
                while (!mInQ.empty()) {
                    Request served = (Request) mInQ.first();
                    served.out();
                    hold(random.negexp(mServiceTime));
                    served.into(mOutQ);
                    if (!mServerQ.empty()) 
                        activate((Service) mServerQ.first());
                }
                wait(mInQ);
            }
        } 
    }

    class TaxInServer extends Process {
        public void actions() {
            while (true) {
                out();
                while (!mClientTaxRequestQueue.empty()) {
                    Request served = (Request) mClientTaxRequestQueue.first();
                    served.out();
                    served.mTaxInTime = time();
                    System.out.println("TaxInServer " + served.mRequest + " i " + served.mTaxInTime);
                    hold(random.normal(4, 1));
                    System.out.println("TaxInServer " + served.mRequest + " o " + time());
                    served.into(mTaxMeRequestQueue);
                    if (!mMeInServers.empty()) 
                        activate((MeInServer) mMeInServers.first());
                    customers++;
                    // Here we should really wait for this individual reply.
                    // How do I wait for this.
                    // Here we instead have queues with one server.
                }
                wait(mTaxInServers);
            }
        } 
    }

    class MeInServer extends Process {
        public void actions() {
            while (true) {
                out();
                while (!mTaxMeRequestQueue.empty()) {
                    Request served = (Request) mTaxMeRequestQueue.first();
                    served.out();
                    served.mMeInTime = time();
                    System.out.println("MeInServer " + served.mRequest + " i " + served.mMeInTime);
                    hold(random.normal(4, 1));
                    System.out.println("MeInServer " + served.mRequest + " o " + time());
                    served.into(mMeTaxRequestQueue);
                    if (!mTaxOutServers.empty()) 
                        activate((TaxOutServer) mTaxOutServers.first());
                    customers++;
                }
                wait(mMeInServers);
            }
        } 
    }
    
    class TaxOutServer extends Process {
        public void actions() {
            while (true) {
                out();
                while (!mMeTaxRequestQueue.empty()) {
                    Request served = (Request) mMeTaxRequestQueue.first();
                    served.out();
                    hold(random.normal(4, 1));
                    served.mTaxOutTime = time();
                    System.out.println("TaxOutputServer " + served.mRequest + " i " + served.mTaxOutTime);
//                    served.into(mTaxMeRequestQueue);
//                    if (!mTaxOutServers.empty()) 
//                        activate((TaxInServer) mTaxOutServers.first());
                    throughTime += served.mTaxOutTime - served.mTaxInTime; 
                    customers++;
                }
                wait(mTaxOutServers);
            }
        } 
    }

    
    public static void main(String args[]) {
        activate(new Simulation4());
    } 
}