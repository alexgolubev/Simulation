package com.cinnober.simulation.old;

import dk.ruc.javaSimulation.*;
import dk.ruc.javaSimulation.Process;

/*
* Simulation of a queuing system.
*
* This version models is based on passive customers 
* and active servers.
*/

public class Simulation3 extends Process {
    int servers = 2;
    Random random = new Random(7);

    Head customerQueue = new Head();
    Head serverQueue = new Head();
    int busyServers;
    double throughTime;
    int customers;
    long startTime = System.currentTimeMillis();
  
    public void actions() {
        for (int i = 1; i <= servers; i++) 
            new Server().into(serverQueue);
        activate(new CustomerGenerator());
        hold(600);
        System.out.println("Customers = " + customers);
        System.out.println("Av.elapsed time = " + 
                           throughTime/customers);
        System.out.println("\nExecution time: " +
                            ((System.currentTimeMillis() 
                             - startTime)/1000.0) +			   
                           " secs.\n");
    }

    class CustomerGenerator extends Process {
        public void actions() {
            while (true) {
                new Customer().into(customerQueue);
                if (!serverQueue.empty()) 
	          activate((Server) serverQueue.first());
                hold(random.uniform(1, 3));
            }
        }
    }

    class Customer extends Link {
        double arrivalTime = time();
    }

    class Server extends Process {
        public void actions() {
            while (true) {
                out();
                while (!customerQueue.empty()) {
                    Customer served = (Customer) customerQueue.first();
                    served.out();
                    hold(random.normal(4, 1));
                    customers++;
                    throughTime += time() - served.arrivalTime;
                }
                wait(serverQueue);
            }
        } 
    }

    public static void main(String args[]) {
        activate(new Simulation3());
    } 
}