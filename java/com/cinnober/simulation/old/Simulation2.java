package com.cinnober.simulation.old;

import dk.ruc.javaSimulation.*;
import dk.ruc.javaSimulation.Process;

/*
* Simulation of a queuing system.
*
* This version models is based on active customers 
* (processes) and passive servers.
*/

public class Simulation2 extends Process {
    int servers = 2;
    Random random = new Random(7);

    Head customerQueue = new Head();
    Head serverQueue = new Head();
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
                activate(new Customer());
                hold(random.uniform(1, 3));
            }
        }
    }

    class Customer extends Process {
        public void actions() {
            double arrivalTime = time();
            if (serverQueue.empty()) {
                wait(customerQueue);
                out();
            }
            Server server = (Server) serverQueue.first();
            server.out();
            hold(random.normal(4, 1));
            server.into(serverQueue);
            if (!customerQueue.empty()) 
                activate((Customer) customerQueue.first());
            customers++;
            throughTime += time() - arrivalTime;
        }     
    }

    class Server extends Link {}

    public static void main(String args[]) {
        activate(new Simulation2());
    } 
}