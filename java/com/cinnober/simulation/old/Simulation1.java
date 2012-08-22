package com.cinnober.simulation.old;

import dk.ruc.javaSimulation.*;
import dk.ruc.javaSimulation.Process;

/*
* Simulation of a queuing system.
*
* This version models is based on active customers 
* and active servers (processes).
*/

public class Simulation1 extends Process {
    int servers = 10;
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
        hold(60000);
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
            into(customerQueue);
            if (!serverQueue.empty()) 
                activate((Process) serverQueue.first());
            passivate();
            customers++;
            throughTime += time() - arrivalTime;
        }     
    }

    class Server extends Process {
        public void actions() {
            while (true) {
                out();
                while (!customerQueue.empty()) {
                    Customer served = (Customer) customerQueue.first();
                    served.out();
                    hold(random.normal(4, 1));
                    activate(served);
                }
                wait(serverQueue);
            }
        } 
    }

    public static void main(String args[]) {
        activate(new Simulation1());
    } 
}