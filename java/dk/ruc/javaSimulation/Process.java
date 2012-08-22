/*
  File: Process.java

  Originally written by Keld Helsgaun and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.

  History:
  Date       Who                What
   5May2000  kh         Created public version
  18Feb2004  kh		    Coroutines are now recycled after use. This will often
                        reduce the demand for threads in multiple simulations.
*/

package dk.ruc.javaSimulation;
import java.util.*;

/**
* This class may be used for process-based discrete event simulation.
* <p>
* Processes are created as instances of <tt>Process</tt>-derived
* classes that override the abstract method <tt>actions</tt>. 
* The <tt>actions</tt> method is used to describe their life cycles.
* <p>
* Since <tt>Process</tt> is a subclass of <tt>Link</tt>, 
* every process has the capability of being a member of a two-way list.
* This is useful, for example, when processes must wait in a queue.
*   
* @see dk.ruc.javaSimulation.Link
* @see dk.ruc.javaSimulation.Head
*/

@SuppressWarnings("unchecked")
public abstract class Process extends Link {
    @SuppressWarnings("serial")
    private static class TerminateException extends RuntimeException {}
    private static Set processSet = Collections.synchronizedSet(new HashSet());
    private static boolean termination;
    
    /**
    * The life cycle of this process. 
    */
    protected abstract void actions();

    private final Coroutine myCoroutine = new Coroutine() {	   	
        protected void body() {
            if (MAIN == null)
                MAIN = Process.this;
            processSet.add(Process.this);
            try {
            	actions();
            } catch (TerminateException e) {}
            TERMINATED = true;
            processSet.remove(Process.this);
            if (Process.this == MAIN) {            
                termination = true; 
                while (SQS.SUC != SQS) 
                   SQS.SUC.cancel();           
                MAIN.scheduleAfter(SQS);  
                Object[] pa = processSet.toArray();
                processSet.clear();
                for (int i = 0; i < pa.length; i++) 
                    if (!((Process) pa[i]).TERMINATED)
                        resume(((Process) pa[i]).myCoroutine);
                MAIN.cancel();
                MAIN = null;
                terminated = true;
                termination = false;
             } else { 
            	if (SUC != null)
              	    cancel();
            	terminated = true;
            	resume(SQS.SUC.myCoroutine);
            }
        }
    };

    /**
    * The predecessor of this process in the event list.
    */	
    private Process PRED;    
    
    /**
    * The successor of this process in the event list.
    */	
    private Process SUC;
    
    /**
    * The event time of this process when scheduled.
    */
    private double EVTIME;
    
    /**
    * An indication of whether this process has executed its actions.
    * The value is made publically available through the method 
    * <tt>terminated</tt>.
    */
    private boolean TERMINATED;
    
    /**
    * The list head of the event list, a circular two-way list.
    */
    private final static Process SQS;

    static { 
        SQS = new Process() {
            protected void actions() {}
        };
        SQS.EVTIME = -1;  SQS.PRED = SQS.SUC = SQS;
    }
    
    /**
    * The main process.
    * This is the first activated process in a simulation.
    * The value is made publically available through the method 
    * <tt>main</tt>.
    */
    private static Process MAIN;

    /**
    * Tests if this process is scheduled.
    *
    * @return <tt>true</tt> if this process is not currently 
    * in the event list; <tt>false</tt> otherwise.
    */	
    public final boolean idle() { 
        return SUC == null; 
    }
    
    /**
    * Tests if this process is terminated. 
    *
    * @return <tt>true</tt> if this process has executed 
    * all its actions; <tt>false</tt> otherwise.
    */
    public final boolean terminated() { 
        return TERMINATED; 
    }
    
    /**
    * Returns the event time of this process.
    * 
    * @exception <tt>RuntimeException</tt>
    * if this process is idle.
    */
    public final double evTime() { 
        if (idle())
            error("No evTime for idle process");
        return EVTIME; 
    }
    
    /**
    * Returns the next process (if any) in the event list.
    */
    public final Process nextEv() { 
        return SUC == SQS ? null : SUC; 
    }

    /**
    * Returns the currently active process.
    */
    public static final Process current() { 
        return SQS.SUC != SQS ? SQS.SUC : null; 
    } 
    
    /**
    * Returns the current simulation time.
    */
    public static final double time() { 
        return SQS.SUC != SQS ? SQS.SUC.EVTIME : 0; 
    }
    
    /**
    * Returns the main process.
    * The main process is the first process activated 
    * in a simulation.
    */
    public static final Process main() { 
        return MAIN; 
    }
    
    /**
    * Throws a run-time exception with a specified error message.
    *
    * @exception <tt>RuntimeException</tt> 
    * always. 
    */
    private static void error(String msg) {
        throw new RuntimeException(msg);
    }
    
    /**
    * Suspends the currently active process for a specified 
    * period of simulated time.
    * <p>
    * The process is rescheduled for
    * reactivation at <tt>time()</tt> + <tt>t</tt>.
    *
    * @param <tt>t</tt> The length of the period of suspension.
    */
    public static final void hold(double t) {
        if (SQS.SUC == SQS)
           error("Hold: SQS is empty"); 
        Process Q = SQS.SUC;
        if (t > 0) 
            Q.EVTIME += t;
        t = Q.EVTIME;
        if (Q.SUC != SQS && Q.SUC.EVTIME <= t) {
            Q.cancel();
            Process P = SQS.PRED;
            while (P.EVTIME > t) 
                P = P.PRED;
            Q.scheduleAfter(P);
            resumeCurrent();
        }
    }
    
    /**
    * Passivates the currently active process.
    * <p> 
    * <tt>current</tt> is removed from the event list, and
    * the actions of the new <tt>current</tt> are resumed.
    *
    * @exception <tt>RuntimeException</tt>
    * if the event list becomes empty.
    */
    public static final void passivate() {
        if (SQS.SUC == SQS)
           error("Passivate: SQS is empty"); 
        Process CURRENT = SQS.SUC;
        CURRENT.cancel();
        if (SQS.SUC == SQS)
            error("passivate causes SQS to become empty");
        resumeCurrent();
    } 

    /**
    * Causes the currently active process to wait in a queue. 
    * <p>
    * The currently active process is added to the two-way list 
    * passed as the parameter, and <tt>passivate</tt> is called.
    *
    * @param <tt>q</tt> The head of the list.
    *
    * @exception <tt>RuntimeException</tt>
    * if the event list becomes empty.
    */		 
    public static final void wait(Head q) {
        Process CURRENT = SQS.SUC;
        if (CURRENT == SQS)
            error("Wait: SQS is empty"); 
        CURRENT.into(q);
        CURRENT.cancel();
        if (SQS.SUC == SQS)
            error("wait causes SQS to become empty");
        resumeCurrent();
    }

    /**
    * Cancels a scheduled event.
    * <p>
    * The process passed as the parameter is removed from
    * the event list and the actions of the new 
    * <tt>current</tt> are resumed.
    * If the process is currently active or suspended, 
    * it becomes passive.
    * If it is passive, terminated or <tt>null</tt>,
    * the call has no effect.
    *
    * @param <tt>p</tt> The process to be cancelled.
    *
    * @exception <tt>RuntimeException</tt>
    * if the event list becomes empty.
    */
    public static final void cancel(Process p) {
        if (p == null || p.SUC == null)
            return;
        Process CURRENT = SQS.SUC;
        p.cancel();
        if (SQS.SUC != CURRENT)
            return;
        if (SQS.SUC == SQS)
            error("cancel causes SQS to become empty");
       	resumeCurrent();
    }

    /**
    * Internal method used to activate or reactivate a process. 
    * The method is invoked by the <tt>activate</tt> and 
    * <tt>reactivate</tt> methods. 
    * 
    * @param reac <tt>true</tt> if the process, <tt>x</tt>, 
    * is to be reactivated; <tt>false</tt> otherwise.
    * @param <tt>x</tt> The process to be (re)activated. 
    * The first process in the activation call.
    * @param <tt>code</tt> A code defined from the activation call:
    *<pre>        <i>absent</i>       direct_code = 0
    *        at           at_code     = 1
    *        delay        delay_code  = 2
    *        before       before_code = 3
    *        after        after_code  = 4</pre>
    * @param <tt>t</tt> The time specified in the activation call, 
    * if present, otherwise it is zero.
    * @param <tt>y</tt> The second process in the activation call,
    * if present, otherwise it is <tt>null</tt>.
    * @param <tt>prio</tt> <tt>true</tt> if <tt>prior</tt> is 
    * specified in the activation call; <tt>false</tt> otherwise.
    *
    * @exception <tt>RuntimeException</tt>
    * if the event list becomes empty.      
    */	
    private static final void activat(boolean reac, Process x, int code,
                                      double t, Process y, boolean prio) {
        if (x == null || x.TERMINATED || (!reac && x.SUC != null))
            return;
        Process CURRENT = SQS.SUC, P = null;
        double NOW = time();
        switch(code) {
        case direct_code:
            if (x == CURRENT)
                return;
            t = NOW; P = SQS;
            break;
        case delay_code:
            t += NOW;
        case at_code:
            if (t <= NOW) {
                if (prio && x == CURRENT)
                    return;
                t = NOW;
            }
            break;
        case before_code:
        case after_code:
             if (y == null || y.SUC == null) {
                 if (x.SUC != null)
                     x.cancel();
                 if (SQS.SUC == SQS)
                     error("reactivate causes SQS to become empty");
                 return;
             }
             if (x == y)
                 return;
             t = y.EVTIME;
             P = code == before_code ? y.PRED : y;
        }
        if (x.SUC != null)
            x.cancel();
        if (P == null) {
            for (P = SQS.PRED; P.EVTIME > t; P = P.PRED)
                 ;
            if (prio)
                while (P.EVTIME == t)
                    P = P.PRED;
        }
        x.EVTIME = t;
        x.scheduleAfter(P);
        if (SQS.SUC != CURRENT)
            resumeCurrent();
    }
   
    /*
    * Auxiliary classes for representing activation keywords. 
    */
    
    private static final class At     {}
    private static final class Delay  {}
    private static final class Before {}
    private static final class After  {}
    private static final class Prior  {}
    
    /* Activation keywords */
     
    public static final At     at     = new At();
    public static final Delay  delay  = new Delay();
    public static final Before before = new Before();
    public static final After  after  = new After();
    public static final Prior  prior  = new Prior();
    
    /* Codes passed to the <tt>activat</tt> method */
    
    private static final int direct_code = 0;
    private static final int at_code     = 1;
    private static final int delay_code  = 2;
    private static final int before_code = 3;
    private static final int after_code  = 4;
    
    /**
    * Causes the specified passive process to become active 
    * at the current simulation time (after processes with the
    * same event time).
    * <b>The call has no effect unless the process is passive.</b>
    * <p>
    * The process is inserted into the event list at a
    * position corresponding to the current simulation time
    * and <b>after</b> any processes with the same event time.
    *
    * @param <tt>p</tt> The process to be activated.
    */
    public static final void activate(Process p) { 
        activat(false, p, direct_code, 0, null, false);  
    }
      
    /**
    * Causes the specified passive process to become active 
    * at the specified event time (after processes with the
    * same event time).
    * <b>The call has no effect unless the process is passive.</b>
    * <p>
    * The process is inserted into the event list at a
    * position corresponding to the current simulation time
    * and <b>after</b> any processes with the same event time.
    * 
    * @param <tt>p</tt> The process to be activated.
    * @param <tt>at</tt> The reference <tt>at</tt>.  
    * @param <tt>t</tt> The event time. 
    */
    public static final void activate(Process p, At at, double t) { 
        activat(false, p, at_code, t, null, false); 
    }
      
    /**
    * Causes the specified passive process to become active 
    * at the specified event time (before processes with the
    * same event time).
    * <b>The call has no effect unless the process is passive.</b>
    * <p>
    * The process is inserted into the event list at a
    * position corresponding to the current simulation time
    * and <b>before</b> any processes with the same event time.
    * 
    * @param <tt>p</tt> The process to be activated.
    * @param <tt>at</tt> The reference <tt>at</tt>.  
    * @param <tt>t</tt> The event time.
    * @param <tt>prior</tt> The constant <tt>prior</tt>.   
    */  
    public static final void activate(Process p, At at, double t, Prior prior) { 
        activat(false, p, at_code, t, null, prior != null); 
    }
      
    /**
    * Causes the specified passive process to become active 
    * after the specified delay (after processes with the
    * same event time). 
    * <b>The call has no effect unless the process is passive.</b>
    * <p>
    * The process is inserted into the event list at a position 
    * corresponding to the current simulation time plus the
    * the specified delay.
    * The process is inserted <b>after</b> any processes with the
    * same event time.
    * 
    * @param <tt>p</tt> The process to be activated.
    * @param <tt>delay</tt> The reference <tt>delay</tt>. 
    * @param <tt>t</tt> The delay.
    */
    public static final void activate(Process p, Delay delay, double t) { 
        activat(false, p, delay_code, t, null, false); 
    }
     
    /**
    * Causes the specified passive process to become active 
    * after the specified delay (before processes with the
    * same event time). 
    * <b>The call has no effect unless the process is passive.</b>
    * <p>
    * The process is inserted into the event list at a position 
    * corresponding to the current simulation time plus the
    * the specified delay.
    * The process is inserted <b>before</b> any processes with the
    * same event time.
    * 
    * @param <tt>p</tt> The process to be activated.
    * @param <tt>delay</tt> The reference <tt>delay</tt>.  
    * @param <tt>t</tt> The event time.
    * @param <tt>prior</tt> The reference <tt>prior</tt>. 
    */
    public static final void activate(Process p, Delay d, double t, Prior prior) { 
        activat(false, p, delay_code, t, null, prior != null); 
    }
      
    /**
    * Schedules the first (passive) process immediately before 
    * the second (scheduled) one, and at the same event time. 
    * <b>The call has no effect unless the first process is passive
    * and the second one is scheduled.</b>
    * <p>
    * The process <tt>p1</tt> is inserted into the event list 
    * immediately before the process <tt>p2</tt> and with 
    * the same event time.
    *
    * @param <tt>p1</tt> The process to be activated.
    * @param <tt>before</tt> The reference <tt>before</tt>.
    * @param <tt>p2</tt> The process before which <tt>p1</tt> 
    * is to be scheduled.   
    */
    public static final void activate(Process p1, Before before, Process p2) { 
        activat(false, p1, before_code, 0, p2, false); 
    }
        
    /**
    * Schedules the first (passive) process immediately after 
    * the second (scheduled) one, and at the same event time.
    * <b>The call has no effect unless the first process is passive
    * and the second one is scheduled.</b>
    * <p>
    * The process <tt>p1</tt> is inserted into the event list 
    * immediately after the process <tt>p2</tt> and with 
    * the same event time.
    *
    * @param <tt>p1</tt> The process to be activated.
    * @param <tt>before</tt> The constant <tt>after</tt>.
    * @param <tt>p2</tt> The process after which <tt>p1</tt> 
    * is to be scheduled. 
    */
    public static final void activate(Process p1, After after, Process p2) { 
        activat(false, p1, after_code, 0, p2, false); 
    }
    
    /**
    * Causes the specified process to become active 
    * at the current simulation time (after processes with the
    * same event time).
    * <p>
    * The process is positioned in the event list at a
    * position corresponding to the current simulation time
    * and <b>after</b> any processes with the same event time.
    *
    * @param <tt>p</tt> The process to be reactivated.
    */
    public static final void reactivate(Process p) { 
        activat(true, p, direct_code, 0, null, false);  
    }
      
    /**
    * Causes the specified process to become active 
    * at the specified event time (after processes with the
    * same event time).
    * <p>
    * The process is positioned in the event list at a
    * position corresponding to the current simulation time
    * and <b>after</b> any processes with the same event time.
    * 
    * @param <tt>p</tt> The process to be reactivated.
    * @param <tt>at</tt> The reference <tt>at</tt>.  
    * @param <tt>t</tt> The event time. 
    */
    public static final void reactivate(Process p, At at, double t) { 
        activat(true, p, at_code, t, null, false); 
    }
    
    /**
    * Causes the specified process to become active 
    * at the specified event time (before processes with the
    * same event time).
    * <p>
    * The process is positioned in the event list at a
    * position corresponding to the current simulation time
    * and <b>before</b> any processes with the same event time.
    * 
    * @param <tt>p</tt> The process to be reactivated.
    * @param <tt>at</tt> The reference <tt>at</tt>.  
    * @param <tt>t</tt> The event time.
    * @param <tt>prior</tt> The constant <tt>prior</tt>.   
    */   
    public static final void reactivate(Process p, At at, double t, Prior prior) { 
        activat(true, p, at_code, t, null, prior != null); 
    }    
    
    /**
    * Causes the specified process to become active 
    * after the specified delay (after processes with the
    * same event time).
    * <p>
    * The process is positioned in the event list at a
    * position corresponding to the current simulation time plus the
    * the specified delay.
    * The process is inserted <b>after</b> any processes with the
    * same event time.
    * 
    * @param <tt>p</tt> The process to be reactivated.
    * @param <tt>delay</tt> The reference <tt>delay</tt>. 
    * @param <tt>t</tt> The delay.
    */
    public static final void reactivate(Process p, Delay delay, double t) { 
        activat(true, p, delay_code, t, null, false); 
    }
    
    /**
    * Causes the specified process to become active 
    * after the specified delay (before processes with the
    * same event time).
    * <p>
    * The process is positioned in the event list at a
    * position corresponding to the current simulation time plus the
    * the specified delay.
    * The process is positioned in the event list <b>before</b> 
    * any processes with the same event time.
    * 
    * @param <tt>p</tt> The process to be reactivated.
    * @param <tt>delay</tt> The reference <tt>delay</tt>.  
    * @param <tt>t</tt> The event time.
    * @param <tt>prior</tt> The reference <tt>prior</tt>. 
    */
    public static final void reactivate(Process p, Delay d, double t, Prior prior) { 
        activat(true, p, delay_code, t, null, prior != null); 
    }
    
    /**
    * Schedules the first process immediately before 
    * the second one and at the same event time.
    * <p>
    * The process <tt>p1</tt> is positioned in the event list 
    * immediately before the process <tt>p2</tt> and with 
    * the same event time. 
    * If <tt>p1</tt> == <tt>p2</tt> or <tt>p2</tt> is not in the event list, 
    * the call is equivalent to <tt>cancel(p1)</tt>.
    *
    * @param <tt>p1</tt> The process to be reactivated.
    * @param <tt>before</tt> The reference <tt>before</tt>.
    * @param <tt>p2</tt> The process before which <tt>p1</tt> 
    * is to be scheduled.   
    */
    public static final void reactivate(Process p1, Before before, Process p2) { 
        activat(true, p1, before_code, 0, p2, false); 
    }
    
    /**
    * Schedules the first process immediately after 
    * the second one and at the same event time.
    * <p>
    * The process <tt>p1</tt> is positioned in the event list 
    * immediately after the process <tt>p2</tt> and with 
    * the same event time. 
    * If <tt>p1</tt> == <tt>p2</tt> or <tt>p2</tt> is not in the event list, 
    * the call is equivalent to <tt>cancel(p1)</tt>.
    *
    * @param <tt>p1</tt> The process to be reactivated.
    * @param <tt>before</tt> The reference <tt>after</tt>.
    * @param <tt>p2</tt> The process before which <tt>p1</tt> 
    * is to be scheduled.   
    */
    public static final void reactivate(Process p1, After after, Process p2) { 
        activat(true, p1, after_code, 0, p2, false); 
    }
    
    /**
    * Inserts this process after another process in the event list.
    *
    * @param <tt>p</tt> The process after which the process is to 
    * be inserted.
    */
    private final void scheduleAfter(Process p) {
        PRED = p; 
        SUC = p.SUC;
        p.SUC = SUC.PRED = this;
    }
    
    /**
    * Removes this process from the event list. 
    */
    private final void cancel() {
        PRED.SUC = SUC;
        SUC.PRED = PRED;
        PRED = SUC = null;
    }
    
    /**
    * Resumes the current process. 
    */
    private final static void resumeCurrent() {
    	Coroutine.resume(SQS.SUC.myCoroutine);
        if (termination) 
            throw new TerminateException();
    } 
}
