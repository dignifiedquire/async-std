use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::iter;
use std::sync::atomic::{self, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, ThreadId};
use std::time::Duration;

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use kv_log_macro::trace;
use once_cell::unsync::OnceCell;

use crate::rt::Reactor;
use crate::sync::Spinlock;
use crate::task::Runnable;
use crate::utils::{abort_on_panic, random};

thread_local! {
    /// A reference to the current machine, if the current thread runs tasks.
    static MACHINE: OnceCell<Machine> = OnceCell::new();

    /// This flag is set to true whenever `task::yield_now()` is invoked.
    static YIELD_NOW: Cell<bool> = Cell::new(false);
}

struct Scheduler {
    /// Set to `true` while a machine is polling the reactor.
    polling: bool,

    /// Idle processors.
    processors: Vec<Processor>,
}

/// An async runtime.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<InnerRuntime>,
}

struct InnerRuntime {
    /// The reactor.
    reactor: Reactor,

    /// The global queue of tasks.
    injector: Injector<Runnable>,

    /// Handles to local queues for stealing work.
    stealers: Vec<Stealer<Runnable>>,

    /// The scheduler state.
    sched: Mutex<Scheduler>,

    /// Running machines.
    machines: Mutex<HashMap<ThreadId, Machine>>,

    need_machine: atomic::AtomicBool,
    num_spinning_threads: atomic::AtomicUsize,
}

impl Runtime {
    /// Creates a new runtime.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InnerRuntime::new()),
        }
    }

    /// Returns a reference to the reactor.
    pub fn reactor(&self) -> &Reactor {
        &self.inner.reactor
    }

    /// Flushes the task slot so that tasks get run more fairly.
    pub fn yield_now(&self) {
        YIELD_NOW.with(|flag| flag.set(true));
    }

    /// Schedules a task.
    pub fn schedule(&self, task: Runnable) {
        let last_thread = task.last_thread();
        trace!("runtime:schedule", { task_id: task.id().0 });
        // if we have a last_thread, try scheduling on that machine.
        if let Some(last_thread) = last_thread {
            // TODO: reduce locking
            let machines = self.inner.machines.lock().unwrap();
            if let Some(m) = machines.get(&last_thread) {
                let m = m.clone();
                drop(machines);

                m.schedule(&self, task);
                return;
            }
        } else {
            // only wake a spinning thread on the first spawn, which is indicated
            // by the fact that there is no thread_id
            self.wake_spinning_thread();
        }

        MACHINE.with(|machine| {
            // If the current thread is a worker thread, schedule it onto the current machine.
            // Otherwise, push it into the global task queue.
            match machine.get() {
                None => {
                    // self.wake_spinning_thread();
                    self.inner.injector.push(task);
                    self.notify();
                }
                Some(m) => m.schedule(&self, task),
            }
        });
    }
    /// Unparks a thread polling the reactor.
    fn notify(&self) {
        atomic::fence(Ordering::SeqCst);
        self.reactor().notify().unwrap();
    }

    /// Attempts to poll the reactor without blocking on it.
    ///
    /// Returns `Ok(true)` if at least one new task was woken.
    ///
    /// This function might not poll the reactor at all so do not rely on it doing anything. Only
    /// use for optimization.
    fn quick_poll(&self) -> io::Result<bool> {
        if let Ok(sched) = self.inner.sched.try_lock() {
            if !sched.polling {
                return self.reactor().poll(Some(Duration::from_secs(0)));
            }
        }
        Ok(false)
    }

    /// Runs the runtime on the current thread.
    pub fn run(&self) {
        let mut idle = 0;
        let mut delay = 0;

        loop {
            // Get a list of new machines to start, if any need to be started.
            for m in self.make_machines() {
                idle = 0;
                m.start(self.clone());
            }

            // Sleep for a bit longer if the scheduler state hasn't changed in a while.
            if idle > 10 {
                delay = (delay * 2).min(10_000);
            } else {
                idle += 1;
                delay = 1000;
            }

            thread::sleep(Duration::from_micros(delay));
        }
    }

    /// Returns the number of currently ideling threads.
    fn num_spinning_threads(&self) -> usize {
        self.inner.num_spinning_threads.load(Ordering::Relaxed)
    }

    /// Tries to wake an ideling thread, or creates a new one, if none is idle.
    fn wake_spinning_thread(&self) {
        let n = self.num_spinning_threads();
        trace!("runtime:wake_thread", { num_spinning_threads: n });
        if n > 1 {
            // nothing to do, we have at least one spinning thread.
            return;
        }

        self.inner
            .need_machine
            .compare_and_swap(false, true, Ordering::Relaxed);
        trace!("runtime:wake_thread:need_thread", {});
    }

    /// Returns a list of machines that need to be started.
    fn make_machines(&self) -> Vec<Machine> {
        let mut sched = self.inner.sched.lock().unwrap();
        let mut to_start = Vec::new();

        // If no machine has been polling the reactor in a while, that means the runtime is
        // overloaded with work and we need to start another machine.
        let num_spinning = self.num_spinning_threads();
        if (self.inner.need_machine.load(Ordering::Relaxed) && num_spinning < 2)
            || num_spinning == 0
        {
            if let Some(p) = sched.processors.pop() {
                let m = Machine::new(p);
                to_start.push(m.clone());
                self.inner.need_machine.store(false, Ordering::Relaxed);
            }
        }

        to_start
    }
}

impl InnerRuntime {
    fn new() -> Self {
        let cpus = num_cpus::get().max(1);
        let processors: Vec<_> = (0..cpus).map(|_| Processor::new()).collect();
        let stealers = processors.iter().map(|p| p.worker.stealer()).collect();

        Self {
            reactor: Reactor::new().unwrap(),
            injector: Injector::new(),
            stealers,
            sched: Mutex::new(Scheduler {
                processors,
                polling: false,
            }),
            machines: Mutex::new(HashMap::with_capacity(cpus)),
            need_machine: atomic::AtomicBool::new(false),
            num_spinning_threads: atomic::AtomicUsize::new(0),
        }
    }
}

/// A thread running a processor.
#[derive(Clone)]
struct Machine {
    inner: Arc<InnerMachine>,
}

struct InnerMachine {
    /// Holds the processor until it gets stolen.
    processor: Spinlock<Option<Processor>>,

    spinning: atomic::AtomicBool,
}

impl InnerMachine {
    fn new(p: Processor) -> Self {
        Self {
            processor: Spinlock::new(Some(p)),
            spinning: atomic::AtomicBool::new(true),
        }
    }
}
impl Machine {
    /// Creates a new machine running a processor.
    fn new(p: Processor) -> Self {
        Self {
            inner: Arc::new(InnerMachine::new(p)),
        }
    }

    fn start(&self, rt: Runtime) {
        let this = self.clone();

        thread::Builder::new()
            .name("async-std/machine".to_string())
            .spawn(move || {
                {
                    let mut machines = rt.inner.machines.lock().unwrap();
                    machines.insert(thread::current().id(), this.clone());
                }

                trace!("machine:spawn", {});
                abort_on_panic(|| {
                    let _ = MACHINE.with(|machine| machine.set(this.clone()));
                    this.run(&rt);
                    trace!("machine:stop", {});
                })
            })
            .expect("cannot start a machine thread");
    }

    /// Schedules a task onto the machine.
    fn schedule(&self, rt: &Runtime, task: Runnable) {
        match self.inner.processor.lock().as_mut() {
            None => {
                rt.inner.injector.push(task);
                rt.notify();
                // rt.wake_spinning_thread();
            }
            Some(p) => {
                p.schedule(rt, task);
                // we have a task for now, mark as not spinning
                if self
                    .inner
                    .spinning
                    .compare_and_swap(true, false, Ordering::Relaxed)
                {
                    rt.inner
                        .num_spinning_threads
                        .fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Finds the next runnable task.
    fn find_task(&self, rt: &Runtime) -> Steal<Runnable> {
        let mut retry = false;

        // First try finding a task in the local queue or in the global queue.
        if let Some(p) = self.inner.processor.lock().as_mut() {
            if let Some(task) = p.pop_task() {
                return Steal::Success(task);
            }

            match p.steal_from_global(rt) {
                Steal::Empty => {}
                Steal::Retry => retry = true,
                Steal::Success(task) => return Steal::Success(task),
            }
        }

        // Try polling the reactor, but don't block on it.
        let progress = rt.quick_poll().unwrap();

        // Try finding a task in the local queue, which might hold tasks woken by the reactor. If
        // the local queue is still empty, try stealing from other processors.
        if let Some(p) = self.inner.processor.lock().as_mut() {
            if progress {
                if let Some(task) = p.pop_task() {
                    return Steal::Success(task);
                }
            }

            match p.steal_from_others(rt) {
                Steal::Empty => {}
                Steal::Retry => retry = true,
                Steal::Success(task) => return Steal::Success(task),
            }
        }

        if retry { Steal::Retry } else { Steal::Empty }
    }

    /// Runs the machine on the current thread.
    fn run(&self, rt: &Runtime) {
        /// Number of yields when no runnable task is found.
        const YIELDS: u32 = 3;
        /// Number of short sleeps when no runnable task in found.
        const SLEEPS: u32 = 10;
        /// Number of runs in a row before the global queue is inspected.
        const RUNS: u32 = 64;

        // The number of times the thread found work in a row.
        let mut runs = 0;
        // The number of times the thread didn't find work in a row.
        let mut fails = 0;

        let thread_id = thread::current().id();

        // start in spinning mode
        assert!(self.inner.spinning.load(Ordering::Relaxed));
        rt.inner
            .num_spinning_threads
            .fetch_add(1, Ordering::Relaxed);

        loop {
            // Check if `task::yield_now()` was invoked and flush the slot if so.
            YIELD_NOW.with(|flag| {
                if flag.replace(false) {
                    if let Some(p) = self.inner.processor.lock().as_mut() {
                        p.flush_slot(rt);
                    }
                }
            });

            // After a number of runs in a row, do some work to ensure no task is left behind
            // indefinitely. Poll the reactor, steal tasks from the global queue, and flush the
            // task slot.
            if runs >= RUNS {
                runs = 0;
                rt.quick_poll().unwrap();

                if let Some(p) = self.inner.processor.lock().as_mut() {
                    if let Steal::Success(task) = p.steal_from_global(rt) {
                        p.schedule(rt, task);
                    }

                    p.flush_slot(rt);
                }
            }

            // Try to find a runnable task.
            if let Steal::Success(task) = self.find_task(rt) {
                task.run();
                runs += 1;
                fails = 0;
                continue;
            }

            fails += 1;

            // Yield the current thread a few times.
            if fails <= YIELDS {
                thread::yield_now();
                continue;
            }

            // Put the current thread to sleep a few times.
            if fails <= YIELDS + SLEEPS {
                let opt_p = self.inner.processor.lock().take();
                thread::sleep(Duration::from_micros(10));
                *self.inner.processor.lock() = opt_p;
                continue;
            }

            let mut sched = rt.inner.sched.lock().unwrap();

            // One final check for available tasks while the scheduler is locked.

            if let Some(task) = iter::repeat_with(|| self.find_task(rt))
                .find(|s| !s.is_retry())
                .and_then(|s| s.success())
            {
                self.schedule(rt, task);
                continue;
            }

            // If another thread is already blocked on the reactor, there is no point in keeping
            // the current thread around since there is too little work to do.
            if sched.polling {
                drop(sched);
                // trace!("machine:spin:start", {});
                // runs = 0;
                // fails = 0;

                // // mark as spinning
                // let r = rt.get_spinner();
                // spinner = Some(r);
                continue;
            }

            if self
                .inner
                .spinning
                .compare_and_swap(true, false, Ordering::Relaxed)
            {
                rt.inner
                    .num_spinning_threads
                    .fetch_sub(1, Ordering::Relaxed);
            }

            // Take out the machine associated with the current thread.
            // let mut machines = rt.machines.lock().unwrap();
            // let m = match machines.remove(&thread_id) {
            //     None => break, // The processor was stolen.
            //     Some(m) => m,
            // };
            // drop(machines);

            // Unlock the schedule poll the reactor until new I/O events arrive.
            sched.polling = true;
            drop(sched);
            rt.inner.reactor.poll(None).unwrap();

            // Lock the scheduler again and re-register the machine.
            sched = rt.inner.sched.lock().unwrap();
            sched.polling = false;
            // {
            //     rt.machines.lock().unwrap().insert(thread_id, m);
            // }

            if !self
                .inner
                .spinning
                .compare_and_swap(false, true, Ordering::Relaxed)
            {
                rt.inner
                    .num_spinning_threads
                    .fetch_add(1, Ordering::Relaxed);
            }

            runs = 0;
            fails = 0;
        }

        // When shutting down the thread, take the processor out if still available.
        let opt_p = self.inner.processor.lock().take();

        // Return the processor to the scheduler and remove the machine.
        if let Some(p) = opt_p {
            let mut sched = rt.inner.sched.lock().unwrap();
            sched.processors.push(p);
            rt.inner.machines.lock().unwrap().remove(&thread_id);
        }
    }
}

struct Processor {
    /// The local task queue.
    worker: Worker<Runnable>,

    /// Contains the next task to run as an optimization that skips the queue.
    slot: Option<Runnable>,
}

impl Processor {
    /// Creates a new processor.
    fn new() -> Processor {
        Processor {
            worker: Worker::new_fifo(),
            slot: None,
        }
    }

    /// Schedules a task to run on this processor.
    fn schedule(&mut self, rt: &Runtime, task: Runnable) {
        // rt.wake_spinning_thread();

        match self.slot.replace(task) {
            None => {}
            Some(task) => {
                self.worker.push(task);
                rt.notify();
            }
        }
    }

    /// Flushes a task from the slot into the local queue.
    fn flush_slot(&mut self, rt: &Runtime) {
        if let Some(task) = self.slot.take() {
            self.worker.push(task);
            rt.notify();
        }
    }

    /// Pops a task from this processor.
    fn pop_task(&mut self) -> Option<Runnable> {
        self.slot.take().or_else(|| self.worker.pop())
    }

    /// Steals a task from the global queue.
    fn steal_from_global(&self, rt: &Runtime) -> Steal<Runnable> {
        let res = rt.inner.injector.steal_batch_and_pop(&self.worker);
        if res.is_success() {
            trace!("processor:stole_from_global", {});
        }
        res
    }

    /// Steals a task from other processors.
    fn steal_from_others(&self, rt: &Runtime) -> Steal<Runnable> {
        // Pick a random starting point in the list of queues.
        let len = rt.inner.stealers.len();
        let start = random(len as u32) as usize;

        // Create an iterator over stealers that starts from the chosen point.
        let (l, r) = rt.inner.stealers.split_at(start);
        let stealers = r.iter().chain(l.iter());

        // Try stealing a batch of tasks from each queue.
        stealers
            .map(|s| {
                let res = s.steal_batch_and_pop(&self.worker);
                if let Steal::Success(ref t) = res {
                    trace!("processor:stole_from_other", {
                        task_id: t.id().0,
                    });
                }
                res
            })
            .collect()
    }
}
