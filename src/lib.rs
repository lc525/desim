/* Copyright © 2018 Gianmarco Garrisi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

//! This crate implement a discrete event simulation framework
//! inspired by the SimPy library for Python. It uses the generator
//! feature that is nightly. Once the feature is stabilized, also this
//! crate will use stable. Generators will be the only nightly feature
//! used in this crate.
//!
//! # Simulation
//! A simulation is performed scheduling one or more processes that
//! models the environment you are going to simulate. Your model may
//! consider some kind of finite resource that must be shared among
//! the processes, e.g. a bunch of servers in a simulation on queues.
//!
//! After setting up the simulation, it can be run step-by-step, using
//! the `step()` method, or all at once, with `run()`, until and ending
//! condition is met.
//!
//! The simulation will generate a log of all the events.
//!
/*
//! `nonblocking_run` lets you run the simulation in another thread
//! so that your program can go on without waiting for the simulation
//! to finish.
//!
*/
//! # Process
//! A process is implemented using the rust generators syntax.
//! This let us avoid the overhead of spawning a new thread for each
//! process, while still keeping the use of this framework quite simple.
//!
//! When a new process is created in the simulation, an identifier, of type
//! `ProcessId` is assigned to it. That id can be used to schedule an event that
//! resume the process.
//!
//! A process can be stopped and resumed later on. To stop the process, the
//! generator yields an `Effect` that specify what the simulator should do.
//! For example, a generator can set a timeout after witch it is executed again.
//! The process may also return. In that case it can not be resumed anymore.
//!
//!
//! # Resource
//! A resource is a finite amount of entities that can be used by one process
//! a time. When all the instances of the resource of interest are being used by
//! a process, the requiring one is enqueued in a FIFO and is resumed when the
//! resource become available again. When the process does not need the resource
//! anymore, it must release it.
//!
//! A resource can be created in the simulation using the `create_resource`
//! method, which requires the amount of resource and returns an identifier
//! for that resource that can be used to require and release it.
//!
//! A resource can be required and reelased by a process yielding
//! the corresponding `Effect`. There is no check on the fact that a process
//! yielding `Release` was holding a resource with that ID, but if a resource
//! gets more release then requests, the simulation will panic.
//!

#![feature(generators, generator_trait)]
use std::ops::{Generator, GeneratorState};
use std::collections::{BinaryHeap, VecDeque};
use std::cmp::{Ordering, Reverse};
use std::pin::Pin;

pub trait SimStep {
    fn get_effect(&self) -> Effect;
    fn should_log(&self) -> bool;
    fn trigger_event_cb(&self) -> bool;
    /* fn from_sim_event(SimEvent) -> SimStep; */
}

pub trait SimResource {
    type SimResourceId;
    type SimProcessId;
    type SimState: SimStep;

    fn get_id(&self) -> Self::SimResourceId;
    fn set_id(&mut self, Self::SimResourceId);

    fn get_available(&self) -> usize;
    fn get_allocated(&self) -> usize;

    fn set_acquire_cb(&mut self,
                      fn(rId: Self::SimResourceId, req_state: Self::SimState)
                         -> Self::SimState);
    fn set_enqueue_cb(&mut self,
                      fn(rId: Self::SimResourceId, req_state: Self::SimState)
                         -> Self::SimState);
    fn set_release_cb(&mut self,
                      fn(rId: Self::SimResourceId, req_state: Self::SimState)
                         -> Self::SimState);

    fn request(&mut self, Self::SimProcessId, Self::SimState) -> (bool, Self::SimState);
    fn release_and_get_next(&mut self, Self::SimProcessId, Self::SimState)
        -> (Self::SimState, Option<(Self::SimProcessId, Self::SimState)>);
}

/// The effect is yelded by a process generator to
/// interact with the simulation environment.
#[derive(Debug, Copy, Clone)]
pub enum Effect<'t> {
    /// The process that yields this effect will be resumed
    /// after the specified time
    TimeOut(f64),
    /// Similar to TimeOut, but specific to the service time
    /// using a particular list of resources. If the process does not
    /// hold the required resources, they are acquired _in order_
    ServiceTimeOut(f64, Option<&'t Vec<ResourceId>>),
    /// Yielding this effect it is possible to schedule the specified event
    Schedule(Event),
    /// This effect is yielded to request a resource
    Request(ResourceId),
    /// This effect is yielded to release a resource that is not needed anymore.
    Release(ResourceId),
    /// Keep the process' state until it is resumed by another event.
    Wait,
    Trace,
}

#[derive(PartialEq)]
pub enum ProcState {
    Created,
    Running,
    Waiting, // waiting for resource or event
    Ready,   // waiting for sched
    Terminated,  // no more sched & resumes possible
}

pub enum ResType {
    
}

pub enum Scheduler {
    Fifo,
    TimeShare,
}

/// Identifies a process. Can be used to resume it from another one and to schedule it.
pub type ProcessId = usize;
/// Identifies a resource. Can be used to request and release it.
pub type ResourceId = usize;

#[derive(Debug)]
struct Resource<T: SimStep + Clone> {
    id: ResourceId,
    allocated: usize,
    available: usize,
    queue: VecDeque<(ProcessId, T)>,
    acquire_cb: Option<fn(rId: ResourceId, req_state: T) -> T>,
    enqueue_cb: Option<fn(rId: ResourceId, req_state: T) -> T>,
    release_cb: Option<fn(rId: ResourceId, req_state: T) -> T>,
}

struct Process<T: SimStep + Clone> {
    state: ProcState,
    code: Box<dyn Generator<Yield = T, Return = ()> + Unpin>,
    /* res_inuse: Vec<ResourceId>,
     * res_inqueue: Vec<ResourceId> */
}

/* struct ProcessGroupEvents {
 *     sched_time: f64,
 *     sched: Scheduler,
 *     sched_events: BinaryHeap<Reverse<Event>>
 * } */

/// This struct provides the methods to create and run the simulation
/// in a single thread.
///
/// It provides methods to create processes and finite resources that
/// must be shared among them.
///
/// See the crate-level documentation for more information about how the
/// simulation framework works
pub struct Simulation<T : SimStep + Clone> {
    time: f64,
    steps: usize,
    /* cpus: Resource<T>, */
    processes: Vec<Option<Process< T>>>,
    process_free_list: Vec<ProcessId>,
    future_events: BinaryHeap<Reverse<Event>>,
    processed_steps: Vec<(Event, T)>,
    resources: Vec<Resource<T>>,
    res_acquire_cb: Option<fn(rId: ResourceId, req_state: T) -> T>,
    res_enqueue_cb: Option<fn(rId: ResourceId, req_state: T) -> T>,
    res_release_cb: Option<fn(rId: ResourceId, req_state: T) -> T>,
    event_processed_cb: Option<fn(event: Event, sim_state: T) -> bool>,
}

/*
pub struct ParallelSimulation {
    processes: Vec<Box<Generator<Yield = Effect, Return = ()>>>
}
 */

/// An event that can be scheduled by a process, yelding the `Event` `Effect`
/// or by the owner of a `Simulation` through the `schedule` method
#[derive(Debug, Copy, Clone)]
pub struct Event {
    /// Time interval between the current simulation time and the event schedule
    pub time: f64,
    /// Process to execute when the event occur
    pub process: ProcessId,
}

/// Specify which condition must be met for the simulation to stop.
pub enum EndCondition {
    /// Run the simulation until a certain point in time is reached.
    Time(f64),
    /// Run the simulation until there are no more events scheduled.
    NoEvents,
    /// Execute exactly N steps of the simulation.
    NSteps(usize),
}

impl<T : SimStep + Clone> Simulation<T> {
    /// Create a new `Simulation` environment.
    pub fn new() -> Simulation<T> {
        Simulation::<T>::default()
    }

    /// Returns the current simulation time
    pub fn time(&self) -> f64 {
        self.time
    }

    /// Returns the log of processed events
    pub fn processed_steps(&self) -> &[(Event, T)] {
        self.processed_steps.as_slice()
    }

    pub fn on_resource_acquire(&mut self, cb: fn(rId: ResourceId, req_state: T) -> T) {
        self.res_acquire_cb = Some(cb);
    }
    pub fn on_resource_enqueue(&mut self, cb: fn(rId: ResourceId, req_state: T) -> T) {
        self.res_enqueue_cb = Some(cb);
    }
    pub fn on_resource_release(&mut self, cb: fn(rId: ResourceId, req_state: T) -> T) {
        self.res_release_cb = Some(cb);
    }
    pub fn on_event_processed(&mut self, cb: fn(event: Event, sim_state: T) -> bool) {
        self.event_processed_cb = Some(cb);
    }

    fn event_and_log(&mut self, event: &Event, sim_state: T) {
        let mut add_to_log = true;
        if let Some(on_event) = self.event_processed_cb {
            if sim_state.trigger_event_cb() {
                add_to_log = on_event(*event, sim_state.clone());
            }
        }
        if add_to_log && sim_state.should_log() {
            self.processed_steps.push((*event, sim_state));
        }
    }


    /// Create a process.
    ///
    /// For more information about a process, see the crate level documentation
    ///
    /// Returns the identifier of the process.
    pub fn create_process(
        &mut self,
        process_code: Box<dyn Generator<Yield = T, Return = ()> + Unpin>,
    ) -> ProcessId {
        let p = Process {
            state: ProcState::Created,
            code: process_code,
            /* res_inuse: vec!(),
             * res_inqueue: vec!(), */
        };
        if let Some(pid) = self.process_free_list.pop() {
            //reuse completed process id
            self.processes[pid] = Some(p);
            pid
        } else {
            let id = self.processes.len();
            self.processes.push(Some(p));
            id
        }
    }

    /// Create a new finite resource, of which n instancies are available.
    ///
    /// For more information about a resource, see the crate level documentation
    ///
    /// Returns the identifier of the resource
    pub fn create_resource_no_cb(&mut self, n: usize) -> ResourceId {

        let id = self.resources.len();
        self.resources.push(Resource {
            id: id,
            allocated: n,
            available: n,
            queue: VecDeque::new(),
            acquire_cb: None,
            enqueue_cb: None,
            release_cb: None,
        });
        id
    }
    pub fn create_resource(&mut self, n: usize) -> ResourceId {

        let id = self.resources.len();
        self.resources.push(Resource {
            id: id,
            allocated: n,
            available: n,
            queue: VecDeque::new(),
            acquire_cb: self.res_acquire_cb,
            enqueue_cb: self.res_enqueue_cb,
            release_cb: self.res_release_cb,
        });
        id
    }
    pub fn create_resource_with_cb(&mut self, n: usize,
                                   acquire_cb: Option<fn(rId: ResourceId, req_state: T) -> T>,
                                   enqueue_cb: Option<fn(rId: ResourceId, req_state: T) -> T>,
                                   release_cb: Option<fn(rId: ResourceId, req_state: T) -> T>,
                                   ) -> ResourceId {

        let id = self.resources.len();
        self.resources.push(Resource {
            id: id,
            allocated: n,
            available: n,
            queue: VecDeque::new(),
            acquire_cb: acquire_cb,
            enqueue_cb: enqueue_cb,
            release_cb: release_cb,
        });
        id
    }

    /// Schedule a process to be executed. Another way to schedule events is
    /// yielding `Effect::Event` from a process during the simulation.
    pub fn schedule_event(&mut self, event: Event) {
        if let Some(ref mut p) = &mut self.processes[event.process] {
            p.state = ProcState::Running;
            self.future_events.push(Reverse(event));
        }
    }

    /// Proceed in the simulation by 1 step
    pub fn step(&mut self) -> bool {
        match self.future_events.pop() {
            Some(Reverse(event)) => {
                self.time = event.time;
                if let Some(ref mut p) = &mut self.processes[event.process] {
                    if p.state == ProcState::Running {
                        let gstatepin = Pin::new(p.code.as_mut()).resume(());
                        match gstatepin.clone() {
                            GeneratorState::Yielded(y) => {
                                self.event_and_log(&event, y);
                            }
                            GeneratorState::Complete(_) => {}
                        }
                        match gstatepin {
                            GeneratorState::Yielded(y) => match y.get_effect() {
                                Effect::TimeOut(t) => {
                                    self.future_events.push(
                                        Reverse(
                                            Event {
                                                time: self.time + t,
                                                process: event.process
                                            }
                                        )
                                    )
                                },
                                Effect::ServiceTimeOut(t, _req_res) => {
                                    self.future_events.push(
                                        Reverse(
                                            Event {
                                                time: self.time + t,
                                                process: event.process
                                            }
                                        )
                                    )
                                }
                                Effect::Schedule(mut e) =>{
                                    e.time += self.time;
                                    self.future_events.push(Reverse(e))
                                },
                                Effect::Request(r) => {
                                    let res = &mut self.resources[r];
                                    let (acquired, sim_state) = res.request(event.process, y);
                                    self.event_and_log(&event, sim_state);
                                    if acquired {
                                        // the process can use the resource immediately
                                        self.future_events.push(Reverse(Event {
                                            time: self.time,
                                            process: event.process,
                                        }));
                                    }
                                }
                                Effect::Release(r) => {
                                    let res = &mut self.resources[r];
                                    let (r_state, next_p) =
                                      res.release_and_get_next(event.process, y);
                                    self.event_and_log(&event, r_state);
                                    if let Some((pid, next_proc_state)) = next_p {
                                       let ev = Event {
                                           time: self.time,
                                           process: pid
                                       };
                                       self.future_events.push(Reverse(ev.clone()));
                                       self.event_and_log(&ev, next_proc_state);
                                    }
                                    // the process releasing the resource can be
                                    // resumed immediately
                                    self.future_events.push(Reverse(Event {
                                        time: self.time,
                                        process: event.process,
                                    }))
                                }
                                Effect::Wait => {}
                                Effect::Trace => {
                                    // trace acts like a no-op, so the process needs to
                                    // be scheduled again with the same time and resumed
                                    self.future_events.push(Reverse(Event {
                                        time: self.time,
                                        process: event.process,
                                    }))
                                }
                            },
                            GeneratorState::Complete(_) => {
                                self.processes[event.process] = None;
                                self.process_free_list.push(event.process);
                            }
                        }
                    }
                }
                self.steps += 1;
                true
            }
            None => {
                false
            }
        }
    }

    /// Run the simulation until and ending condition is met.
    pub fn run(mut self, until: EndCondition) -> Simulation<T> {
        while !self.check_ending_condition(&until) {
            let res = self.step();
            if !res {
                break;
            }
        }
        self
    }
/*
    pub fn nonblocking_run(mut self, until: EndCondition) -> thread::JoinHandle<Simulation> {
        thread::spawn(move || {
            self.run(until)
        })
    }
*/

    /// Return `true` if the ending condition was met, `false` otherwise.
    fn check_ending_condition(&self, ending_condition: &EndCondition) -> bool {
        match &ending_condition {
            EndCondition::Time(t) => if self.time >= *t {
                return true
            },
            EndCondition::NoEvents => if self.future_events.len() == 0 {
                return true
            },
            EndCondition::NSteps(n) => if self.steps == *n {
                return true
            },
        }
        false
    }
}

impl<'t, T: SimStep + Clone> Default for Simulation<T> {
    fn default() -> Self {
        Simulation::<T> {
            time: 0.0,
            steps: 0,
            processes: Vec::default(),
            process_free_list: vec!(),
            future_events: BinaryHeap::default(),
            processed_steps: Vec::default(),
            resources: Vec::default(),
            res_acquire_cb: None,
            res_enqueue_cb: None,
            res_release_cb: None,
            event_processed_cb: None,
        }
    }
}

impl<T: SimStep + Clone> SimResource for Resource<T> {
    type SimResourceId = ResourceId;
    type SimProcessId = ProcessId;
    type SimState = T;

    fn get_id(&self) -> ResourceId { self.id }
    fn set_id(&mut self, r_id: ResourceId) { self.id =r_id; }

    fn get_available(&self) -> usize { self.available }
    fn get_allocated(&self) -> usize { self.allocated }

    fn set_acquire_cb(&mut self,
                      acq_fn: fn(rId: ResourceId, req_state: Self::SimState)
                                 -> Self::SimState)
    {
        self.acquire_cb = Some(acq_fn);
    }
    fn set_enqueue_cb(&mut self,
                      enq_fn: fn(rId: ResourceId, req_state: Self::SimState)
                                 -> Self::SimState)
    {
        self.enqueue_cb = Some(enq_fn);
    }
    fn set_release_cb(&mut self,
                      rel_fn: fn(rId: ResourceId, req_state: Self::SimState)
                                 -> Self::SimState)
    {
        self.release_cb = Some(rel_fn);
    }

    fn request(&mut self, pid: ProcessId, sim_state: T) -> (bool, T) {
        if self.available == 0 {
            self.queue.push_back((pid, sim_state.clone()));
            if let Some(que_fn) = self.enqueue_cb {
                let new_state = que_fn(self.id, sim_state);
                return (false, new_state)
            } else {
                return (false, sim_state)
            }
        }
        // process can use resource immediately
        self.available -= 1;
        if let Some(acq_fn) = self.acquire_cb {
            let new_state = acq_fn(self.id, sim_state);
            return (true, new_state);
        } else {
            return (true, sim_state);
        }
    }
    fn release_and_get_next(&mut self, pid: ProcessId, sim_state: T) -> (T, Option<(ProcessId, T)>) {
        let mut release_state = None;
        if let Some(rel_fn) = self.release_cb {
            release_state = Some(rel_fn(pid, sim_state.clone()));
        }
        if self.queue.is_empty() {
            assert!(self.available < self.allocated);
            self.available += 1;
        }
        if let Some((pid, state)) = self.queue.pop_front() {
            let mut acq_state = None;
            if let Some(acq_fn) = self.acquire_cb {
                acq_state = Some(acq_fn(pid, state.clone()));
            }
            return(release_state.unwrap_or(sim_state),
                   Some((pid, acq_state.unwrap_or(state))));

        } else {
            return (release_state.unwrap_or(sim_state), None);
        }
    }

}

impl PartialEq for Event {
    fn eq(&self, other: &Event) -> bool {
        self.time == other.time
    }
}

impl Eq for Event {}


impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Event) -> Option<Ordering> {
        self.time.partial_cmp(&other.time)
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Event) -> Ordering {
        match self.time.partial_cmp(&other.time) {
            Some(o) => o,
            None => panic!("Event time was uncomparable. Maybe a NaN"),
        }
    }
}

impl<'t> SimStep for Effect<'t> {
    fn get_effect(&self) -> Effect { *self }
    fn should_log(&self) -> bool { true }
    fn trigger_event_cb(&self) -> bool { true }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        use Simulation;
        use Effect;
        use Event;

        let mut s = Simulation::new();
        let p = s.create_process(Box::new(|| {
            let mut a = 0.0;
            loop {
                a += 1.0;

                yield Effect::TimeOut(a);
            }
        }));
        s.schedule_event(Event{time: 0.0, process: p});
        s.step();
        s.step();
        assert_eq!(s.time(), 1.0);
        s.step();
        assert_eq!(s.time(), 3.0);
        s.step();
        assert_eq!(s.time(), 6.0);
    }

    #[test]
    fn run() {
        use Simulation;
        use Effect;
        use Event;
        use EndCondition;

        let mut s = Simulation::new();
        let p = s.create_process( Box::new(|| {
            let tik = 0.7;
            loop{
                println!("tik");
                yield Effect::TimeOut(tik);
            }
        }));
        s.schedule_event(Event{time: 0.0, process: p});
        let s = s.run(EndCondition::Time(10.0));
        println!("{}", s.time());
        assert!(s.time() >= 10.0);
    }

    #[test]
    fn resource() {
        use Simulation;
        use Effect;
        use Event;
        use EndCondition::NoEvents;

        let mut s = Simulation::new();
        let r = s.create_resource(1);

        // simple process that lock the resource for 7 time units
        let p1 = s.create_process(Box::new(move || {
            yield Effect::Request(r);
            yield Effect::TimeOut(7.0);
            yield Effect::Release(r);
        }));
        // simple process that holds the resource for 3 time units
        let p2 = s.create_process(Box::new(move || {
            yield Effect::Request(r);
            yield Effect::TimeOut(3.0);
            yield Effect::Release(r);
        }));

        // let p1 start immediately...
        s.schedule_event(Event{time: 0.0, process: p1});
        // let p2 start after 2 t.u., when r is not available
        s.schedule_event(Event{time: 2.0, process: p2});
        // p2 will wait r to be free (time 7.0) and its timeout
        // of 3.0 t.u. The simulation will end at time 10.0

        let s = s.run(NoEvents);
        println!("{:?}", s.processed_steps());
        assert_eq!(s.time(), 10.0);
    }
}
