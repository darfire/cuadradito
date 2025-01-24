use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::time::{sleep as tokio_sleep, Duration, Instant};
use tokio::sync::Notify;

use async_trait::async_trait;

#[async_trait]
pub trait TimeKeeper {
  fn get_current_instant(&self) -> Instant;

  fn get_current_time(&self) -> SystemTime;

  async fn sleep(&mut self, duration: Duration);
}

#[async_trait]
pub trait ControllableTimeKeeper: TimeKeeper {
  fn advance_instant(&mut self, duration: Duration);

  fn get_next_instant(&self) -> Option<Instant>;

  fn advance_to_first(&mut self) {
    if let Some(time) = self.get_next_instant() {
      self.advance_instant(time - self.get_current_instant());
    }
  }
}

pub struct SystemTimeKeeper {
}

impl SystemTimeKeeper {
  pub fn new() -> Self {
    Self { }
  }
}

struct WaitingTask {
  instant: Instant,
  notify: Arc<Notify>,
}

impl Eq for WaitingTask {}

impl PartialEq for WaitingTask {
  fn eq(&self, other: &Self) -> bool {
    self.instant == other.instant
  }
}

impl PartialOrd for WaitingTask {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for WaitingTask {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    other.instant.cmp(&self.instant)
  }
}

pub struct ControllableSystemTimeKeeper {
  start_time: SystemTime,
  start_instant: Instant,

  current_instant: Instant,
  waiting_tasks: BinaryHeap<WaitingTask>,
}

#[async_trait]
impl TimeKeeper for SystemTimeKeeper {
  fn get_current_instant(&self) -> Instant {
    Instant::now()
  }

  fn get_current_time(&self) -> SystemTime {
    SystemTime::now()
  }

  async fn sleep(&mut self, duration: Duration) {
    tokio_sleep(duration).await;
  }
}

#[async_trait]
impl TimeKeeper for ControllableSystemTimeKeeper {
  fn get_current_instant(&self) -> Instant {
    self.current_instant
  }

  fn get_current_time(&self) -> SystemTime {
    self.start_time + self.current_instant.duration_since(self.start_instant)
  }

  async fn sleep(&mut self, duration: Duration) {
    let notify = Arc::new(Notify::new());
    let waiting_task = WaitingTask {
      instant: self.current_instant + duration,
      notify: notify.clone(),
    };

    self.waiting_tasks.push(waiting_task);
  }
}

impl ControllableTimeKeeper for ControllableSystemTimeKeeper {
  fn advance_instant(&mut self, duration: Duration) {
    self.current_instant += duration;

    // notify all tasks that are ready
    while let Some(task) = self.waiting_tasks.peek() {
      if task.instant <= self.current_instant {
        let task = self.waiting_tasks.pop();

        if let Some(task) = task {
          task.notify.notify_one();
        } else {
          break;
        }
      } else {
        break;
      }
    }
  }

  fn get_next_instant(&self) -> Option<Instant> {
    self.waiting_tasks.peek().map(|task| task.instant)
  }
}

impl ControllableSystemTimeKeeper {
  pub fn new() -> Self {
    let start_instant = Instant::now(); 
    Self {
      start_time: SystemTime::now(),
      start_instant,
      current_instant: start_instant,
      waiting_tasks: BinaryHeap::new(),
    }
  }

  pub fn with_start_time(&mut self, start_time: SystemTime) -> &mut Self {
    self.start_time = start_time;
    self
  }
}


pub enum GlobalTimeKeeper {
  System(SystemTimeKeeper),
  Controllable(ControllableSystemTimeKeeper),
}


static mut TIME_KEEPER: Option<Box<GlobalTimeKeeper>> = None;


pub fn set_time_keeper(time_keeper: Box<GlobalTimeKeeper>) {
  unsafe {
    TIME_KEEPER = Some(time_keeper);
  }
}


pub fn get_time_keeper() -> &'static mut GlobalTimeKeeper {
  unsafe {
    TIME_KEEPER.as_mut().expect("TimeKeeper is not set")
  }
}


pub fn get_current_instant() -> Instant {
  match get_time_keeper() {
    GlobalTimeKeeper::System(time_keeper) => {
      time_keeper.get_current_instant()
    },
    GlobalTimeKeeper::Controllable(time_keeper) => {
      time_keeper.get_current_instant()
    },
  }
}


pub fn get_current_time() -> SystemTime {
  match get_time_keeper() {
    GlobalTimeKeeper::System(time_keeper) => {
      time_keeper.get_current_time()
    },
    GlobalTimeKeeper::Controllable(time_keeper) => {
      time_keeper.get_current_time()
    },
  }
}


pub async fn sleep(duration: Duration) {
  match get_time_keeper() {
    GlobalTimeKeeper::System(_) => {
      tokio_sleep(duration).await;
    },
    GlobalTimeKeeper::Controllable(time_keeper) => {
      time_keeper.sleep(duration).await;
    },
  }
}


pub fn advance_instant(duration: Duration) {
  match get_time_keeper() {
    GlobalTimeKeeper::System(_) => {
      panic!("advance_time is not supported for SystemTimeKeeper");
    },
    GlobalTimeKeeper::Controllable(time_keeper) => {
      time_keeper.advance_instant(duration);
    },
  }
}

pub fn advance_to_first() {
  match get_time_keeper() {
    GlobalTimeKeeper::System(_) => {
      panic!("advance_to_first is not supported for SystemTimeKeeper");
    },
    GlobalTimeKeeper::Controllable(time_keeper) => {
      time_keeper.advance_to_first();
    },
  }
}