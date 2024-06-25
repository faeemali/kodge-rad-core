/**
 * A timer based on the system clock (not a monotonic clock). This means
 * timer adjustments will be made if the system time is adjusted (more than likely,
 * timers will expire sooner, but clocks may also be stretched).
 *
 * This is NOT suitable if accurate timing is required.
 */
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct Timer {
    val: u128,
    start_time: u128,
}

impl Timer {
    pub fn new(val: Duration) -> Self {
        Timer {
            val: val.as_millis(),
            start_time: Timer::get_curr_time_as_millis(),
        }
    }

    /**
     * retrieves the system time. This is NOT from a monotonic clock
     */
    fn get_curr_time_as_millis() -> u128 {
        let curr_sys_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        curr_sys_time.as_millis()
    }

    pub fn timed_out(&self) -> bool {
        let curr_time = Timer::get_curr_time_as_millis();
        if curr_time < self.start_time {
            /* possible clock adjustment (backwards) */
            return true;
        }

        let diff = curr_time - self.start_time;
        diff > self.val
    }

    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.start_time = Timer::get_curr_time_as_millis();
    }

    #[allow(dead_code)]
    pub fn reset_to_time(&mut self, val: Duration) {
        self.val = val.as_millis();
        self.start_time = Timer::get_curr_time_as_millis();
    }
}