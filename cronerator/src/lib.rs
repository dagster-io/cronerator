// Except for explicit areas where we enable unsafe
#![allow(unsafe_op_in_unsafe_fn)]
#![deny(unsafe_code)]

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use chrono::DateTime;
use chrono_tz::Tz;
use cron::{Schedule, OwnedScheduleIterator};


#[pyclass]
pub struct Cronerator {
    schedule: Schedule,
    iterator: OwnedScheduleIterator<Tz>,
}

#[pymethods]
impl Cronerator {
    #[new]
    fn new(cron_expr: &str, start_time: Option<DateTime<Tz>>) -> PyResult<Self> {
        let schedule = Schedule::try_from(cron_expr)
            .map_err(|e| PyValueError::new_err(format!("Invalid cron expression: {}", e)))?;

        let iterator = match start_time {
            Some(time) => schedule.after_owned(time),
            None => schedule.upcoming_owned(chrono_tz::UTC),
        };
        Ok(Cronerator {
            iterator,
            schedule,
        })
    }

    fn get_next(&mut self) -> Option<DateTime<Tz>> {
        self.iterator.next()
    }

    fn get_prev(&mut self) -> Option<DateTime<Tz>> {
        self.iterator.next_back()
    }

    fn __contains__(&self, datetime: DateTime<Tz>) -> bool {
        self.schedule.includes(datetime)
    }

    fn __iter__(slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf
    }

    fn __next__(&mut self) -> Option<DateTime<Tz>> {
        self.get_next()
    }
}

#[pymodule]
fn cronerator(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Cronerator>()?;
    Ok(())
}