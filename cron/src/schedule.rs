use chrono::offset::{LocalResult, TimeZone};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::ops::Bound::{Included, Unbounded};

#[cfg(feature = "serde")]
use core::fmt;
#[cfg(feature = "serde")]
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize, Serializer,
};

use crate::ordinal::*;
use crate::queries::*;
use crate::time_unit::*;

impl From<Schedule> for String {
    fn from(schedule: Schedule) -> String {
        schedule.source
    }
}

#[derive(Clone, Debug, Eq)]
pub struct Schedule {
    source: String,
    fields: ScheduleFields,
}

impl Schedule {
    pub(crate) fn new(source: String, fields: ScheduleFields) -> Schedule {
        Schedule { source, fields }
    }

    fn next_after<Z>(&self, after: &DateTime<Z>) -> Option<DateTime<Z>>
    where
        Z: TimeZone,
    {
        #[cfg(feature = "vixie")]
        let dow_and_dom_specific =
            !self.fields.days_of_week.is_all() && !self.fields.days_of_month.is_all();

        let mut query = NextAfterQuery::from(after);
        for year in self
            .fields
            .years
            .ordinals()
            .range((Included(query.year_lower_bound()), Unbounded))
            .cloned()
        {
            // It's a future year, the current year's range is irrelevant.
            if year > after.year() as u32 {
                query.reset_month();
            }

            let month_start = query.month_lower_bound();
            if !self.fields.months.ordinals().contains(&month_start) {
                query.reset_month();
            }
            let month_range = (Included(month_start), Included(Months::inclusive_max()));
            for month in self.fields.months.ordinals().range(month_range).cloned() {
                let days_of_month = self.fields.days_of_month.ordinals();
                let day_of_month_start = query.day_of_month_lower_bound();
                let day_of_month_end = days_in_month(month, year);
                let day_of_month_range = (
                    Included(day_of_month_start.min(day_of_month_end)),
                    Included(day_of_month_end),
                );

                #[cfg(not(feature = "vixie"))]
                let mut day_of_month_candidates = days_of_month
                    .range(day_of_month_range)
                    .cloned()
                    .filter(|dom| {
                        let dow = match day_of_week(year, month, *dom) {
                            Ok(dow) => dow,
                            Err(_) => return false,
                        };
                        self.fields
                            .days_of_week
                            .ordinals()
                            .contains(&dow)
                    })
                    .peekable();

                #[cfg(feature = "vixie")]
                let mut day_of_month_candidates = {
                    let days_of_week = self.fields.days_of_week.ordinals();

                    (day_of_month_start..=day_of_month_end)
                        .filter(|dom| {
                            let dow = match day_of_week(year, month, *dom) {
                                Ok(dow) => dow,
                                Err(_) => return false,
                            };

                            if dow_and_dom_specific {
                                return days_of_month.contains(dom)
                                    || days_of_week.contains(&dow);
                            }
                            days_of_month.contains(dom)
                                && days_of_week.contains(&dow)
                        })
                        .peekable()
                };

                if day_of_month_candidates.peek() != Some(&day_of_month_start) {
                    query.reset_day_of_month();
                }

                for day_of_month in day_of_month_candidates {
                    let date_candidate = match NaiveDate::from_ymd_opt(year as i32, month, day_of_month) {
                        Some(d) => d,
                        None => continue,
                    };

                    let hour_start = query.hour_lower_bound();
                    if !self.fields.hours.ordinals().contains(&hour_start) {
                        query.reset_hour();
                    }
                    let hour_range = hour_start..=Hours::inclusive_max();

                    for hour in self.fields.hours.ordinals().range(hour_range).cloned() {
                        let minute_start = query.minute_lower_bound();
                        if !self.fields.minutes.ordinals().contains(&minute_start) {
                            query.reset_minute();
                        }
                        let minute_range =
                            (Included(minute_start), Included(Minutes::inclusive_max()));

                        for minute in 
                            self.fields.minutes.ordinals().range(minute_range).cloned()
                        {
                            let second_range = query.second_lower_bound()..=Seconds::inclusive_max();

                            for second in
                                self.fields.seconds.ordinals().range(second_range).cloned()
                            {
                                let naive_time = match NaiveTime::from_hms_opt(hour, minute, second) {
                                    Some(t) => t,
                                    None => continue,
                                };
                                let naive_dt = NaiveDateTime::new(date_candidate, naive_time);
                                match naive_dt.and_local_timezone(after.timezone()) {
                                    LocalResult::Single(dt) if dt > *after => return Some(dt),
                                    // hourly schedules use both occurrences of a repeated hour
                                    LocalResult::Ambiguous(fold_0, _) if self.fields.hours.is_all() && fold_0 > *after => return Some(fold_0),
                                    // hourly schedules just skip ahead to the next valid time
                                    LocalResult::None => if !self.fields.hours.is_all() {
                                        let next_valid = find_next_valid_datetime(&after.timezone(), naive_dt);
                                        if next_valid > *after {
                                            return Some(next_valid);
                                        } else {
                                            continue;
                                        }
                                    },
                                    _ => continue,
                                };

                            } // End of seconds range
                            query.reset_minute();
                        } // End of minutes range

                        // Check the repeated hour for any matching times.
                        let naive_dt = NaiveDateTime::new(date_candidate, NaiveTime::from_hms_opt(hour, minute_start, 0).unwrap());
                        let in_fold = match naive_dt.and_local_timezone(after.timezone()) {
                            LocalResult::Ambiguous(_, _) => true,
                            _ => false,
                        };
                        if in_fold {
                            let next_fold_range = (
                                Included(0),
                                Included(Minutes::inclusive_max()),
                            );

                            for minute in 
                                self.fields.minutes.ordinals().range(next_fold_range).cloned() 
                            {
                                let second_start = query.second_lower_bound();
                                let second_range =
                                    (Included(second_start), Included(Seconds::inclusive_max()));

                                for second in
                                    self.fields.seconds.ordinals().range(second_range).cloned()
                                {
                                    let naive_time = match NaiveTime::from_hms_opt(hour, minute, second) {
                                        Some(t) => t,
                                        None => continue,
                                    };
                                    let naive_dt = NaiveDateTime::new(date_candidate, naive_time);
                                    match naive_dt.and_local_timezone(after.timezone()) {
                                        LocalResult::Single(dt) if dt > *after => return Some(dt),
                                        LocalResult::Ambiguous(_, fold_1) if fold_1 > *after => return Some(fold_1),
                                        _ => continue,
                                    };

                                } // End of seconds range
                                query.reset_minute();
                            } // End of minutes range
                        }

                        query.reset_hour();
                    } // End of hours range
                    query.reset_day_of_month();
                } // End of Day of Month range
                query.reset_month();
            } // End of Month range
        }

        // We ran out of dates to try.
        None
    }

    fn prev_from<Z>(&self, before: &DateTime<Z>) -> Option<DateTime<Z>>
    where
        Z: TimeZone,
    {
        #[cfg(feature = "vixie")]
        let dow_and_dom_specific =
            !self.fields.days_of_week.is_all() && !self.fields.days_of_month.is_all();

        let mut query = PrevFromQuery::from(before);
        for year in self
            .fields
            .years
            .ordinals()
            .range((Unbounded, Included(query.year_upper_bound())))
            .rev()
            .cloned()
        {
            let month_start = query.month_upper_bound();

            if !self.fields.months.ordinals().contains(&month_start) {
                query.reset_month();
            }
            let month_range = (Included(Months::inclusive_min()), Included(month_start));

            for month in self
                .fields
                .months
                .ordinals()
                .range(month_range)
                .rev()
                .cloned()
            {
                let days_of_month = self.fields.days_of_month.ordinals();
                let day_of_month_end = query.day_of_month_upper_bound();
                let day_of_month_range = (
                    Included(DaysOfMonth::inclusive_min()),
                    Included(days_in_month(month, year).min(day_of_month_end)),
                );

                #[cfg(not(feature = "vixie"))]
                let mut day_of_month_candidates = days_of_month
                    .range(day_of_month_range)
                    .rev()
                    .cloned()
                    .filter(|dom| {
                        let dow = match day_of_week(year, month, *dom) {
                            Ok(dow) => dow,
                            Err(_) => return false,
                        };

                        self.fields
                            .days_of_week
                            .ordinals()
                            .contains(&dow)
                    })
                    .peekable();

                #[cfg(feature = "vixie")]
                let mut day_of_month_candidates = {
                    let days_of_week = self.fields.days_of_week.ordinals();

                    (DaysOfMonth::inclusive_min()..=day_of_month_end)
                        .into_iter()
                        .rev()
                        .filter(|dom| {
                            let dow = match day_of_week(year, month, *dom) {
                                Ok(dow) => dow,
                                Err(_) => return false,
                            };

                            if dow_and_dom_specific {
                                return days_of_month.contains(dom)
                                    || days_of_week.contains(&dow);
                            }
                            days_of_month.contains(dom)
                                && days_of_week.contains(&dow)
                        })
                        .peekable()
                };

                if day_of_month_candidates.peek() != Some(&day_of_month_end) {
                    query.reset_day_of_month();
                }

                for day_of_month in day_of_month_candidates {
                    let date_candidate = match NaiveDate::from_ymd_opt(year as i32, month, day_of_month) {
                        Some(d) => d,
                        None => continue,
                    };
                    let hour_start = query.hour_upper_bound();
                    if !self.fields.hours.ordinals().contains(&hour_start) {
                        query.reset_hour();
                    }
                    let hour_range = Hours::inclusive_min()..=hour_start;

                    for hour in self
                        .fields
                        .hours
                        .ordinals()
                        .range(hour_range)
                        .rev()
                        .cloned()
                    {
                        let minute_start = query.minute_upper_bound();
                        if !self.fields.minutes.ordinals().contains(&minute_start) {
                            query.reset_minute();
                        }
                        let minute_range = Minutes::inclusive_min()..=minute_start;

                        for minute in 
                            self.fields.minutes.ordinals().range(minute_range).rev().cloned() 
                        {
                            let second_range = Seconds::inclusive_min()..=query.second_upper_bound();

                            for second in 
                                self.fields.seconds.ordinals().range(second_range).rev().cloned()
                            {
                                let naive_time = match NaiveTime::from_hms_opt(hour, minute, second) {
                                    Some(t) => t,
                                    None => continue,
                                };
                                let naive_dt = NaiveDateTime::new(date_candidate, naive_time);
                                match naive_dt.and_local_timezone(before.timezone()) {
                                    LocalResult::None if !self.fields.hours.is_all() => {
                                        let next_valid = find_next_valid_datetime(&before.timezone(), naive_dt);
                                        if next_valid < *before {
                                            return Some(next_valid);
                                        } else {
                                            continue;
                                        }
                                    },
                                    LocalResult::Single(dt) if dt < *before => return Some(dt),
                                    LocalResult::Ambiguous(_, fold_1 ) if fold_1 < *before => return Some(fold_1),
                                    _ => {}
                                };
                            }
                            query.reset_minute();
                        } // End of minutes range

                        // We need to check the repeated hour for any matching times.
                        let naive_dt = NaiveDateTime::new(date_candidate, NaiveTime::from_hms_opt(hour, minute_start, 0).unwrap());
                        let in_fold = match naive_dt.and_local_timezone(before.timezone()) {
                            LocalResult::Ambiguous(_, _) => true,
                            _ => false,
                        };

                        if in_fold {
                            let next_fold_range = Minutes::inclusive_min()..=Minutes::inclusive_max();

                            for minute in 
                                self.fields.minutes.ordinals().range(next_fold_range).rev().cloned() 
                            {
                                let second_range = Seconds::inclusive_min()..=query.second_upper_bound();

                                for second in
                                    self.fields.seconds.ordinals().range(second_range).rev().cloned()
                                {
                                    let naive_time = match NaiveTime::from_hms_opt(hour, minute, second) {
                                        Some(t) => t,
                                        None => continue,
                                    };
                                    let naive_dt = NaiveDateTime::new(date_candidate, naive_time);
                                    match naive_dt.and_local_timezone(before.timezone()) {
                                        LocalResult::Single(dt) if dt < *before => return Some(dt),
                                        LocalResult::Ambiguous(fold_0, _ ) if self.fields.hours.is_all() && fold_0 < *before => return Some(fold_0),
                                        _ => {}
                                    };
                                } // End of seconds range
                                query.reset_minute();
                            } // End of minutes range
                        }

                        query.reset_hour();
                    } // End of hours range
                    query.reset_day_of_month();
                } // End of Day of Month range
                query.reset_month();
            } // End of Month range
        }

        // We ran out of dates to try.
        None
    }

    /// Provides an iterator which will return each DateTime that matches the schedule starting with
    /// the current time if applicable.
    pub fn upcoming<Z>(&self, timezone: Z) -> ScheduleIterator<'_, Z>
    where
        Z: TimeZone,
    {
        self.after(&timezone.from_utc_datetime(&Utc::now().naive_utc()))
    }

    /// The same, but with an iterator with a static ownership
    pub fn upcoming_owned<Z: TimeZone>(&self, timezone: Z) -> OwnedScheduleIterator<Z> {
        self.after_owned(timezone.from_utc_datetime(&Utc::now().naive_utc()))
    }

    /// Like the `upcoming` method, but allows you to specify a start time other than the present.
    pub fn after<Z>(&self, after: &DateTime<Z>) -> ScheduleIterator<'_, Z>
    where
        Z: TimeZone,
    {
        ScheduleIterator::new(self, after)
    }

    /// The same, but with a static ownership.
    pub fn after_owned<Z: TimeZone>(&self, after: DateTime<Z>) -> OwnedScheduleIterator<Z> {
        OwnedScheduleIterator::new(self.clone(), after)
    }

    #[cfg(feature = "vixie")]
    /// Vixie cron behavior: If DOM is specific and DOW is inspecific, then only DOM is considered.
    /// If DOW is specific and DOM is inspecific, then only DOW is considered
    /// If both are specific, then either is considered.
    fn includes_dom_dow<Z>(&self, date_time: &DateTime<Z>) -> bool
    where
        Z: TimeZone,
    {
        let dow_inspecific = self.fields.days_of_week.is_all();
        let dom_inspecific = self.fields.days_of_month.is_all();
        let dow_includes = self
            .fields
            .days_of_week
            .includes(date_time.weekday().number_from_sunday());
        let dom_includes = self
            .fields
            .days_of_month
            .includes(date_time.day() as Ordinal);

        (dow_inspecific || dom_inspecific)
            && (!dow_inspecific || dow_includes)
            && (!dom_inspecific || dom_includes)
    }

    #[cfg(not(feature = "vixie"))]
    /// Quartz (the default) cron behavior: Both DOM and DOW must match.
    fn includes_dom_dow<Z>(&self, date_time: &DateTime<Z>) -> bool
    where
        Z: TimeZone,
    {
        self.fields
            .days_of_week
            .includes(date_time.weekday().number_from_sunday())
            && self
                .fields
                .days_of_month
                .includes(date_time.day() as Ordinal)
    }

    pub fn includes<Z>(&self, date_time: DateTime<Z>) -> bool
    where
        Z: TimeZone,
    {
        self.fields.years.includes(date_time.year() as Ordinal)
            && self.fields.months.includes(date_time.month() as Ordinal)
            && self.includes_dom_dow(&date_time)
            && self.fields.hours.includes(date_time.hour() as Ordinal)
            && self.fields.minutes.includes(date_time.minute() as Ordinal)
            && self.fields.seconds.includes(date_time.second() as Ordinal)
    }

    /// Returns a [TimeUnitSpec] describing the years included in this [Schedule].
    pub fn years(&self) -> &impl TimeUnitSpec {
        &self.fields.years
    }

    /// Returns a [TimeUnitSpec] describing the months of the year included in this [Schedule].
    pub fn months(&self) -> &impl TimeUnitSpec {
        &self.fields.months
    }

    /// Returns a [TimeUnitSpec] describing the days of the month included in this [Schedule].
    pub fn days_of_month(&self) -> &impl TimeUnitSpec {
        &self.fields.days_of_month
    }

    /// Returns a [TimeUnitSpec] describing the days of the week included in this [Schedule].
    pub fn days_of_week(&self) -> &impl TimeUnitSpec {
        &self.fields.days_of_week
    }

    /// Returns a [TimeUnitSpec] describing the hours of the day included in this [Schedule].
    pub fn hours(&self) -> &impl TimeUnitSpec {
        &self.fields.hours
    }

    /// Returns a [TimeUnitSpec] describing the minutes of the hour included in this [Schedule].
    pub fn minutes(&self) -> &impl TimeUnitSpec {
        &self.fields.minutes
    }

    /// Returns a [TimeUnitSpec] describing the seconds of the minute included in this [Schedule].
    pub fn seconds(&self) -> &impl TimeUnitSpec {
        &self.fields.seconds
    }

    pub fn timeunitspec_eq(&self, other: &Schedule) -> bool {
        self.fields == other.fields
    }

    /// Returns a reference to the source cron expression.
    pub fn source(&self) -> &str {
        &self.source
    }
}

impl Display for Schedule {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.source)
    }
}

impl PartialEq for Schedule {
    fn eq(&self, other: &Schedule) -> bool {
        self.source == other.source
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleFields {
    years: Years,
    days_of_week: DaysOfWeek,
    months: Months,
    days_of_month: DaysOfMonth,
    hours: Hours,
    minutes: Minutes,
    seconds: Seconds,
}

impl ScheduleFields {
    pub(crate) fn new(
        seconds: Seconds,
        minutes: Minutes,
        hours: Hours,
        days_of_month: DaysOfMonth,
        months: Months,
        days_of_week: DaysOfWeek,
        years: Years,
    ) -> ScheduleFields {
        ScheduleFields {
            years,
            days_of_week,
            months,
            days_of_month,
            hours,
            minutes,
            seconds,
        }
    }
}

pub struct ScheduleIterator<'a, Z>
where
    Z: TimeZone,
{
    schedule: &'a Schedule,
    previous_datetime: Option<DateTime<Z>>,
}
//TODO: Cutoff datetime?

impl<'a, Z> ScheduleIterator<'a, Z>
where
    Z: TimeZone,
{
    fn new(schedule: &'a Schedule, starting_datetime: &DateTime<Z>) -> Self {
        ScheduleIterator {
            schedule,
            previous_datetime: Some(starting_datetime.clone()),
        }
    }
}

impl<Z> Iterator for ScheduleIterator<'_, Z>
where
    Z: TimeZone,
{
    type Item = DateTime<Z>;

    fn next(&mut self) -> Option<DateTime<Z>> {
        let previous = match &self.previous_datetime {
            Some(dt) => dt,
            None => return None,
        };
        self.previous_datetime = self.schedule.next_after(&previous);
        return self.previous_datetime.clone();
    }
}

impl<Z> DoubleEndedIterator for ScheduleIterator<'_, Z>
where
    Z: TimeZone,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let previous = match &self.previous_datetime {
            Some(dt) => dt,
            None => return None,
        };
        self.previous_datetime = self.schedule.prev_from(&previous);
        return self.previous_datetime.clone();
    }
}

/// A `ScheduleIterator` with a static lifetime.
pub struct OwnedScheduleIterator<Z>
where
    Z: TimeZone,
{
    schedule: Schedule,
    previous_datetime: Option<DateTime<Z>>,
}

impl<Z> OwnedScheduleIterator<Z>
where
    Z: TimeZone,
{
    pub fn new(schedule: Schedule, starting_datetime: DateTime<Z>) -> Self {
        Self {
            schedule,
            previous_datetime: Some(starting_datetime),
        }
    }
}

impl<Z> Iterator for OwnedScheduleIterator<Z>
where
    Z: TimeZone,
{
    type Item = DateTime<Z>;

    fn next(&mut self) -> Option<DateTime<Z>> {
        let previous = match &self.previous_datetime {
            Some(dt) => dt,
            None => return None,
        };
        self.previous_datetime = self.schedule.next_after(&previous);
        return self.previous_datetime.clone();
    }
}

impl<Z: TimeZone> DoubleEndedIterator for OwnedScheduleIterator<Z> {
    fn next_back(&mut self) -> Option<Self::Item> {
       let previous = match &self.previous_datetime {
            Some(dt) => dt,
            None => return None,
        };
        self.previous_datetime = self.schedule.prev_from(&previous);
        return self.previous_datetime.clone();
    }
}

fn is_leap_year(year: Ordinal) -> bool {
    let by_four = year.is_multiple_of(4);
    let by_hundred = year.is_multiple_of(100);
    let by_four_hundred = year.is_multiple_of(400);
    by_four && ((!by_hundred) || by_four_hundred)
}

fn days_in_month(month: Ordinal, year: Ordinal) -> u32 {
    let is_leap_year = is_leap_year(year);
    match month {
        9 | 4 | 6 | 11 => 30,
        2 if is_leap_year => 29,
        2 => 28,
        _ => 31,
    }
}

fn find_next_valid_datetime<Z: TimeZone>(
    tz: &Z,
    mut naive_dt: NaiveDateTime
) -> DateTime<Z> {
    // Align to next 15-minute boundary (00, 15, 30, 45)
    let current_minute = naive_dt.minute();
    let next_aligned_minute = ((current_minute / 15) + 1) * 15;
    
    if next_aligned_minute >= 60 {
        // Roll over to next hour at :00
        naive_dt = naive_dt.with_minute(0).unwrap().with_second(0).unwrap() + chrono::Duration::hours(1);
    } else {
        naive_dt = naive_dt.with_minute(next_aligned_minute).unwrap().with_second(0).unwrap();
    }
    
    loop {
        match tz.from_local_datetime(&naive_dt) {
            LocalResult::Single(dt) => return dt,
            LocalResult::Ambiguous(dt, _) => return dt, // Take the first occurrence
            LocalResult::None => {
                // Increment by 15 minutes and try again
                naive_dt += chrono::Duration::minutes(15);
            }
        }
    }
}

#[cfg(not(feature = "vixie"))]
fn day_of_week(year: u32, month: u32, day: u32) -> Result<u32, ()> {
    match chrono::NaiveDate::from_ymd_opt(year as i32, month, day) {
        Some(d) => Ok(d.weekday().number_from_sunday()),
        None => Err(()),
    }
}

#[cfg(feature = "vixie")]
fn day_of_week(year: u32, month: u32, day: u32) -> Result<u32, ()> {
    match chrono::NaiveDate::from_ymd_opt(year as i32, month, day) {
        Some(d) => Ok(d.weekday().num_days_from_sunday()),
        None => Err(()),
    }
}

#[cfg(feature = "serde")]
struct ScheduleVisitor;

#[cfg(feature = "serde")]
impl Visitor<'_> for ScheduleVisitor {
    type Value = Schedule;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a valid cron expression")
    }

    // Supporting `Deserializer`s shall provide an owned `String`.
    //
    // The `Schedule` will decode from a `&str` to it,
    // then store the owned `String` as `Schedule::source`.
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Schedule::try_from(v).map_err(de::Error::custom)
    }

    // `Deserializer`s not providing an owned `String`
    // shall provide a `&str`.
    //
    // The `Schedule` will decode from the `&str`,
    // then clone into the heap to store as an owned `String`
    // as `Schedule::source`.
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Schedule::try_from(v).map_err(de::Error::custom)
    }
}

#[cfg(feature = "serde")]
impl Serialize for Schedule {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.source())
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for Schedule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Hint that the `Deserialize` type `Schedule`
        // would benefit from taking ownership of
        // buffered data owned by the `Deserializer`:
        //
        // The deserialization "happy path" decodes from a `&str`,
        // then stores the source as owned `String`.
        //
        // Thus, the optimized happy path receives an owned `String`
        // if the `Deserializer` in use supports providing one.
        deserializer.deserialize_string(ScheduleVisitor)
    }
}

#[cfg(test)]
mod test {
    #[cfg(feature = "serde")]
    use serde_test::{assert_tokens, Token};

    #[cfg(feature = "serde")]
    #[test]
    fn test_ser_de_schedule_tokens() {
        let schedule = Schedule::from_str("* * * * * * *").expect("valid format");
        assert_tokens(&schedule, &[Token::String("* * * * * * *")])
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_invalid_ser_de_schedule_tokens() {
        use serde_test::assert_de_tokens_error;

        assert_de_tokens_error::<Schedule>(
            &[Token::String(
                "definitively an invalid value for a cron schedule!",
            )],
            "definitively an invalid value for a cron schedule!\n\
                ^\n\
                The 'Minutes' field does not support using names. 'definitively' specified.",
        );
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_ser_de_schedule_shorthand() {
        let serialized = postcard::to_stdvec(&Schedule::try_from("@hourly").expect("valid format"))
            .expect("serializable schedule");

        let schedule: Schedule =
            postcard::from_bytes(&serialized).expect("deserializable schedule");

        let starting_date = Utc.with_ymd_and_hms(2017, 2, 25, 22, 29, 36).unwrap();
        assert!([
            Utc.with_ymd_and_hms(2017, 2, 25, 23, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2017, 2, 26, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2017, 2, 26, 1, 0, 0).unwrap(),
        ]
        .into_iter()
        .eq(schedule.after(&starting_date).take(3)));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_ser_de_schedule_period_values_range() {
        let serialized =
            postcard::to_stdvec(&Schedule::try_from("0 0 0 1-31/10 * ?").expect("valid format"))
                .expect("serializable schedule");

        let schedule: Schedule =
            postcard::from_bytes(&serialized).expect("deserializable schedule");

        let starting_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        assert!([
            Utc.with_ymd_and_hms(2020, 1, 11, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2020, 1, 21, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2020, 1, 31, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2020, 2, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2020, 2, 11, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2020, 2, 21, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2020, 3, 1, 0, 0, 0).unwrap(),
        ]
        .into_iter()
        .eq(schedule.after(&starting_date).take(7)));
    }
}
