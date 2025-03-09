#![allow(non_camel_case_types)]

use std::mem::MaybeUninit;
use std::ptr;
use std::time::{Instant, SystemTime};

fn stats(t0: Instant, mut v: Vec<i32>) {
    let elapsed = t0.elapsed().as_secs_f32();
    v.sort();
    println!("{:.1} ms => {:.0} ns/iter", 1e3 * elapsed, 1e9 * elapsed / v.len() as f32);
    println!("min {}, 10%ile {}, med {}, 90%ile {}, 95%ile {}, 99%ile {}, 99.9%ile {}, max {}",
    v[0], v[v.len() / 10], v[v.len() / 2], v[v.len() * 9 / 10], v[v.len() * 95 / 100], v[v.len() * 99 / 100], v[v.len() * 999 / 1000], v[v.len() - 1]);
}

fn bench_instant(n_rep: usize) {
    println!("instant: starting...");
    let t0 = Instant::now();
    let mut v = Vec::with_capacity(n_rep);
    while v.len() < n_rep {
        let dt = Instant::now().elapsed();
        if dt.as_nanos() > 0 {
            v.push(dt.as_nanos() as i32);
        }
    }
    stats(t0, v);

    let t1 = Instant::now();
    for _ in 0..n_rep {
        let _ = std::hint::black_box(Instant::now());
    }
    let elapsed = t1.elapsed().as_secs_f32();
    println!("{:.1} ms => {:.0} ns/iter", 1e3 * elapsed, 1e9 * elapsed / n_rep as f32);
}

fn bench_system_time(n_rep: usize) {
    println!("system time: starting...");
    let t0 = Instant::now();
    let mut v = Vec::with_capacity(n_rep);
    while v.len() < n_rep {
        let dt = SystemTime::now().elapsed().unwrap();
        if dt.as_nanos() > 0 {
            v.push(dt.as_nanos() as i32);
        }
    }

    stats(t0, v);


    let t1 = Instant::now();
    for _ in 0..n_rep {
        let _ = std::hint::black_box(SystemTime::now());
    }
    let elapsed = t1.elapsed().as_secs_f32();
    println!("{:.1} ms => {:.0} ns/iter", 1e3 * elapsed, 1e9 * elapsed / n_rep as f32);
}

#[cfg(target_os = "macos")]
fn bench_get_time_of_day(n_rep: usize) {
    println!("get time of day: starting...");
    let t0 = Instant::now();
    let mut v = Vec::with_capacity(n_rep);
    while v.len() < n_rep {
        unsafe {
            let mut t0= MaybeUninit::<libc::timeval>::uninit();
            assert_eq!(libc::gettimeofday(t0.as_mut_ptr(), ptr::null_mut()), 0, "can gettimeofday() ever fail?");
            let t0 = t0.assume_init();
            let mut t1 = MaybeUninit::<libc::timeval>::uninit();
            assert_eq!(libc::gettimeofday(t1.as_mut_ptr(), ptr::null_mut()), 0, "can gettimeofday() ever fail?");
            let t1 = t1.assume_init();
            if t0 != t1 {
                let dt_nanos = 1000_000_000_i64 * (t1.tv_sec - t0.tv_sec) + 1000_i64 * i64::from(t1.tv_usec - t0.tv_usec);
                if 0 < dt_nanos {
                    v.push(dt_nanos as i32);
                }
            }
        }
    }

    stats(t0, v);


    let t1 = Instant::now();
    for _ in 0..n_rep {
        let mut t0= MaybeUninit::<libc::timeval>::uninit();
        let t0 = unsafe {
            assert_eq!(libc::gettimeofday(t0.as_mut_ptr(), ptr::null_mut()), 0, "can gettimeofday() ever fail?");
            t0.assume_init();
        };
        let _ = std::hint::black_box(t0);
    }
    let elapsed = t1.elapsed().as_secs_f32();
    println!("{:.1} ms => {:.0} ns/iter", 1e3 * elapsed, 1e9 * elapsed / n_rep as f32);
}

#[cfg(target_os = "macos")]
fn bench_clock_gettime(n_rep: usize, clk: libc::clockid_t, name: &str) {
    println!("clock_gettime({name}): starting...");
    let t0 = Instant::now();
    let mut v = Vec::with_capacity(n_rep);
    while v.len() < n_rep {
        unsafe {
            let mut t0= MaybeUninit::<libc::timespec>::uninit();
            assert_eq!(libc::clock_gettime(clk, t0.as_mut_ptr()), 0, "can clock_gettime() ever fail?");
            let t0 = t0.assume_init();
            let mut t1 = MaybeUninit::<libc::timespec>::uninit();
            assert_eq!(libc::clock_gettime(clk, t1.as_mut_ptr()), 0, "can clock_gettime() ever fail?");
            let t1 = t1.assume_init();
            if t0 != t1 {
                let dt_nanos = 1000_000_000_i64 * (t1.tv_sec - t0.tv_sec) + t1.tv_nsec - t0.tv_nsec;
                if 0 < dt_nanos {
                    v.push(dt_nanos as i32);
                }
            }
        }
    }

    stats(t0, v);


    let t1 = Instant::now();
    for _ in 0..n_rep {
        let mut t0= MaybeUninit::<libc::timespec>::uninit();
        let t0 = unsafe {
            assert_eq!(libc::clock_gettime(clk, t0.as_mut_ptr()), 0, "can clock_gettime() ever fail?");
            t0.assume_init();
        };
        let _ = std::hint::black_box(t0);
    }
    let elapsed = t1.elapsed().as_secs_f32();
    println!("{:.1} ms => {:.0} ns/iter", 1e3 * elapsed, 1e9 * elapsed / n_rep as f32);
}



fn main() {
    bench_instant(10_000_000);
    println!();

    bench_system_time(1000_000);
    #[cfg(target_os = "macos")]
    {
        bench_system_time(1000_000);
        println!();
        bench_get_time_of_day(1000_000);
        println!();
        bench_clock_gettime(1000_000, libc::CLOCK_REALTIME, "REALTIME");
        println!();
        bench_clock_gettime(1000_000, libc::CLOCK_MONOTONIC, "MONOTONIC");
        println!();
        bench_clock_gettime(1000_000, libc::CLOCK_UPTIME_RAW, "UPTIME_RAW");
    }
}