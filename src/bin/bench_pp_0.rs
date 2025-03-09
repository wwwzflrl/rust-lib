use std::hash::{DefaultHasher, Hasher};
use std::io::{Cursor, Write};
use std::{env, fmt};
use std::fmt::Formatter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use core_affinity::{self, CoreId};
use log::{trace, debug, info, warn, error, log_enabled, Level, LevelFilter};
use rand::{rng, RngCore};

const GIGA: u64 = 1_000_000_000;

mod gl {
    use std::sync::atomic::{AtomicBool, Ordering};

    static INTERRUPTED: AtomicBool = AtomicBool::new(false);

    pub fn interrupt() {
        INTERRUPTED.store(true, Ordering::Relaxed);
    }
    pub fn is_interrupted() -> bool {
        INTERRUPTED.load(Ordering::Relaxed)
    }
}

mod time {
    use libc::{self, timespec};
    use std::io::Error as IoError;
    use std::mem::MaybeUninit;
    use std::num::NonZeroU64;
    use std::ops::Sub;
    use super::GIGA;

    pub fn clock_gettime() -> timespec {
        let mut ts = MaybeUninit::uninit();
        unsafe {
            // assert_eq!(
            //     libc::clock_gettime(libc::CLOCK_REALTIME, ts.as_mut_ptr()),
            //     0,
            //     "libc::clock_gettime: {}",
            //     IoError::last_os_error()
            // );
            ts.assume_init()
        }
    }

    #[repr(transparent)]
    #[derive(Debug, Copy, Clone)]
    pub struct NanoUnixTime {
        raw: NonZeroU64,
    }

    /*
    struct NanoUnixTimeBreakdown {
        tv_sec: u32,
        tv_nsec: u32,
    }
     */

    impl NanoUnixTime {
        pub fn now() -> Self {
            let ts = clock_gettime();
            Self {
                raw: NonZeroU64::new(ts.tv_sec as u64 * GIGA + ts.tv_nsec as u64)
                    .expect("clock_gettime must not return zero"),
            }
        }
        pub fn as_u64(&self) -> u64 {
            self.raw.get()
        }
        /*
        pub fn as_datetime_utc(&self) -> DateTime<Utc> {
            DateTime::<Utc>::from_timestamp_nanos(self.as_u64() as i64)
        }
        pub fn breakdown(&self) -> NanoUnixTimeBreakdown {
            let u = self.as_u64();
            NanoUnixTimeBreakdown {
                tv_sec: (u / GIGA) as u32,
                tv_nsec: (u % GIGA) as u32,
            }
        }
         */
    }

    impl From<u64> for NanoUnixTime {
        fn from(raw: u64) -> Self {
            Self{raw: NonZeroU64::new(raw).expect("zero timestamp")}
        }
    }

    impl Sub<&NanoUnixTime> for NanoUnixTime {
        type Output = i64;
        fn sub(self, rhs: &Self) -> Self::Output {
            self.as_u64() as i64 - rhs.as_u64() as i64
        }
    }
}

mod ring {
    use std::sync::atomic::{AtomicU64, Ordering};

    use heapless::spsc::{Consumer, Producer, Queue};

    struct Hdr {
        len: u32,
        t_r: AtomicU64,
    }

    #[repr(C)]
    pub struct Layout<const W: usize> {
        h: Hdr,
        b: [u8; W],
    }

    pub struct Ring<const W: usize, const N: usize>
    // where
    //     [(); size_of::<Layout<W>>() * (N - 1)]:,
    {
        a: Queue<usize, N>,
        o: Queue<usize, N>,
        b: Box<[u8]>
        // b: [u8; size_of::<Layout<W>>() * (N - 1)], // heapless::spsc::Queue can hold up to N-1 elems where N is a power of 2
    }

    impl<const W: usize, const N: usize> Ring<W, N>
    // where
    //     [(); size_of::<Layout<W>>() * (N - 1)]:,
    {
        pub fn new() -> Self {
            let mut a = Queue::new();
            for i in 0..N - 1 {
                a.enqueue(i).expect("new: enqueue");
            }
            Self {
                a,
                o: Queue::new(),
                b: vec![0u8; size_of::<Layout<W>>() * (N - 1)].into_boxed_slice(),
            }
        }

        pub fn channels(&mut self) -> (Sender<W, N>, Receiver<W, N>) {
            let (ap, ac) = self.a.split();
            let (op, oc) = self.o.split();
            (
                Sender {
                    a: ac,
                    o: op,
                    b: self.b.as_mut_ptr(),
                },
                Receiver {
                    a: ap,
                    o: oc,
                    b: self.b.as_ptr(),
                },
            )
        }
    }

    pub struct Sender<'ring, const W: usize, const N: usize> {
        a: Consumer<'ring, usize, N>,
        o: Producer<'ring, usize, N>,
        b: *mut u8,
    }

    unsafe impl<'ring, const W: usize, const N: usize> Send for Sender<'ring, W, N> {}

    impl<'ring, const W: usize, const N: usize> Sender<'ring, W, N> {
        pub fn write<'endpoint>(
            &'endpoint mut self,
        ) -> Option<raii::Sending<'ring, 'endpoint, W, N>> {
            match self.a.peek() {
                Some(&i) => Some(raii::Sending { x: self, i }),
                None => None,
            }
        }

        fn finish_write(&mut self) {
            let i = unsafe { self.a.dequeue_unchecked() };
            self.o.enqueue(i).expect("finish: enqueue");
        }
    }

    pub struct Receiver<'ring, const W: usize, const N: usize> {
        a: Producer<'ring, usize, N>,
        o: Consumer<'ring, usize, N>,
        b: *const u8,
    }

    unsafe impl<'ring, const W: usize, const N: usize> Send for Receiver<'ring, W, N> {}

    impl<'ring, const W: usize, const N: usize> Receiver<'ring, W, N> {
        pub fn read<'endpoint>(
            &'endpoint mut self,
        ) -> Option<raii::Receiving<'ring, 'endpoint, W, N>> {
            match self.o.peek() {
                Some(&i) => Some(raii::Receiving { x: self, i }),
                None => None,
            }
        }

        fn finish_read(&mut self) {
            let i = unsafe { self.o.dequeue_unchecked() };
            self.a.enqueue(i).expect("finish: enqueue");
        }
    }

    pub mod raii {
        use super::*;
        use crate::time::NanoUnixTime;
        use std::ptr;

        pub struct Sending<'ring, 'endpoint, const W: usize, const N: usize>
        where
            'ring: 'endpoint,
        {
            pub(super) x: &'endpoint mut Sender<'ring, W, N>,
            pub(super) i: usize,
        }

        impl<'ring, 'endpoint, const W: usize, const N: usize> Sending<'ring, 'endpoint, W, N> {
            unsafe fn hdr_as_raw_ptr(&self) -> *mut Hdr {
                unsafe { &raw mut (*self.x.b.cast::<Layout<W>>().add(self.i)).h }
            }

            unsafe fn buf_as_raw_slice(&self) -> *mut [u8] {
                unsafe {
                    ptr::slice_from_raw_parts_mut(
                        (&raw mut (*self.x.b.cast::<Layout<W>>().add(self.i)).b).cast(),
                        W,
                    )
                }
            }

            pub fn record_time(&mut self) -> NanoUnixTime {
                let t = NanoUnixTime::now();
                unsafe {
                    let h = &raw mut (*self.x.b.cast::<Layout<W>>().add(self.i)).h;
                    let at = &raw mut (*h).t_r;
                    (&mut (*at)).store(t.as_u64(), Ordering::Relaxed);
                }
                t
            }
        }

        impl<'ring, 'endpoint, const W: usize, const N: usize> AsRef<[u8]>
        for Sending<'ring, 'endpoint, W, N>
        {
            fn as_ref(&self) -> &[u8] {
                unsafe { &*self.buf_as_raw_slice() }
            }
        }

        impl<'ring, 'endpoint, const W: usize, const N: usize> AsMut<[u8]>
        for Sending<'ring, 'endpoint, W, N>
        {
            fn as_mut(&mut self) -> &mut [u8] {
                unsafe { &mut *self.buf_as_raw_slice() }
            }
        }

        impl<'ring, 'endpoint, const W: usize, const N: usize> Drop for Sending<'ring, 'endpoint, W, N> {
            fn drop(&mut self) {
                self.x.finish_write();
            }
        }

        impl<'ring, 'endpoint, const W: usize, const N: usize> AsRef<u32>
        for Sending<'ring, 'endpoint, W, N>
        {
            fn as_ref(&self) -> &u32 {
                unsafe { &*(&raw const (*self.hdr_as_raw_ptr()).len) }
            }
        }

        impl<'ring, 'endpoint, const W: usize, const N: usize> AsMut<u32>
        for Sending<'ring, 'endpoint, W, N>
        {
            fn as_mut(&mut self) -> &mut u32 {
                unsafe { &mut *(&raw mut (*self.hdr_as_raw_ptr()).len) }
            }
        }

        pub struct Receiving<'ring, 'endpoint, const W: usize, const N: usize>
        where
            'ring: 'endpoint,
        {
            pub(super) x: &'endpoint mut Receiver<'ring, W, N>,
            pub(super) i: usize,
        }

        impl<'ring, 'endpoint, const W: usize, const N: usize> Receiving<'ring, 'endpoint, W, N> {
            pub fn timestamp(&self) -> NanoUnixTime {
                let u64 =
                    unsafe {
                        (&*(&raw const (*(&raw const (*self.x.b.cast::<Layout<W>>().add(self.i)).h)).t_r)).load(Ordering::Acquire)
                    };
                NanoUnixTime::from(u64)
            }
        }

        impl<'ring, 'endpoint, const W: usize, const N: usize> AsRef<[u8]>
        for Receiving<'ring, 'endpoint, W, N>
        {
            fn as_ref(&self) -> &[u8] {
                unsafe {
                    let len =
                        (&raw const (*(&raw const (*self.x.b.cast::<Layout<W>>().add(self.i)).h))
                            .len)
                            .read();
                    &*ptr::slice_from_raw_parts(
                        (&raw const (*self.x.b.cast::<Layout<W>>().add(self.i)).b).cast::<u8>(),
                        len as usize,
                    )
                }
            }
        }

        impl<'ring, 'endpoint, const W: usize, const N: usize> Drop for Receiving<'ring, 'endpoint, W, N> {
            fn drop(&mut self) {
                self.x.finish_read();
            }
        }
    }
}


/// Low quality but super fast Pseudo Random Number Generator of LCG type
/// https://en.wikipedia.org/wiki/Linear_congruential_generator#Parameters_in_common_use
struct RngMMIX(u64);

impl RngMMIX {
    fn new(real_rng: &mut rand::rngs::ThreadRng) -> Self {
        Self(real_rng.next_u64())
    }

    fn logic_1round(x: u64) -> u64 {
        x.wrapping_mul(6364136223846793005_u64).wrapping_add(1442695040888963407_u64)
    }
    fn next_u64(&mut self) -> u64 {
        let new = Self::logic_1round(self.0);
        self.0 = new;
        new
    }

    fn next_small(&mut self) -> u32 {
        let r = self.next_u64();
        1.max((r & 0xFF) as u32) << 16  // between 2**16 .. 2**24
    }
}



fn producer<const W: usize, const N: usize>(
    cpu_core: usize,
    mut i: u64,
    rep_pp: u32,
    sym: &str,
    report: Arc<spin::Mutex<ReportState>>,
    mut s: ring::Sender<W, N>,
    mut r: ring::Receiver<W, N>,
) {
    info!("producer {sym}: core affinity {cpu_core}");
    if ! core_affinity::set_for_current(CoreId{id: cpu_core}) {
        let msg = "failed to set CPU affinity";
        if env::consts::OS == "linux" {
            panic!("{msg}");
        } else {
            warn!("{msg}");
        }
    }

    {
        let mut report = report.lock();
        report.sym.clear();
        report.sym.push_str(sym);
    }

    let mut rng = RngMMIX::new(&mut rng());

    let mut k = 0_u32;
    let mut k_nxt = rng.next_small();

    let mut last_write_time: Option<time::NanoUnixTime> = None;
    let mut mock_recv_buf = [0u8; 2000];

    'outer: while ! gl::is_interrupted() {
        while let Some(last_write_time_data) = last_write_time {
            debug!("producer: last_write_time {}", last_write_time_data.as_u64());
            for retry_read_reply in 0.. {
                if let Some(input) = r.read() {
                    let (i_pp, len, t_latest_recv) =
                        {
                            let input = input;
                            let input_bytes = input.as_ref();
                            let i_pp = u32::from_le_bytes(input_bytes[..4].try_into().expect("should be larger than 4 B"));
                            let len = input_bytes.len() - 4;
                            mock_recv_buf[..len].copy_from_slice(&input_bytes[4..]);
                            (i_pp, len, input.timestamp())
                        };
                    debug!("producer received pong back: i_pp: {i_pp}, len: {len}");

                    if i_pp == rep_pp - 1 {
                        let dt = t_latest_recv - &last_write_time_data;
                        let mut report = report.lock();
                        debug!("reporting latency: {dt} ns");
                        report.add(dt);
                        last_write_time = None;
                    } else {
                        for retry_write_nxt in 0.. {
                            if let Some(mut output) = s.write() {
                                let mut out_buf = Cursor::<&mut [u8]>::new(output.as_mut());
                                out_buf.write(&(i_pp + 1).to_le_bytes()).expect("increment ping pong counter");
                                out_buf.write_all(&mock_recv_buf[..len]).expect("copying mock json");
                                *output.as_mut() = out_buf.position() as u32;
                                output.record_time();
                                break
                            } else {
                                assert!(retry_write_nxt < 100_000_000_u64,
                                        "failed to get a buffer for the next write, {retry_write_nxt} times");
                            }
                            if gl::is_interrupted() {
                                break 'outer
                            }
                        }

                    }
                    break
                } else {
                    assert!(retry_read_reply < 100_000_000_u64,
                            "reply timeout, {:.3} sec",
                            1e-9 * (time::NanoUnixTime::now() - &last_write_time_data) as f32);
                }
                if gl::is_interrupted() {
                    break 'outer
                }
            }
        }

        debug!("producer: about to publish something...");
        'writing_loop: while ! gl::is_interrupted() {
            if k < k_nxt {
                k += 1;
                continue
            }
            k = 0;
            k_nxt = rng.next_small();

            debug!("producer: publishing something...");
            let mock_data_p = 1e-3 * rng.next_small() as f32;
            let mock_data_q = 1e-3 * rng.next_small() as f32;
            let mock_seq_no = i;
            i = i.wrapping_add(1);

            for retry_write_reservation in 0.. {
                if let Some(mut output) = s.write() {
                    debug!("producer: reserved output buffer");
                    let mut out_buf = Cursor::<&mut [u8]>::new(output.as_mut());
                    out_buf.write(&0u32.to_le_bytes()).expect("set ping pong counter");
                    write!(out_buf, r#"{{"n": {mock_seq_no}, "p": {mock_data_p}, "q": {mock_data_q}}}"#).expect("mock json");
                    *output.as_mut() = out_buf.position() as u32;
                    last_write_time = Some(output.record_time());
                    break 'writing_loop
                } else {
                    const CRAPPY_BACKPRESSURE_THRESHOLD: u32 = 100_000_000;
                    if CRAPPY_BACKPRESSURE_THRESHOLD < retry_write_reservation {
                        let retry_score = retry_write_reservation - CRAPPY_BACKPRESSURE_THRESHOLD;
                        assert!(retry_score < 100, "ring spsc full");
                        // Mathematically \sum_{i=1}^n i*i == n*(n+1)*(2n+1)/6, when n==100, 338 ms. Repeated calls of sleep prob consume more
                        thread::sleep(Duration::from_micros((retry_score * retry_score).into()))
                    }
                }
                if gl::is_interrupted() {
                    break 'outer;
                }
            }
        }
    }
    info!("producer: interrupted. bye.");
}

struct Chan<'str, 'sender_ring, 'recv_ring, const W: usize, const N: usize> {
    seq_id: u8,
    sym: &'str str,
    r: ring::Receiver<'recv_ring, W, N>,
    s: ring::Sender<'sender_ring, W, N>,
}


fn consumer<const W: usize, const N: usize>(
    chan: &mut [Chan<W, N>],
) -> Result<()> {
    if ! core_affinity::set_for_current(CoreId{id: 0}) {
        let msg = "failed to set CPU affinity";
        if env::consts::OS == "linux" {
            panic!("{msg}");
        } else {
            warn!("{msg}");
        }
    }

    'outer: loop {
        for Chan{seq_id, sym, r, s} in chan.iter_mut() {
            trace!("consumer: reading from {sym}...");
            for consecutive_read_loop in 0.. {
                match r.read() {
                    Some(input) => {
                        debug!("consumer: received an input for {sym}");
                        if log_enabled!(Level::Debug) {
                            let js = std::str::from_utf8(&input.as_ref()[4..]).expect("input data should be JSON in UTF-8");
                            debug!("consumer: received an input for {sym}: `{js}`");
                        }
                        for retry_write_reservation in 0.. {
                            if let Some(mut output) = s.write() {
                                debug!("consumer: reserved an output buffer for {sym}");
                                let mut write_buf: &mut [u8] = output.as_mut();
                                let input_data = input.as_ref();
                                write_buf.write(input_data).expect("in and out buffers should have the same capacity");
                                *output.as_mut() = input_data.len() as u32;
                                output.record_time();
                                break
                            } else {
                                assert!(retry_write_reservation < 100_000_000);
                            }
                            if gl::is_interrupted() {
                                break 'outer
                            }
                        }
                    },
                    None => {
                        if 0 < consecutive_read_loop {
                            debug!("consumer: there were {consecutive_read_loop} reads")
                        }
                        break
                    }
                }

                if gl::is_interrupted() {
                    break 'outer
                }
            }
        }
        if gl::is_interrupted() {
            break
        }
    }

    info!("consumer: interrupted. bye.");
    Ok(())
}

fn report_printer(mut reports: Vec<Arc<spin::Mutex<ReportState>>>) {
    'outer: loop {
        for rep in reports.iter_mut() {
            if let Some(rep) = rep.try_lock() {
                debug!("report: lock acquired for {}", rep.sym);
                if rep.new.load(Ordering::Acquire) {
                    rep.new.store(false, Ordering::Relaxed);
                    info!("symbol {}: {}", rep.sym, rep.hs);
                }
            }
        }
        for _ in 0..10 {
            if gl::is_interrupted() {
                break 'outer
            }
            thread::sleep(Duration::from_secs_f32(0.1));
        }
    }
    info!("report_printer: interrupted. bye.")
}

struct WorkUnit {
    seq_id: u8,
    sym: String,
    i0: u64,
    req: ring::Ring<2048, 1024>,
    rsp: ring::Ring<2048, 1024>,  // just for experimenting
}

impl WorkUnit {
    fn get_random_ish(seq_id: u8, sym: &str) -> u64 {
        let mut h = DefaultHasher::new();
        h.write_u8(seq_id);
        h.write(sym.as_bytes());
        h.finish() & ((1 << 60) - 1)
    }

    fn new(seq_id: u8, sym: &str) -> Self {
        let i0 = Self::get_random_ish(seq_id, sym);
        Self {
            seq_id, sym: sym.to_owned(), i0,
            req: ring::Ring::<2048, 1024>::new(),
            rsp: ring::Ring::<2048, 1024>::new(),
        }
    }
}

struct LatencyHistogramElem {
    ns: u32,
    c: u64,
}

struct LatencyHistogram {
    v: Vec<LatencyHistogramElem>
}

impl LatencyHistogram {
    fn new() -> Self {
        let mut v = Vec::with_capacity(5 * 8);
        for i in 0..=8 {
            for m in [1, 2, 3, 5, 7] {  // 1 ns, 2 ns, ... 5e8 ns = 0.5 s, 7e8 ns == 0.7 s
                v.push(LatencyHistogramElem{ns: m * 10u32.pow(i), c: 0})
            }
        }
        v.push(LatencyHistogramElem{ns: u32::MAX, c: 0});
        Self{v}
    }

    fn add(&mut self, ns: i64) {
        if ns < 0 {
            self.v[0].c += 1;
        } else {
            let ns = ns as u64;
            for e in self.v.iter_mut() {
                if ns < u64::from(e.ns) || e.ns == u32::MAX {
                    e.c += 1;
                    break
                }
            }
        }
    }
}

impl fmt::Display for LatencyHistogram {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.v.iter().enumerate().fold(None, |acc, (i, e)| {
            if 0 < e.c {
                if let Some((i_min, _)) = acc {
                    Some((i_min, i))
                } else {
                    Some((i, i))
                }
            } else {
                acc
            }
        }) {
            Some((min, max)) => {
                let mut first = true;
                for e in self.v[min..=max].iter() {
                    if first {
                        first = false;
                    } else {
                        writeln!(f)?;
                    }
                    write!(f, "{} ns: {}", e.ns, e.c)?;
                }
                Ok(())
            }
            None => write!(f, "(empty)")
        }
    }
}

struct ReportState {
    sym: String,
    hs: LatencyHistogram,
    new: AtomicBool,
}

impl ReportState {
    fn new(sym: &str) -> Self {
        Self{sym: sym.to_owned(), hs: LatencyHistogram::new(), new: AtomicBool::new(false)}
    }

    fn add(&mut self, ns: i64) {
        self.hs.add(ns);
        self.new.store(true, Ordering::Release);
    }
}


#[derive(Debug, Parser)]
pub struct MultiRingBuffer {
    #[clap(short, long, default_value="0", num_args=0..1, default_missing_value="1")]
    verbose: u8,

    #[clap(short, long, default_value="1")]
    rep_pp: u32,
}

impl MultiRingBuffer {
    fn run(&self) -> Result<()> {
        info!("starting {self:?}");

        let mut works = [
            WorkUnit::new(0, "BTC-USDT"),
            WorkUnit::new(1, "ETH-USDT"),
            WorkUnit::new(2, "SOL-USDT"),
            WorkUnit::new(3, "XRP-USDT"),
        ];

        let reports =
            works.iter().map(|w| Arc::new(spin::Mutex::new(ReportState::new(&w.sym)))).collect::<Vec<_>>();

        thread::scope(|sc| {
            let mut chans = works.each_mut().map(|w| {
                info!("work unit #{} for {}", w.seq_id, w.sym);
                let (req_snd, req_rcv) = w.req.channels();
                let (rsp_snd, rsp_rcv) = w.rsp.channels();
                let cpu_id = usize::from(w.seq_id + 1);
                let i0 = w.i0;
                let sym = &w.sym;
                let rep = Arc::clone(reports.iter().find(|w| &w.lock().sym == sym).expect("single element must match"));
                sc.spawn(move || producer(cpu_id, i0, self.rep_pp, sym, rep, req_snd, rsp_rcv));
                Chan{
                    seq_id: w.seq_id,
                    sym: &w.sym,
                    r: req_rcv,
                    s: rsp_snd,
                }
            });
            sc.spawn(move || report_printer(reports));
            consumer(&mut chans).unwrap()
        });
        Ok(())
    }
}

fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(if env::args().any(|e| e == "-v") { LevelFilter::Debug } else { LevelFilter::Info })
        .format_file(true).format_line_number(true)
        .format_module_path(true)
        .format_timestamp_micros()
        .init();
    info!("starting");

    ctrlc::set_handler(gl::interrupt)?;

    MultiRingBuffer::parse().run()
}