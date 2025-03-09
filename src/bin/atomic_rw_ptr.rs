use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

const N: u32 = 100;

fn receiver (buf_ptr: usize) {
    unsafe {
        let buf_ptr: *const u64 = buf_ptr as *const u64; // this is just for passing a pointer to another thread which isn't usually allowed, not essential
        let buf_ptr: *const AtomicU64 = buf_ptr.cast::<AtomicU64>();
        let ref_atomic: &AtomicU64 = &*buf_ptr;
        let mut val: u64 = ref_atomic.load(Ordering::Acquire);
        for i in 1..N {
            loop {
                let new = (&*buf_ptr).load(Ordering::Acquire);
                if new != val {
                    println!("{val} <- ${new}");
                    val = new;
                    break;
                }
            }
        }
        println!("receiver done");
    }
}

fn main() {
    let mut buf = [0_u8; size_of::<u64>()];
    let buf_ptr = buf.as_mut_ptr() as usize; // this is just for passing a pointer to another thread which isn't usually allowed
    thread::scope(|sc| {
        sc.spawn(|| receiver(buf_ptr));
        thread::sleep(Duration::from_millis(10));
        for i in 1..N {
            buf[3] = i as u8;
            thread::sleep(Duration::from_millis(10));
        }
        println!("sender done");
    });
    println!("bye");
}