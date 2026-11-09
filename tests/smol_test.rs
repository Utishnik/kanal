//! Async channel tests running on the smol runtime.
//!
//! These mirror the most important parts of `tests/async_test.rs` and exercise
//! the `async-smol` timer backend of the async backoff. They are only compiled
//! when the `async-smol` feature is enabled (needed for `smol` itself), which
//! also guarantees a compatible timer selection for the smol executor: with
//! `async-smol` on, the timer is either smol's own or the runtime agnostic
//! `futures-timer` fallback (multi-feature setups), never tokio's or compio's
//! (a runtime that the smol executor has no access to).
#![cfg(all(not(loom), feature = "async-smol"))]

use std::collections::VecDeque;

use futures::stream::{FusedStream, StreamExt};
use kanal::{bounded_async, unbounded_async, AsyncReceiver, AsyncSender};

/// Smaller than the tokio/compio suites: this suite is meant to validate the
/// smol backend quickly, not to benchmark throughput.
const MESSAGES: usize = 1_000;
const THREADS: usize = 4;

fn new<T>(cap: Option<usize>) -> (AsyncSender<T>, AsyncReceiver<T>) {
    match cap {
        None => unbounded_async(),
        Some(cap) => bounded_async(cap),
    }
}

fn assert_send<T: Send>(_: T) {}

#[test]
fn futures_are_send() {
    smol::block_on(async {
        let (tx, rx) = unbounded_async::<u8>();
        assert_send(rx.recv());
        assert_send(tx.send(1));
        let mut elements = VecDeque::from([1u8, 2, 3]);
        assert_send(tx.send_many(&mut elements));
    });
}

#[test]
fn mpsc() {
    for cap in [Some(0), Some(1), Some(64), None] {
        smol::block_on(async move {
            let (tx, rx) = new::<usize>(cap);
            let mut tasks = Vec::new();
            for _ in 0..THREADS {
                let tx = tx.clone();
                tasks.push(smol::spawn(async move {
                    for i in 0..MESSAGES / THREADS {
                        tx.send(i).await.unwrap();
                    }
                }));
            }

            for _ in 0..MESSAGES {
                rx.recv().await.unwrap();
            }
            for task in tasks {
                task.await;
            }
        });
    }
}

#[test]
fn mpmc() {
    for cap in [Some(0), Some(64), None] {
        smol::block_on(async move {
            let (tx, rx) = new::<usize>(cap);
            let mut tasks = Vec::new();
            for _ in 0..THREADS {
                let tx = tx.clone();
                tasks.push(smol::spawn(async move {
                    for i in 0..MESSAGES / THREADS {
                        tx.send(i).await.unwrap();
                    }
                }));
            }
            for _ in 0..THREADS {
                let rx = rx.clone();
                tasks.push(smol::spawn(async move {
                    for _ in 0..MESSAGES / THREADS {
                        rx.recv().await.unwrap();
                    }
                }));
            }
            for task in tasks {
                task.await;
            }
        });
    }
}

#[test]
fn spsc() {
    smol::block_on(async {
        let (tx, rx) = new::<usize>(Some(0));
        let producer = smol::spawn(async move {
            for i in 0..MESSAGES {
                tx.send(i).await.unwrap();
            }
        });
        for i in 0..MESSAGES {
            assert_eq!(rx.recv().await.unwrap(), i);
        }
        producer.await;
    });
}

#[test]
fn integrity_rendezvous() {
    smol::block_on(async {
        let (tx, rx) = new::<usize>(Some(0));
        let producer = smol::spawn(async move {
            for i in 0..MESSAGES {
                tx.send(i * 2).await.unwrap();
                tx.send(i * 2 + 1).await.unwrap();
            }
        });
        for i in 0..MESSAGES {
            assert_eq!(rx.recv().await.unwrap(), i * 2);
            assert_eq!(rx.recv().await.unwrap(), i * 2 + 1);
        }
        producer.await;
    });
}

#[test]
fn send_many() {
    for cap in [Some(0), Some(3), Some(64), None] {
        smol::block_on(async move {
            let (tx, rx) = new::<usize>(cap);
            let producer = smol::spawn(async move {
                let mut elements: VecDeque<usize> = (0..MESSAGES).collect();
                tx.send_many(&mut elements).await.unwrap();
                assert!(elements.is_empty());
            });
            for i in 0..MESSAGES {
                assert_eq!(rx.recv().await.unwrap(), i);
            }
            producer.await;
        });
    }
}

#[test]
fn drain_into() {
    smol::block_on(async {
        let (tx, rx) = new::<usize>(Some(8));
        let producer = smol::spawn(async move {
            for i in 0..MESSAGES {
                tx.send(i).await.unwrap();
            }
        });

        let mut all = Vec::new();
        while all.len() < MESSAGES {
            let mut batch = Vec::new();
            let received = rx.drain_into_blocking(&mut batch).await.unwrap();
            assert!(received >= 1);
            assert_eq!(received, batch.len());
            all.extend(batch);
        }
        producer.await;
        assert_eq!(all, (0..MESSAGES).collect::<Vec<_>>());
    });
}

#[test]
fn stream() {
    smol::block_on(async {
        let (tx, rx) = new::<usize>(Some(4));
        let producer = smol::spawn(async move {
            for i in 0..MESSAGES {
                tx.send(i).await.unwrap();
            }
        });

        let mut stream = rx.stream();
        for i in 0..MESSAGES {
            assert_eq!(stream.next().await, Some(i));
        }
        assert_eq!(stream.next().await, None);
        assert!(stream.is_terminated());
        producer.await;
    });
}
