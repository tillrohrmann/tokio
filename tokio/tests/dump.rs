#![cfg(all(
    tokio_unstable,
    feature = "taskdump",
    target_os = "linux",
    any(
        target_arch = "aarch64",
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "s390x"
    )
))]

use std::hint::black_box;
use tokio::runtime::{self, Handle};

#[inline(never)]
async fn a() {
    black_box(b()).await
}

#[inline(never)]
async fn b() {
    black_box(c()).await
}

#[inline(never)]
async fn c() {
    loop {
        black_box(tokio::task::yield_now()).await
    }
}

#[test]
fn current_thread() {
    let rt = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    async fn dump() {
        let handle = Handle::current();
        let dump = handle.dump().await;

        let tasks: Vec<_> = dump.tasks().iter().collect();

        assert_eq!(tasks.len(), 3);

        for task in tasks {
            let id = task.id();
            let trace = task.trace().to_string();
            eprintln!("\n\n{id}:\n{trace}\n\n");
            assert!(trace.contains("dump::a"));
            assert!(trace.contains("dump::b"));
            assert!(trace.contains("dump::c"));
            assert!(trace.contains("tokio::task::yield_now"));
        }
    }

    rt.block_on(async {
        tokio::select!(
            biased;
            _ = tokio::spawn(a()) => {},
            _ = tokio::spawn(a()) => {},
            _ = tokio::spawn(a()) => {},
            _ = dump() => {},
        );
    });
}

/// Regression tests for a `RefCell already borrowed` panic in the
/// `current_thread` scheduler.
///
/// `Handle::dump` used to hold a mutable borrow of the scheduler core while
/// tracing re-polled the live tasks. A task that woke itself or another task
/// while being polled then re-entered `Schedule::schedule`, which borrows the
/// core again, and panicked.
mod wake_during_trace {
    use super::*;

    use std::future::Future;
    use std::sync::{Arc, Mutex};
    use std::task::{Poll, Waker};

    /// A task that wakes *itself* while being traced.
    ///
    /// The wake is deferred to the end of the poll, so the panic used to be
    /// raised outside the task's panic guard and escape through `Handle::dump`
    /// into whoever asked for the dump. The panic is the failure signal here;
    /// there is nothing left to assert afterwards.
    #[test]
    fn self_wake() {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let task = tokio::spawn(std::future::poll_fn(|cx| {
                if Handle::is_tracing() {
                    cx.waker().wake_by_ref();
                }
                Poll::<()>::Pending
            }));

            tokio::task::yield_now().await;
            let _dump = Handle::current().dump().await;

            task.abort();
        });
    }

    /// Two tasks that wake *each other* while being traced.
    ///
    /// Here the wake happens inside the traced task's own `poll`, so the panic
    /// used to be caught by the task's panic guard: the dump appeared to
    /// succeed while silently killing the task. The assertion below is the only
    /// symptom, which is why this case needs one.
    #[test]
    fn cross_wake() {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            fn task(
                me: Arc<Mutex<Option<Waker>>>,
                other: Arc<Mutex<Option<Waker>>>,
            ) -> impl Future<Output = ()> + Send + 'static {
                std::future::poll_fn(move |cx| {
                    *me.lock().unwrap() = Some(cx.waker().clone());

                    if Handle::is_tracing() {
                        if let Some(waker) = other.lock().unwrap().as_ref() {
                            waker.wake_by_ref();
                        }
                    }

                    Poll::<()>::Pending
                })
            }

            let slot_a = Arc::new(Mutex::new(None));
            let slot_b = Arc::new(Mutex::new(None));

            let a = tokio::spawn(task(slot_a.clone(), slot_b.clone()));
            let b = tokio::spawn(task(slot_b, slot_a));

            tokio::task::yield_now().await;
            let _dump = Handle::current().dump().await;

            assert!(
                !a.is_finished() && !b.is_finished(),
                "taking a task dump must not terminate the traced tasks"
            );
            a.abort();
            b.abort();
        });
    }
}

#[test]
fn multi_thread() {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(3)
        .build()
        .unwrap();

    async fn dump() {
        let handle = Handle::current();
        let dump = handle.dump().await;

        let tasks: Vec<_> = dump.tasks().iter().collect();

        assert_eq!(tasks.len(), 3);

        for task in tasks {
            let id = task.id();
            let trace = task.trace().to_string();
            eprintln!("\n\n{id}:\n{trace}\n\n");
            assert!(trace.contains("dump::a"));
            assert!(trace.contains("dump::b"));
            assert!(trace.contains("dump::c"));
            assert!(trace.contains("tokio::task::yield_now"));
        }
    }

    rt.block_on(async {
        tokio::select!(
            biased;
            _ = tokio::spawn(a()) => {},
            _ = tokio::spawn(a()) => {},
            _ = tokio::spawn(a()) => {},
            _ = dump() => {},
        );
    });
}

/// Regression tests for #6035.
///
/// These tests ensure that dumping will not deadlock if a future completes
/// during a trace.
mod future_completes_during_trace {
    use super::*;

    use core::future::{poll_fn, Future};

    /// A future that completes only during a trace.
    fn complete_during_trace() -> impl Future<Output = ()> + Send {
        use std::task::Poll;
        poll_fn(|cx| {
            if Handle::is_tracing() {
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
    }

    #[test]
    fn current_thread() {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        async fn dump() {
            let handle = Handle::current();
            let _dump = handle.dump().await;
        }

        rt.block_on(async {
            let _ = tokio::join!(tokio::spawn(complete_during_trace()), dump());
        });
    }

    #[test]
    fn multi_thread() {
        let rt = runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        async fn dump() {
            let handle = Handle::current();
            let _dump = handle.dump().await;
            tokio::task::yield_now().await;
        }

        rt.block_on(async {
            let _ = tokio::join!(tokio::spawn(complete_during_trace()), dump());
        });
    }
}

/// Regression test for #6051.
///
/// This test ensures that tasks notified outside of a worker will not be
/// traced, since doing so will un-set their notified bit prior to them being
/// run and panic.
#[test]
fn notified_during_tracing() {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(3)
        .build()
        .unwrap();

    let timeout = async {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    };

    let timer = rt.spawn(async {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_nanos(1)).await;
        }
    });

    let dump = async {
        loop {
            let handle = Handle::current();
            let _dump = handle.dump().await;
        }
    };

    rt.block_on(async {
        tokio::select!(
            biased;
            _ = timeout => {},
            _ = timer => {},
            _ = dump => {},
        );
    });
}
