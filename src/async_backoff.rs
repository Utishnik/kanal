//! Asynchronous counterpart of [`crate::backoff`].
//!
//! The sync channel uses a homemade backoff that spins, yields the OS thread
//! and finally sleeps. Doing any of that from an async poll would block the
//! executor thread, so the async futures drive [`AsyncWait`] instead:
//!
//! 1. a short sequence of immediate yields to the runtime,
//! 2. then a runtime timer with a geometrically increasing sleep.
//!
//! Every step arms its own wakeup (a fresh task wake in the yield phase and a
//! timer future afterwards), so an operation always makes progress even when
//! the counterpart's wakeup cannot be observed (for example because a signal
//! was already popped from the waitlist before the waker could be updated).
//!
//! Sleeps are delegated to the native timer of the selected runtime
//! (`async-tokio`, `async-smol` or `async-compio`). When no runtime feature is
//! enabled, or several are enabled at once (`--all-features`), the runtime
//! agnostic [`futures_timer::Delay`] is used instead.

use core::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(not(loom))]
use branches::likely;
#[cfg(not(loom))]
use core::time::Duration;

/// Number of immediate runtime yields performed before falling back to a
/// timer. This mirrors the yield phase of the sync backoff without ever
/// blocking the executor.
const YIELDS: u32 = 32;
/// First timer-backed sleep, in nanoseconds.
const SLEEP_START_NANOS: u64 = 1 << 10;
/// Upper bound of the timer-backed sleep (about 262 microseconds), matching
/// [`crate::signal`]'s sync backoff.
#[cfg_attr(loom, allow(dead_code))]
const SLEEP_MAX_NANOS: u64 = 1 << 18;

/// Poll-based backoff driver used by the async channel operations.
///
/// The driver is intentionally poll based (rather than an `async fn`):
/// the futures already implement `poll`, and a signal wait completes by
/// observing an atomic state that the counterpart updates without an
/// additional future.
pub(crate) struct AsyncWait {
    /// Remaining immediate yields for the current wait.
    yields: u32,
    /// Sleep duration to use the next time a timer is armed.
    sleep_nanos: u64,
    /// Currently armed timer, if any.
    timer: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl fmt::Debug for AsyncWait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncWait")
            .field("yields", &self.yields)
            .field("sleep_nanos", &self.sleep_nanos)
            .field("sleeping", &self.timer.is_some())
            .finish()
    }
}

impl AsyncWait {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self {
            yields: YIELDS,
            sleep_nanos: SLEEP_START_NANOS,
            timer: None,
        }
    }

    /// Restores the initial backoff profile. Called once an operation
    /// succeeds, so the next contention episode starts with cheap yields
    /// again.
    #[inline(always)]
    pub(crate) fn reset(&mut self) {
        self.yields = YIELDS;
        self.sleep_nanos = SLEEP_START_NANOS;
        // Dropping an armed timer is fine: its only side effect is a wakeup
        // that will be ignored by the executor.
        self.timer = None;
    }

    /// Drives the backoff once.
    ///
    /// Returns [`Poll::Ready`] when the caller should retry the operation, or
    /// [`Poll::Pending`] when the task has been rescheduled and the caller
    /// should return `Pending` to the executor.
    #[cfg(not(loom))]
    #[inline(always)]
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            // A timer is armed: wait for it, then let the caller retry.
            if let Some(timer) = self.timer.as_mut() {
                match timer.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        self.timer = None;
                        return Poll::Ready(());
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Cheap phase: hand the CPU back to the runtime and ask to be
            // polled again immediately.
            if likely(self.yields > 0) {
                self.yields -= 1;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            // Contended for a while: arm a runtime timer and poll it in the
            // same call so its waker is registered before we return Pending.
            let dur = Duration::from_nanos(self.sleep_nanos);
            self.sleep_nanos =
                self.sleep_nanos.saturating_mul(2).min(SLEEP_MAX_NANOS);
            self.timer = Some(sleep_future(dur));
        }
    }

    /// Loom does not model time nor benefit from spinning. The loom variants
    /// of the acquire/wait helpers block with loom's cooperative yield, so
    /// this driver is never exercised there; it exists only to keep the
    /// futures' layout uniform and simply reschedules the task.
    #[cfg(loom)]
    #[allow(dead_code)]
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// Creates a future that completes after `dur` using the selected runtime's
/// native timer, or the runtime agnostic fallback.
#[cfg(not(loom))]
#[inline]
fn sleep_future(dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // Exactly one runtime backend enabled: use its timer.
    #[cfg(all(
        feature = "async-tokio",
        not(feature = "async-smol"),
        not(feature = "async-compio")
    ))]
    return Box::pin(tokio::time::sleep(dur));

    #[cfg(all(
        not(feature = "async-tokio"),
        feature = "async-smol",
        not(feature = "async-compio")
    ))]
    return Box::pin(async move {
        smol::Timer::after(dur).await;
    });

    #[cfg(all(
        not(feature = "async-tokio"),
        not(feature = "async-smol"),
        feature = "async-compio"
    ))]
    return Box::pin(async move {
        // compio's `TimerFuture` borrows runtime state through an `Rc`, so it
        // is `!Send`, while the channel futures must stay `Send`. compio
        // solves this itself with its `SendWrapper` (which the driver and
        // executor use for the same reason), so the native
        // `compio::time::sleep` is stored directly inside the channel future.
        //
        // The wrapper matches compio's thread-per-core affinity rules: the
        // timer is armed, polled and dropped on the runtime thread that polls
        // the channel future, like any other compio timer.
        let timer =
            compio_send_wrapper::SendWrapper::new(compio::time::sleep(dur));
        timer.await;
    });

    // No backend, or an ambiguous combination (e.g. `--all-features`): fall
    // back to the runtime agnostic timer so the future stays `Send` and works
    // under any executor.
    #[cfg(not(any(
        all(
            feature = "async-tokio",
            not(feature = "async-smol"),
            not(feature = "async-compio")
        ),
        all(
            not(feature = "async-tokio"),
            feature = "async-smol",
            not(feature = "async-compio")
        ),
        all(
            not(feature = "async-tokio"),
            not(feature = "async-smol"),
            feature = "async-compio"
        )
    )))]
    return Box::pin(futures_timer::Delay::new(dur));
}

// Exercise the runtime agnostic fallback timer whenever it is the selected
// backend: no runtime feature, several of them (`--all-features`), and any
// other ambiguous combination. The runtime-specific timers are covered by the
// tokio/smol/compio integration suites.
#[cfg(all(
    test,
    feature = "async",
    not(loom),
    not(any(
        all(
            feature = "async-tokio",
            not(feature = "async-smol"),
            not(feature = "async-compio")
        ),
        all(
            not(feature = "async-tokio"),
            feature = "async-smol",
            not(feature = "async-compio")
        ),
        all(
            not(feature = "async-tokio"),
            not(feature = "async-smol"),
            feature = "async-compio"
        )
    ))
))]
mod tests {
    use core::task::{Context, Poll};
    use std::time::Duration;

    use super::{AsyncWait, YIELDS};

    /// The driver must never sleep on the executor thread: the yield phase
    /// only reschedules the task, and the sleep phase completes through the
    /// timer (`futures-timer` in this configuration).
    #[test]
    fn yields_then_completes_through_timer() {
        let mut wait = AsyncWait::new();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        for _ in 0..YIELDS {
            assert!(
                matches!(wait.poll(&mut cx), Poll::Pending),
                "yield phase must only reschedule"
            );
        }

        // Use a long enough timer for the assertion below to be stable.
        wait.sleep_nanos = Duration::from_millis(20).as_nanos() as u64;
        assert!(
            matches!(wait.poll(&mut cx), Poll::Pending),
            "armed timer must suspend the task"
        );

        std::thread::sleep(Duration::from_millis(100));
        assert!(
            matches!(wait.poll(&mut cx), Poll::Ready(())),
            "timer must complete and let the caller retry"
        );
    }
}

// Directly validate the native runtime timers selected by the backend
// features. Integration suites exercise the channel, but rarely force the
// backoff timer, so this covers each backend's `sleep_future` in isolation.
#[cfg(all(test, feature = "async", not(loom)))]
mod timer_tests {
    #[cfg(all(
        feature = "async-tokio",
        not(feature = "async-smol"),
        not(feature = "async-compio")
    ))]
    #[test]
    fn tokio_timer_fires() {
        use core::time::Duration;

        fn assert_send<T: Send>(_: &T) {}

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let timer = super::sleep_future(Duration::from_millis(1));
            assert_send(&timer);
            timer.await;
        });
    }

    #[cfg(all(
        not(feature = "async-tokio"),
        feature = "async-smol",
        not(feature = "async-compio")
    ))]
    #[test]
    fn smol_timer_fires() {
        use core::time::Duration;

        fn assert_send<T: Send>(_: &T) {}

        smol::block_on(async {
            let timer = super::sleep_future(Duration::from_millis(1));
            assert_send(&timer);
            timer.await;
        });
    }

    #[cfg(all(
        not(feature = "async-tokio"),
        not(feature = "async-smol"),
        feature = "async-compio"
    ))]
    #[test]
    fn compio_timer_fires() {
        use core::time::Duration;

        fn assert_send<T: Send>(_: &T) {}

        compio::runtime::Runtime::new().unwrap().block_on(async {
            let timer = super::sleep_future(Duration::from_millis(1));
            assert_send(&timer);
            timer.await;
        });
    }
}
