//! Transaction utilities module.

use std::pin::Pin;
use std::sync::Arc;

use futures::Future;

use tokio::sync::Mutex;

type RollbackStep<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send>>;

pub struct TxState<E> {
    rb_stack: Mutex<Vec<RollbackStep<E>>>,
}

impl<E> TxState<E> {
    fn new() -> Self {
        Self {
            rb_stack: Mutex::new(Vec::new()),
        }
    }

    /// Complete a step in the transaction, pushing its rollback step on the stack.
    pub async fn complete(&self, step: RollbackStep<E>) {
        let mut guard = self.rb_stack.lock().await;
        guard.push(step);
    }

    async fn rollback(&self) -> Result<(), E> {
        let mut guard = self.rb_stack.lock().await;
        for rb_step in std::mem::take(&mut (*guard)) {
            Box::pin(rb_step).await?;
        }
        Ok(())
    }
}

pub async fn try_rollback<F, Fut, Res, E>(func: F) -> Result<Res, E>
where
    F: FnOnce(Arc<TxState<E>>) -> Fut,
    Fut: Future<Output = Result<Res, E>>,
    E: std::fmt::Display,
{
    let rollback_state = Arc::new(TxState::new());
    match func(rollback_state.clone()).await {
        Ok(r) => Ok(r),
        Err(e) => {
            if let Err(rb_err) = rollback_state.rollback().await {
                tracing::warn!("error while rolling back '{e}': {rb_err}");
            }

            // We return the original error.
            Err(e)
        }
    }
}
