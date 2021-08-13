use chrono::prelude::*;
use futures::StreamExt;
use k8s_openapi::{
    api::core::v1::{Event as k8Event, ObjectReference},
    apimachinery::pkg::apis::meta::v1::MicroTime,
};
use kube::{
    api::{Api, ListParams, ObjectMeta, Patch, PatchParams, PostParams},
    Client,
    CustomResource,
    ResourceExt,
};
use kube_runtime::{
    controller::{Context, Controller, ReconcilerAction},
    finalizer::{finalizer, Event},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::Snafu;
use std::{collections::HashMap, ops::Deref, sync::Arc, time::Duration};
use tracing::{debug, error, info, trace, warn};
pub mod mayastor {
    tonic::include_proto!("mayastor");
}

use mayastor::{
    mayastor_client::MayastorClient,
    CreatePoolRequest,
    DestroyPoolRequest,
    ListBlockDevicesRequest,
    Null,
};

/// Our for Pool spec
#[derive(
    CustomResource,
    Serialize,
    Deserialize,
    Default,
    Debug,
    PartialEq,
    Clone,
    JsonSchema,
)]
#[kube(
    group = "openebs.io",
    version = "v1alpha1",
    kind = "MayastorPool",
    plural = "mayastorpools",
    // The name of the struct that gets created that represents a resource
    namespaced,
    status = "MayastorPoolStatus",
    derive = "PartialEq",
    derive = "Default",
    shortname = "msp"
)]

/// The pool spec which contains the paramaters we consult when creating the
/// pool
pub struct MayastorPoolSpec {
    /// The node the pool is placed on
    node: String,
    /// The disk device the pool is located on
    disks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[non_exhaustive]
pub enum PoolState {
    Creating,
    Created,
    Online,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
/// Status of the pool which is driven and changed but the controller loop
pub struct MayastorPoolStatus {
    /// the state of the pool
    state: PoolState,
    /// used number of bytes
    used: Option<u64>,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to reconcile CRD {}", name))]
    ReconcileError { name: String },
    #[snafu(display(
        "To many reconciliation loops ({}) without forward progress",
        timeout
    ))]
    Duplicate { timeout: u64 },

    #[snafu(display("Failed to connect to node {}", source))]
    RpcError { source: tonic::transport::Error },

    #[snafu(display("gRPC call failed {}", source))]
    RpcStatus { source: tonic::Status },
    #[snafu(display("CRD spec contains invalid values {}", value))]
    SpecError { value: String },
    #[snafu(display("Kubernetes error: {}", source))]
    Kube { source: kube::Error },
}

impl From<tonic::transport::Error> for Error {
    fn from(source: tonic::transport::Error) -> Self {
        Error::RpcError {
            source,
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(source: tonic::Status) -> Self {
        Error::RpcStatus {
            source,
        }
    }
}

impl ToString for PoolState {
    fn to_string(&self) -> String {
        match &*self {
            PoolState::Creating => "Creating",
            PoolState::Created => "Created",
            PoolState::Online => "Online",
            PoolState::Error => "Error",
        }
        .to_string()
    }
}

impl From<PoolState> for String {
    fn from(p: PoolState) -> Self {
        p.to_string()
    }
}

/// Additional per resource context during the runtime; it is volatile
#[derive(Clone)]
pub struct ResourceContext {
    /// The latest CRD known to us
    inner: MayastorPool,
    /// Counter that keeps track of how many times the reconcile loop has run
    /// within the current state
    num_retries: u64,
    /// Reference to the operator context
    ctx: Arc<OperatorContext>,
}

impl Deref for ResourceContext {
    type Target = MayastorPool;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Data we want access to in error/reconcile calls
pub struct OperatorContext {
    /// Reference to our k8s client
    client: Client,
    /// Hashtable of name and the full last seen CRD
    inventory: tokio::sync::RwLock<HashMap<String, ResourceContext>>,
}

impl OperatorContext {
    /// Upsert the potential new CRD into the operator context. If an existing
    /// resource with the same name is present, the old resource is
    /// returned.
    pub async fn upsert(
        &self,
        ctx: Arc<OperatorContext>,
        msp: MayastorPool,
    ) -> ResourceContext {
        let resource = ResourceContext {
            inner: msp,
            num_retries: 0,
            ctx,
        };

        let mut i = self.inventory.write().await;
        debug!(count = ?i.keys().count(), "current number of CRDS");

        match i.get_mut(&resource.name()) {
            Some(p) => {
                if p.resource_version() == resource.resource_version() {
                    if matches!(
                        resource.status,
                        Some(MayastorPoolStatus {
                            state: PoolState::Online,
                            ..
                        })
                    ) {
                        return p.clone();
                    }

                    warn!(status =? resource.status, "duplicate event or long running operation");

                    // The status should be the same here as well
                    assert_eq!(&p.status, &resource.status);
                    p.num_retries += 1;
                    return p.clone();
                }

                // Its a new resource version which means we will swap it out
                // to reset the counter.
                assert!(p.resource_version() < resource.resource_version());
                let p = i
                    .insert(resource.name(), resource.clone())
                    .expect("existing resource should be present");
                info!(name = ?p.name(), "new resource_version inserted");
                resource
            }

            None => {
                let p = i.insert(resource.name(), resource.clone());
                assert!(p.is_none());
                resource
            }
        }
    }

    pub async fn remove(&self, name: String) -> Option<ResourceContext> {
        let mut i = self.inventory.write().await;
        let removed = i.remove(&name);
        if let Some(removed) = removed {
            info!(name =? removed.name(), "removed from inventory");
            return Some(removed);
        }
        None
    }
}

impl ResourceContext {
    /// Called when putting our finalizer on top of the resource.
    pub async fn put_finalizer(
        _msp: MayastorPool,
    ) -> Result<ReconcilerAction, Error> {
        Ok(ReconcilerAction {
            requeue_after: None,
        })
    }

    /// Our notification that we should remove the pool and then the finalizer
    pub async fn delete_finalizer(
        resource: ResourceContext,
    ) -> Result<ReconcilerAction, Error> {
        let ctx = resource.ctx.clone();
        resource.delete_pool().await?;
        ctx.remove(resource.name()).await;
        Ok(ReconcilerAction {
            requeue_after: None,
        })
    }

    /// Clone the inner value of this resource
    fn inner(&self) -> MayastorPool {
        self.inner.clone()
    }

    /// Construct an API handle for the resource
    fn api(&self) -> Api<MayastorPool> {
        Api::namespaced(self.ctx.client.clone(), &self.namespace().unwrap())
    }

    /// Patch the given MSP status to the state provided. When not online the
    /// size should be asumed to be zero.
    async fn patch_status(
        &self,
        status: MayastorPoolStatus,
    ) -> Result<MayastorPool, Error> {
        let status = json!({ "status": status });

        let ps = PatchParams::apply("Mayastor pool operator");

        let o = self
            .api()
            .patch_status(&self.name(), &ps, &Patch::Merge(&status))
            .await
            .map_err(|source| Error::Kube {
                source,
            })?;

        info!(name = ?o.name(), old = ?self.status, new =?o.status, "status changed");

        Ok(o)
    }
    /// Create a pool when there is no status found. When no status is found for
    /// this resource it implies that it does not exist yet and so we create
    /// it. We set the state of the of the object to Creating, such that we
    /// can track the its progress
    async fn start(&self) -> Result<ReconcilerAction, Error> {
        let status = MayastorPoolStatus {
            state: PoolState::Creating,
            used: Some(0),
        };

        let _ = self.patch_status(status).await?;

        Ok(ReconcilerAction {
            requeue_after: None,
        })
    }

    /// Mark the resource as errorerd which is its final state. A pool in the
    /// error state will not be deleted.
    async fn mark_error(&self) {
        let o = self
            .patch_status(MayastorPoolStatus {
                state: PoolState::Error,
                used: Some(0),
            })
            .await;

        trace!(?o);
    }

    /// Delete the pool from the mayastor instance
    async fn delete_pool(&self) -> Result<ReconcilerAction, Error> {
        let mut handle = self.grpc_handle().await?;

        if matches!(
            self.status,
            Some(MayastorPoolStatus {
                state: PoolState::Error,
                ..
            })
        ) {
            return Ok(ReconcilerAction {
                requeue_after: None,
            });
        }

        if handle
            .list_pools(Null {})
            .await?
            .into_inner()
            .pools
            .iter()
            .any(|p| p.name == self.name())
        {
            handle
                .destroy_pool(DestroyPoolRequest {
                    name: self.name(),
                })
                .await?;

            self.k8s_notify(
                "Destroyed pool",
                "Destroy",
                "The pool has been destroyed",
                "Normal",
            )
            .await;
        }

        Ok(ReconcilerAction {
            requeue_after: None,
        })
    }

    /// Online the pool which is no-op from the dataplane standpoint of view
    async fn online_pool(self) -> Result<ReconcilerAction, Error> {
        let mut handle = self.grpc_handle().await?;

        if handle
            .list_pools(Null {})
            .await?
            .into_inner()
            .pools
            .iter()
            .any(|p| p.name == self.name() && p.state == 1)
        {
            let _ = self
                .patch_status(MayastorPoolStatus {
                    state: PoolState::Online,
                    used: Some(self.status.as_ref().unwrap().used.unwrap()),
                })
                .await?;

            self.k8s_notify(
                "Online pool",
                "Online",
                "Pool online and ready to roll!",
                "Normal",
            )
            .await;
        }

        Ok(ReconcilerAction {
            requeue_after: None,
        })
    }

    /// Create or import the pool, on failure try again. When we reach max error
    /// count we fail the whole thing.
    pub async fn create_or_import(self) -> Result<ReconcilerAction, Error> {
        if self.num_retries == 10 {
            self.k8s_notify(
                "Failing pool creation",
                "Creating",
                &format!("Retry attempts ({}) exceeded", self.num_retries),
                "Error",
            )
            .await;
            self.mark_error().await;
            return Err(Error::ReconcileError {
                name: self.name(),
            });
        }

        let mut handle = self.grpc_handle().await?;

        if handle
            .list_block_devices(ListBlockDevicesRequest {
                all: true,
            })
            .await?
            .into_inner()
            .devices
            .iter()
            .find(|b| b.devname == self.spec.disks[0])
            .is_none()
        {
            self.k8s_notify(
                "Create or import",
                "Missing",
                "The block device(s) can not be found on the host",
                "Warn",
            )
            .await;

            return Err(Error::SpecError {
                value: self.spec.disks[0].clone(),
            });
        }

        let pool = handle
            .create_pool(CreatePoolRequest {
                name: self.name(),
                disks: self.spec.disks.clone(),
            })
            .await?
            .into_inner();

        self.k8s_notify(
            "Create or Import",
            "Created",
            &format!(
                "Created pool {:?} capacity: {:?}",
                pool.name, pool.capacity
            ),
            "Normal",
        )
        .await;

        let new = self
            .patch_status(MayastorPoolStatus {
                state: PoolState::Created,
                used: Some(pool.capacity),
            })
            .await?;

        trace!(?new);

        // We are done creating the pool, we patched to created which triggers a
        // new loop. Any error in the loop will call our error handler where we
        // decide what to do
        Ok(ReconcilerAction {
            requeue_after: None,
        })
    }

    async fn is_missing(&self) -> Result<ReconcilerAction, Error> {
        self.patch_status(MayastorPoolStatus {
            state: PoolState::Creating,
            used: None,
        })
        .await?;
        return Ok(ReconcilerAction {
            requeue_after: None,
        });
    }

    /// When the pool is placed online, we keep checking it
    async fn pool_check(&self) -> Result<ReconcilerAction, Error> {
        if !self
            .grpc_handle()
            .await?
            .list_pools(Null {})
            .await?
            .into_inner()
            .pools
            .iter()
            .any(|p| p.name == self.name())
        {
            info!(pool = ?self.name(), "offline");

            let _ = self
                .patch_status(MayastorPoolStatus {
                    state: PoolState::Creating,
                    used: None,
                })
                .await;

            self.k8s_notify(
                "Offline",
                "Check",
                "The pool can not be located scheduling import operation",
                "Warning",
            )
            .await;

            return self.is_missing().await;
        }

        Ok(ReconcilerAction {
            requeue_after: Some(Duration::from_secs(5)),
        })
    }

    /// Post an event, typically these events are used to indicate that
    /// something happend. They should not be used to "log" generic
    /// information. Events are GC-ed by k8s automatically.
    ///
    /// action:
    ///     What action was taken/failed regarding to the Regarding object.
    /// reason:
    ///     This should be a short, machine understandable string that gives the
    ///     reason for the transition into the object's current status.
    /// message:
    ///     A human-readable description of the status of this operation.
    /// type_:
    ///     Type of this event (Normal, Warning), new types could be added in
    ///     the  future

    async fn k8s_notify(
        &self,
        action: &str,
        reason: &str,
        message: &str,
        type_: &str,
    ) {
        let client = self.ctx.client.clone();
        let e: Api<k8Event> = Api::namespaced(client, "mayastor");
        let pp = PostParams::default();
        let time = Utc::now();

        let metadata = ObjectMeta {
            // the name must be unique for all events we post
            generate_name: Some(format!(
                "{}.{:x}",
                self.name(),
                time.timestamp()
            )),
            namespace: self.namespace(),
            ..Default::default()
        };

        let _ = e
            .create(
                &pp,
                &k8Event {
                    //last_timestamp: Some(time2),
                    event_time: Some(MicroTime(time)),
                    involved_object: ObjectReference {
                        api_version: Some(self.api_version.clone()),
                        field_path: None,
                        kind: Some(self.kind.clone()),
                        name: Some(self.name()),
                        namespace: self.namespace(),
                        resource_version: self.resource_version(),
                        uid: Some(self.name()),
                    },
                    action: Some(action.into()),
                    reason: Some(reason.into()),
                    type_: Some(type_.into()),
                    metadata,
                    reporting_component: Some("MSP-operator".into()),
                    // should be MY_POD_NAME
                    reporting_instance: Some("MSP-operator".into()),
                    message: Some(message.into()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| error!(?e));
    }

    /// Callback hooks for the finalizers
    async fn finalizer(&self) -> Result<ReconcilerAction, Error> {
        let _ = finalizer(
            &self.api(),
            "io.mayastor.pool/cleanup",
            self.inner(),
            |event| async move {
                match event {
                    Event::Apply(msp) => Self::put_finalizer(msp).await,
                    Event::Cleanup(_msp) => {
                        Self::delete_finalizer(self.clone()).await
                    }
                }
            },
        )
        .await
        .map_err(|e| error!(?e));

        Ok(ReconcilerAction {
            requeue_after: None,
        })
    }

    /// Construct a grpc handle to the given instance that is running mayastor
    async fn grpc_handle(
        &self,
    ) -> Result<MayastorClient<tonic::transport::Channel>, Error> {
        let auth = format!("{}:10124", self.inner.spec.node);

        let uri = tonic::transport::Uri::builder()
            .scheme("http")
            .authority(auth.as_str())
            .path_and_query("/")
            .build()
            .map_err(|e| Error::SpecError {
                value: e.to_string(),
            })?;

        let channel = tonic::transport::Channel::builder(uri)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(3))
            .keep_alive_while_idle(true)
            .keep_alive_timeout(Duration::from_secs(5))
            .tcp_keepalive(Some(Duration::from_secs(1)));

        Ok(MayastorClient::connect(channel).await?)
    }
}

/// Determine what we want to do when dealing with errors from the
/// reconciliation loop
fn error_policy(
    error: &Error,
    _ctx: Context<OperatorContext>,
) -> ReconcilerAction {
    let scaler = match error {
        Error::Duplicate {
            timeout,
        } => *timeout,

        Error::ReconcileError {
            name,
        } => {
            error!("to many errors for {} reconciliation aborted", name);
            return ReconcilerAction {
                requeue_after: None,
            };
        }

        Error::RpcError {
            ..
        }
        | Error::RpcStatus {
            ..
        } => 5,
        _ => 3,
    };

    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(scaler)),
    }
}

/// The main work horse
async fn reconcile(
    msp: MayastorPool,
    ctx: Context<OperatorContext>,
) -> Result<ReconcilerAction, Error> {
    let ctx = ctx.into_inner();
    let msp = ctx.upsert(ctx.clone(), msp).await;

    let _ = msp.finalizer().await;

    match msp.status {
        Some(MayastorPoolStatus {
            state: PoolState::Creating,
            ..
        }) => {
            return msp.create_or_import().await;
        }

        Some(MayastorPoolStatus {
            state: PoolState::Created,
            ..
        }) => {
            return msp.online_pool().await;
        }

        Some(MayastorPoolStatus {
            state: PoolState::Online,
            ..
        }) => {
            return msp.pool_check().await;
        }

        Some(MayastorPoolStatus {
            state: PoolState::Error,
            ..
        }) => {
            error!(pool = ?msp.name(), "entered Error as final state");
            Err(Error::ReconcileError {
                name: msp.name(),
            })
        }

        // We use this state to indicate its a new CRD howoever, we could (and
        // perhaps should) use the finalizer callback.
        None => return msp.start().await,
    }
}

async fn pool_controller() -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let namespace = String::from("mayastor");
    let msp: Api<MayastorPool> = Api::namespaced(client.clone(), &namespace);
    let lp = ListParams::default();

    let context = Context::new(OperatorContext {
        client,
        inventory: tokio::sync::RwLock::new(HashMap::new()),
    });

    Controller::new(msp, lp)
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok(_o) => {}
                Err(_e) => {}
            }
        })
        .await;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    pool_controller().await?;
    Ok(())
}
