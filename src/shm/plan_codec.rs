// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::common::{Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

/// The embedder registers this via [`crate::DistributedExt::with_distributed_user_codec`] so the
/// coordinator's per-stage `plan_proto` encode always succeeds over the shm transport.
///
/// The coordinator serializes every stage subplan before handing it to the channel, but the shm
/// transport ships plans over DSM and its `coordinator_channel` drains and discards that request,
/// so the bytes are never read. The encode still runs, and it fails for nodes DataFusion's proto
/// cannot represent (custom scans, `SortMergeJoinExec`). Sitting behind the built-in
/// `DistributedCodec`, this codec is consulted only for those leftover nodes; it encodes each as
/// empty and refuses to decode, since nothing ever decodes the discarded bytes.
#[derive(Debug, Default)]
pub struct ShmDiscardedPlanCodec;

impl PhysicalExtensionCodec for ShmDiscardedPlanCodec {
    fn try_encode(&self, _node: Arc<dyn ExecutionPlan>, _buf: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        internal_err!(
            "ShmDiscardedPlanCodec never decodes: the shm transport discards the coordinator's \
             plan_proto and delivers plans over DSM"
        )
    }
}
