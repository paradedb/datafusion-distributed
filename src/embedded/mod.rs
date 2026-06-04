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

//! Embedded shared-memory transport.
//!
//! A non-Flight `WorkerTransport` for co-located execution, where "workers" are tasks or
//! parallel processes sharing one machine and communicating over a shared-memory mesh rather
//! than gRPC. The transport-mechanism pieces (the MPSC ring, framing, routing, cooperative
//! drain) live here as a reusable library; an embedder supplies the two platform primitives via
//! small seams: how to allocate the shared buffer, and how to wake a blocked consumer.
//!
//! The point of hosting it in this crate is testing: the in-process instantiation runs real
//! distributed queries through the transport in this crate's CI, so an upstream rebase that
//! breaks the `WorkerTransport` contract fails here, before any downstream embedder rebuilds.

// These layers are exercised by their own tests; they get wired into the `WorkerTransport` impl
// and an in-process instantiation in a follow-up, which is when they become reachable from the
// rest of the crate.
#[allow(dead_code)]
mod dsm;
#[allow(dead_code)]
mod mesh;
#[allow(dead_code)]
mod mpsc_ring;
#[allow(dead_code)]
mod runtime;
#[allow(dead_code)]
mod transport;
