// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::task::{Context, Poll};

use anyhow::anyhow;
use futures::{TryStreamExt, ready};
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::sink::SinkParam;
use risingwave_pb::connector_service::coordinate_response::{
    CommitResponse, StartCoordinationResponse,
};
use risingwave_pb::connector_service::{
    CoordinateResponse, coordinate_request, coordinate_response,
};
use tonic::Status;

use crate::manager::sink_coordination::{SinkCoordinatorResponseSender, SinkWriterRequestStream};

pub(super) struct SinkWriterCoordinationHandle {
    request_stream: SinkWriterRequestStream,
    response_tx: SinkCoordinatorResponseSender,
    param: SinkParam,
    vnode_bitmap: Bitmap,
    prev_epoch: Option<u64>,
}

impl SinkWriterCoordinationHandle {
    pub(super) fn new(
        request_stream: SinkWriterRequestStream,
        response_tx: SinkCoordinatorResponseSender,
        param: SinkParam,
        vnode_bitmap: Bitmap,
    ) -> Self {
        Self {
            request_stream,
            response_tx,
            param,
            vnode_bitmap,
            prev_epoch: None,
        }
    }

    pub(super) fn param(&self) -> &SinkParam {
        &self.param
    }

    pub(super) fn vnode_bitmap(&self) -> &Bitmap {
        &self.vnode_bitmap
    }

    pub(super) fn start(
        &mut self,
        log_store_rewind_start_epoch: Option<u64>,
    ) -> anyhow::Result<()> {
        self.response_tx
            .send(Ok(CoordinateResponse {
                msg: Some(coordinate_response::Msg::StartResponse(
                    StartCoordinationResponse {
                        log_store_rewind_start_epoch,
                    },
                )),
            }))
            .map_err(|_| anyhow!("failed to send the start response"))
    }

    pub(super) fn ack_aligned_initial_epoch(
        &mut self,
        aligned_initial_epoch: u64,
    ) -> anyhow::Result<()> {
        self.response_tx
            .send(Ok(CoordinateResponse {
                msg: Some(coordinate_response::Msg::AlignInitialEpochResponse(
                    aligned_initial_epoch,
                )),
            }))
            .map_err(|_| anyhow!("failed to send the start response"))
    }

    pub(super) fn abort(self, status: Status) {
        let _ = self.response_tx.send(Err(status));
    }

    pub(super) fn ack_commit(&mut self, epoch: u64) -> anyhow::Result<()> {
        self.response_tx
            .send(Ok(CoordinateResponse {
                msg: Some(coordinate_response::Msg::CommitResponse(CommitResponse {
                    epoch,
                })),
            }))
            .map_err(|_| anyhow!("failed to send the commit response for epoch {}", epoch))
    }

    pub(super) fn stop(&mut self) -> anyhow::Result<()> {
        self.response_tx
            .send(Ok(CoordinateResponse {
                msg: Some(coordinate_response::Msg::Stopped(true)),
            }))
            .map_err(|_| anyhow!("failed to send the stopped response"))
    }

    pub(super) fn poll_next_request(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<anyhow::Result<coordinate_request::Msg>> {
        let result = try {
            let request = ready!(self.request_stream.try_poll_next_unpin(cx))
                .ok_or_else(|| anyhow!("end of request stream"))?
                .map_err(anyhow::Error::from)?;
            let request = request.msg.ok_or_else(|| anyhow!("None msg in request"))?;
            match &request {
                coordinate_request::Msg::StartRequest(_)
                | coordinate_request::Msg::Stop(_)
                | coordinate_request::Msg::AlignInitialEpochRequest(_) => {}
                coordinate_request::Msg::CommitRequest(request) => {
                    if let Some(prev_epoch) = self.prev_epoch
                        && request.epoch < prev_epoch
                    {
                        return Poll::Ready(Err(anyhow!(
                            "invalid commit epoch {}, prev_epoch {}",
                            request.epoch,
                            prev_epoch
                        )));
                    }
                    if request.metadata.is_none() {
                        return Poll::Ready(Err(anyhow!("empty commit metadata")));
                    };
                    self.prev_epoch = Some(request.epoch);
                }
                coordinate_request::Msg::UpdateVnodeRequest(request) => {
                    let bitmap = Bitmap::from(
                        request
                            .vnode_bitmap
                            .as_ref()
                            .ok_or_else(|| anyhow!("empty vnode bitmap"))?,
                    );
                    self.vnode_bitmap = bitmap;
                }
            };
            request
        };
        Poll::Ready(result)
    }
}
