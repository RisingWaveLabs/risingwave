// Copyright 2022 RisingWave Labs
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

pub mod hummock {

    use bitflags::bitflags;
    bitflags! {

        #[derive(Default)]
        pub struct CompactionFilterFlag: u32 {
            const NONE = 0b00000000;
            const STATE_CLEAN = 0b00000010;
            const TTL = 0b00000100;
        }
    }

    impl From<CompactionFilterFlag> for u32 {
        fn from(flag: CompactionFilterFlag) -> Self {
            flag.bits()
        }
    }
}

pub mod log_store {
    use crate::types::DataType;
    use crate::util::sort_util::OrderType;

    pub const EPOCH_COLUMN_NAME: &str = "kv_log_store_epoch";
    pub const SEQ_ID_COLUMN_NAME: &str = "kv_log_store_seq_id";
    pub const ROW_OP_COLUMN_NAME: &str = "kv_log_store_row_op";
    pub const VNODE_COLUMN_NAME: &str = "kv_log_store_vnode";

    pub const EPOCH_COLUMN_TYPE: DataType = DataType::Int64;
    pub const SEQ_ID_COLUMN_TYPE: DataType = DataType::Int32;
    pub const ROW_OP_COLUMN_TYPE: DataType = DataType::Int16;
    pub const VNODE_COLUMN_TYPE: DataType = DataType::Int16;
    pub const INSERT_OP_CODE: i16 = 1;

    /// Encode an unsigned RisingWave epoch so that its signed integer ordering is preserved in
    /// the log-store primary key.
    pub const fn encode_epoch(epoch: u64) -> i64 {
        epoch as i64 ^ (1i64 << 63)
    }

    /// Decode an epoch stored by [`encode_epoch`].
    pub const fn decode_epoch(encoded_epoch: i64) -> u64 {
        encoded_epoch as u64 ^ (1u64 << 63)
    }

    pub mod v1 {
        use std::sync::LazyLock;

        use super::*;

        /// `epoch`, `seq_id`, `row_op`
        pub const KV_LOG_STORE_PREDEFINED_COLUMNS: [(&str, DataType); 3] = [
            (EPOCH_COLUMN_NAME, EPOCH_COLUMN_TYPE),
            (SEQ_ID_COLUMN_NAME, SEQ_ID_COLUMN_TYPE),
            (ROW_OP_COLUMN_NAME, ROW_OP_COLUMN_TYPE),
        ];

        pub const EPOCH_COLUMN_INDEX: usize = 0;
        pub const SEQ_ID_COLUMN_INDEX: usize = 1;
        pub const ROW_OP_COLUMN_INDEX: usize = 2;

        /// `epoch`, `seq_id`
        pub static PK_ORDERING: LazyLock<[OrderType; 2]> =
            LazyLock::new(|| [OrderType::ascending(), OrderType::ascending_nulls_last()]);
    }

    pub mod v2 {
        use std::sync::LazyLock;

        use super::*;

        /// `epoch`, `seq_id`, `vnode`, `row_op`
        pub const KV_LOG_STORE_PREDEFINED_COLUMNS: [(&str, DataType); 4] = [
            (EPOCH_COLUMN_NAME, EPOCH_COLUMN_TYPE),
            (SEQ_ID_COLUMN_NAME, SEQ_ID_COLUMN_TYPE),
            (VNODE_COLUMN_NAME, VNODE_COLUMN_TYPE),
            (ROW_OP_COLUMN_NAME, ROW_OP_COLUMN_TYPE),
        ];

        pub const EPOCH_COLUMN_INDEX: usize = 0;
        pub const SEQ_ID_COLUMN_INDEX: usize = 1;
        pub const VNODE_COLUMN_INDEX: usize = 2;
        pub const ROW_OP_COLUMN_INDEX: usize = 3;

        pub static PK_ORDERING: LazyLock<[OrderType; 3]> = LazyLock::new(|| {
            [
                OrderType::ascending(),
                OrderType::ascending_nulls_last(),
                OrderType::ascending(),
            ]
        });
    }
}
