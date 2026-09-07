// Copyright 2023 RisingWave Labs
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

use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::{Result, anyhow};

pub(crate) fn read_file_contents<P: AsRef<Path>>(filepath: P) -> Result<String> {
    std::fs::read_to_string(filepath.as_ref()).map_err(|e| {
        anyhow!(
            "Failed to read contents from {} due to {e}",
            filepath.as_ref().display()
        )
    })
}

pub(crate) fn create_file<P: AsRef<Path>>(filepath: P) -> Result<File> {
    std::fs::File::create(filepath.as_ref()).map_err(|e| {
        anyhow!(
            "Failed to create file: {} due to {e}",
            filepath.as_ref().display()
        )
    })
}

pub(crate) fn write_to_file<S: AsRef<str>>(file: &mut File, contents: S) -> Result<()> {
    let s = contents.as_ref().as_bytes();
    file.write_all(s)
        .map_err(|e| anyhow!("Failed to write file due to {e}"))
}
