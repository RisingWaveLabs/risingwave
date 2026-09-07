/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  Box,
  Button,
  Flex,
  FormControl,
  FormLabel,
  NumberInput,
  NumberInputField,
  Select,
  VStack,
} from "@chakra-ui/react"
import Editor from "@monaco-editor/react"
import { saveAs } from "file-saver"
import Head from "next/head"
import { Fragment, useEffect, useState } from "react"
import Title from "../components/Title"
import api from "../lib/api/api"
import { getClusterInfoProfileWorkers } from "../lib/api/cluster"
import useFetch from "../lib/api/fetch"
import { WorkerNode, WorkerType } from "../proto/gen/common"

const SIDEBAR_WIDTH = 200
const DEFAULT_DURATION_SECS = 10

const workerTypeLabel = (workerType: WorkerType) => {
  switch (workerType) {
    case WorkerType.WORKER_TYPE_FRONTEND:
      return "Frontend"
    case WorkerType.WORKER_TYPE_COMPUTE_NODE:
      return "Compute"
    case WorkerType.WORKER_TYPE_COMPACTOR:
      return "Compactor"
    default:
      return "Other"
  }
}

const getWorkerLabel = (
  nodeId: number | undefined,
  workerNodes: WorkerNode[] | undefined
) => {
  if (nodeId === undefined) {
    return "Worker Node"
  }
  const node = (workerNodes ?? []).find((n) => n.id === nodeId)
  if (!node) {
    return `Worker Node ${nodeId}`
  }
  return `Worker Node ${nodeId} (${workerTypeLabel(node.type)})`
}

export default function CpuProfiling() {
  const { response: workerNodes } = useFetch(getClusterInfoProfileWorkers)

  const [workerNodeId, setWorkerNodeId] = useState<number>()
  const [durationSecs, setDurationSecs] = useState(DEFAULT_DURATION_SECS)
  const [isProfiling, setIsProfiling] = useState(false)
  const [displayInfo, setDisplayInfo] = useState(
    'Select a worker node and click "Dump" to collect and download a CPU flame graph.'
  )

  useEffect(() => {
    if (workerNodes && !workerNodeId && workerNodes.length > 0) {
      setWorkerNodeId(workerNodes[0].id)
    }
  }, [workerNodes, workerNodeId])

  async function dumpProfile() {
    if (
      workerNodeId === undefined ||
      !Number.isInteger(durationSecs) ||
      durationSecs < 1 ||
      durationSecs > 300
    ) {
      return
    }

    const workerLabel = getWorkerLabel(workerNodeId, workerNodes)
    setIsProfiling(true)
    setDisplayInfo(
      `Collecting a ${durationSecs}-second CPU profile from ${workerLabel}...`
    )

    try {
      const url = api.urlFor(
        `/monitor/dump_cpu_profile/${workerNodeId}/${durationSecs}`
      )
      const response = await fetch(url)
      if (!response.ok) {
        const errorBody = await response.text()
        const errorDetail = (() => {
          try {
            return JSON.parse(errorBody).error ?? errorBody
          } catch (_) {
            return errorBody
          }
        })()
        throw Error(
          `${response.status} ${response.statusText}${
            errorDetail ? `: ${errorDetail}` : ""
          }`
        )
      }

      const flamegraph = await response.blob()
      const timestamp = new Date().toISOString().replace(/[:.]/g, "-")
      const fileName = `cpu-profile-worker-${workerNodeId}-${timestamp}.svg`
      saveAs(flamegraph, fileName)
      setDisplayInfo(
        `CPU profile from ${workerLabel} downloaded as ${fileName}.`
      )
    } catch (e: any) {
      setDisplayInfo(
        `Failed to collect CPU profile from ${workerLabel}.\n\nError: ${
          e.message
        }${e.cause ? `\nCause: ${e.cause}` : ""}`
      )
    } finally {
      setIsProfiling(false)
    }
  }

  const workerTypeOrder = [
    WorkerType.WORKER_TYPE_COMPUTE_NODE,
    WorkerType.WORKER_TYPE_FRONTEND,
    WorkerType.WORKER_TYPE_COMPACTOR,
  ]
  const groupedWorkerNodes = (workerNodes ?? []).reduce((groups, node) => {
    const list = groups.get(node.type) ?? []
    list.push(node)
    groups.set(node.type, list)
    return groups
  }, new Map<WorkerType, WorkerNode[]>())
  for (const nodes of groupedWorkerNodes.values()) {
    nodes.sort((a, b) => a.id - b.id)
  }

  return (
    <Fragment>
      <Head>
        <title>CPU Profiling</title>
      </Head>
      <Flex p={3} height="calc(50vh - 20px)" flexDirection="column">
        <Title>CPU Profiling</Title>
        <Flex flexDirection="row" height="full" width="full">
          <VStack
            mr={3}
            spacing={3}
            alignItems="flex-start"
            width={SIDEBAR_WIDTH}
            height="full"
          >
            <FormControl>
              <FormLabel textColor="blue.500">Dump CPU Profile</FormLabel>
              <VStack>
                <FormLabel>Worker Nodes</FormLabel>
                <Select
                  value={workerNodeId ?? ""}
                  onChange={(event) =>
                    setWorkerNodeId(parseInt(event.target.value))
                  }
                >
                  {workerTypeOrder.flatMap((workerType) => {
                    const nodes = groupedWorkerNodes.get(workerType)
                    if (!nodes || nodes.length === 0) {
                      return []
                    }
                    return (
                      <optgroup
                        key={workerType}
                        label={workerTypeLabel(workerType)}
                      >
                        {nodes.map((node) => (
                          <option value={node.id} key={node.id}>
                            ({node.id}) {node.host?.host}:{node.host?.port}
                          </option>
                        ))}
                      </optgroup>
                    )
                  })}
                </Select>
                <FormLabel>Duration (seconds)</FormLabel>
                <NumberInput
                  min={1}
                  max={300}
                  step={1}
                  value={durationSecs}
                  onChange={(_, value) =>
                    setDurationSecs(Number.isNaN(value) ? 0 : value)
                  }
                  width="full"
                >
                  <NumberInputField />
                </NumberInput>
                <Button
                  isDisabled={
                    workerNodeId === undefined ||
                    !Number.isInteger(durationSecs) ||
                    durationSecs < 1 ||
                    durationSecs > 300
                  }
                  isLoading={isProfiling}
                  loadingText="Profiling"
                  onClick={dumpProfile}
                  width="full"
                >
                  Dump
                </Button>
              </VStack>
            </FormControl>
          </VStack>
          <Box
            flex={1}
            height="full"
            ml={3}
            overflowX="scroll"
            overflowY="scroll"
          >
            <Editor
              language="plaintext"
              options={{
                fontSize: 13,
                readOnly: true,
                renderWhitespace: "boundary",
                wordWrap: "on",
              }}
              value={displayInfo}
            />
          </Box>
        </Flex>
      </Flex>
    </Fragment>
  )
}
