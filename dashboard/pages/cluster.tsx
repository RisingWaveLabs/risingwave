/*
 * Copyright 2025 RisingWave Labs
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
 *
 */

import { clone, reverse, sortBy } from "lodash"
import Head from "next/head"
import { useCallback, useEffect, useState } from "react"
import { Area, AreaChart, ResponsiveContainer, XAxis, YAxis } from "recharts"
import styled from "styled-components"
import { Metrics, MetricsSample } from "../components/metrics"
import useErrorToast from "../hook/useErrorToast"
import {
  getClusterInfoComputeNode,
  getClusterInfoFrontend,
  getClusterMetrics,
  getClusterVersion,
} from "../lib/api/cluster"
import {
  canvasTexture,
  colors,
  fonts,
  radii,
  shadows,
} from "../lib/design-tokens"
import { WorkerNode } from "../proto/gen/common"

const Page = styled.main`
  min-height: 100vh;
  padding: 32px 24px;
  color: ${colors.foreground};
  background-color: ${colors.background};
  background-image: ${canvasTexture.backgroundImage};
  background-size: ${canvasTexture.backgroundSize};
  font-family: ${fonts.body};

  @media (min-width: 62rem) {
    padding: 48px 40px;
  }
`

const Heading = styled.h1`
  margin: 0;
  font-size: 24px;
  font-weight: 600;
  line-height: 1.2;
  letter-spacing: -0.01em;
`

const VersionText = styled.p`
  margin: 8px 0 0;
  color: ${colors.mutedForeground};
  font-size: 14px;
  line-height: 1.5;
`

const Section = styled.section`
  margin-top: 32px;
`

const SectionTitle = styled.h2`
  margin: 0 0 12px;
  color: ${colors.foregroundSecondary};
  font-size: 14px;
  font-weight: 500;
  line-height: 1.4;
`

const CardsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 16px;
`

const Card = styled.div`
  padding: 16px;
  border: 1px solid rgba(227, 224, 216, 0.65);
  border-radius: ${radii.lg};
  background: rgba(255, 255, 255, 0.96);
  box-shadow: ${shadows.surfaceCard};
`

const NodeHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 8px;
`

const StatusDot = styled.span`
  width: 8px;
  height: 8px;
  flex-shrink: 0;
  border-radius: 50%;
  background: ${colors.success};
`

const NodeTitle = styled.h3`
  margin: 0;
  font-size: 14px;
  font-weight: 600;
  line-height: 1.4;
`

const NodeMeta = styled.p`
  margin: 4px 0 0;
  color: ${colors.mutedForeground};
  font-size: 12px;
  line-height: 1.45;
`

const MetricsLabel = styled.p`
  margin: 0 0 8px;
  color: ${colors.mutedForeground};
  font-size: 12px;
  line-height: 1.45;

  strong {
    color: ${colors.foregroundSecondary};
    font-weight: 600;
  }
`

function WorkerNodeComponent({
  workerNodeType,
  workerNode,
}: {
  workerNodeType: string
  workerNode: WorkerNode
}) {
  return (
    <div>
      <NodeHeader>
        <StatusDot />
        <NodeTitle>
          {workerNodeType} #{workerNode.id}
        </NodeTitle>
      </NodeHeader>
      <NodeMeta>Running</NodeMeta>
      <NodeMeta>
        {workerNode.host?.host}:{workerNode.host?.port}
      </NodeMeta>
    </div>
  )
}

function WorkerNodeMetricsComponent({
  job,
  instance,
  metrics,
  isCpuMetrics,
}: {
  job: string
  instance: string
  metrics: MetricsSample[]
  isCpuMetrics: boolean
}) {
  const metricsCallback = useCallback(() => {
    const filledMetrics: MetricsSample[] = []
    if (metrics.length === 0) {
      return []
    }
    let lastTs: number = metrics.at(-1)!.timestamp
    for (let pt of reverse(clone(metrics))) {
      while (lastTs - pt.timestamp > 0) {
        lastTs -= 60
        filledMetrics.push({
          timestamp: lastTs,
          value: 0,
        })
      }
      filledMetrics.push(pt)
      lastTs -= 60
    }
    while (filledMetrics.length < 60) {
      filledMetrics.push({ timestamp: lastTs, value: 0 })
      lastTs -= 60
    }
    return reverse(filledMetrics)
  }, [metrics])
  return (
    <div>
      <MetricsLabel>
        <strong>{job}</strong> {instance}
      </MetricsLabel>
      <ResponsiveContainer width="100%" height={100}>
        <AreaChart data={metricsCallback()}>
          <XAxis
            dataKey="timestamp"
            type="number"
            domain={["dataMin", "dataMax"]}
            hide={true}
          />
          {isCpuMetrics && <YAxis type="number" domain={[0, 1]} hide={true} />}
          <Area
            isAnimationActive={false}
            type="linear"
            dataKey="value"
            strokeWidth={1.5}
            stroke={colors.accent}
            fill="rgba(42, 157, 244, 0.12)"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

interface ClusterNodeMetrics {
  cpuData: Metrics[]
  memoryData: Metrics[]
}

export default function Cluster() {
  const [frontendList, setFrontendList] = useState<WorkerNode[]>([])
  const [computeNodeList, setComputeNodeList] = useState<WorkerNode[]>([])
  const [metrics, setMetrics] = useState<ClusterNodeMetrics>()
  const [version, setVersion] = useState<string>()
  const toast = useErrorToast()

  useEffect(() => {
    async function doFetch() {
      try {
        setFrontendList(await getClusterInfoFrontend())
        setComputeNodeList(await getClusterInfoComputeNode())
        setVersion(await getClusterVersion())
      } catch (e: any) {
        toast(e)
      }
    }
    doFetch()
    return () => {}
  }, [toast])

  useEffect(() => {
    async function doFetch() {
      while (true) {
        try {
          let metrics: ClusterNodeMetrics = await getClusterMetrics()
          metrics.cpuData = sortBy(metrics.cpuData, (m) => m.metric.instance)
          metrics.memoryData = sortBy(
            metrics.memoryData,
            (m) => m.metric.instance
          )
          setMetrics(metrics)
          await new Promise((resolve) => setTimeout(resolve, 5000)) // refresh every 5 secs
        } catch (e: any) {
          toast(e, "warning")
          break
        }
      }
    }
    doFetch()
    return () => {}
  }, [toast])

  return (
    <>
      <Head>
        <title>Cluster Overview</title>
      </Head>
      <Page>
        <header>
          <Heading>Cluster Overview</Heading>
          <VersionText>Version: {version}</VersionText>
        </header>
        <Section>
          <SectionTitle>Nodes</SectionTitle>
          <CardsGrid>
            {frontendList.map((frontend) => (
              <Card key={frontend.id}>
                <WorkerNodeComponent
                  workerNodeType="Frontend"
                  workerNode={frontend}
                />
              </Card>
            ))}
            {computeNodeList.map((computeNode) => (
              <Card key={computeNode.id}>
                <WorkerNodeComponent
                  workerNodeType="Compute"
                  workerNode={computeNode}
                />
              </Card>
            ))}
          </CardsGrid>
        </Section>
        <Section>
          <SectionTitle>CPU Usage</SectionTitle>
          <CardsGrid>
            {metrics &&
              metrics.cpuData.map((data) => (
                <Card key={data.metric.instance}>
                  <WorkerNodeMetricsComponent
                    job={data.metric.job}
                    instance={data.metric.instance}
                    metrics={data.sample}
                    isCpuMetrics={true}
                  />
                </Card>
              ))}
          </CardsGrid>
        </Section>
        <Section>
          <SectionTitle>Memory Usage</SectionTitle>
          <CardsGrid>
            {metrics &&
              metrics.memoryData.map((data) => (
                <Card key={data.metric.instance}>
                  <WorkerNodeMetricsComponent
                    job={data.metric.job}
                    instance={data.metric.instance}
                    metrics={data.sample}
                    isCpuMetrics={false}
                  />
                </Card>
              ))}
          </CardsGrid>
        </Section>
      </Page>
    </>
  )
}
