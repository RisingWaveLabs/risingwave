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

import {
  Popover,
  PopoverArrow,
  PopoverBody,
  PopoverCloseButton,
  PopoverContent,
  PopoverTrigger,
} from "@chakra-ui/react"
import loadable from "@loadable/component"
import Head from "next/head"

import Link from "next/link"
import {
  Fragment,
  forwardRef,
  type ButtonHTMLAttributes,
  type ComponentProps,
  type CSSProperties,
  type ReactNode,
} from "react"
import useFetch from "../lib/api/fetch"
import {
  Relation,
  StreamingJob,
  getDatabases,
  getSchemas,
  getStreamingJobs,
  getUsers,
} from "../lib/api/streaming"
import extractColumnInfo from "../lib/extractInfo"
import {
  canvasTexture,
  colors,
  fills,
  fonts,
  motion,
  radii,
  shadows,
} from "../lib/design-tokens"
import {
  Sink as RwSink,
  Source as RwSource,
  Table as RwTable,
} from "../proto/gen/catalog"
import { CatalogModal, useCatalogModal } from "./CatalogModal"
import { IconRefresh } from "./utils/stroke-icons"

export const ReactJson = loadable(() => import("react-json-view"))

export type Column<R> = {
  name: string
  width: number
  content: (r: R) => ReactNode
}

const pageStyle: CSSProperties = {
  minHeight: "100vh",
  padding: "32px 24px",
  color: colors.foreground,
  backgroundColor: colors.background,
  backgroundImage: canvasTexture.backgroundImage,
  backgroundSize: canvasTexture.backgroundSize,
  fontFamily: fonts.body,
}

const headingStyle: CSSProperties = {
  margin: 0,
  fontSize: 24,
  fontWeight: 600,
  lineHeight: 1.2,
  letterSpacing: "-0.01em",
}

const countTextStyle: CSSProperties = {
  margin: "8px 0 0",
  color: colors.mutedForeground,
  fontSize: 14,
  lineHeight: 1.5,
}

const headerRowStyle: CSSProperties = {
  display: "flex",
  alignItems: "flex-start",
  justifyContent: "space-between",
  gap: 16,
}

const refreshButtonStyle: CSSProperties = {
  display: "inline-flex",
  flexShrink: 0,
  alignItems: "center",
  gap: 6,
  padding: "6px 12px",
  border: "1px solid rgba(227, 224, 216, 0.65)",
  borderRadius: radii.md,
  color: colors.foregroundSecondary,
  background: "rgba(255, 255, 255, 0.96)",
  boxShadow: shadows.surfaceCard,
  fontSize: 12,
  fontWeight: 500,
  lineHeight: 1.4,
  cursor: "pointer",
  transition: `background-color ${motion.durationMs.micro}ms ${motion.easeOut}`,
}

const tableCardStyle: CSSProperties = {
  marginTop: 24,
  overflowX: "auto",
  border: "1px solid rgba(227, 224, 216, 0.65)",
  borderRadius: radii.lg,
  background: "rgba(255, 255, 255, 0.96)",
  boxShadow: shadows.surfaceCard,
}

const tableStyle: CSSProperties = {
  width: "100%",
  borderCollapse: "collapse",
  fontSize: 14,
  lineHeight: 1.5,
}

const thStyle: CSSProperties = {
  padding: "10px 16px",
  borderBottom: `1px solid ${colors.border}`,
  color: colors.mutedForeground,
  fontSize: 12,
  fontWeight: 500,
  lineHeight: 1.4,
  textAlign: "left",
  whiteSpace: "nowrap",
}

const tdStyle: CSSProperties = {
  padding: "10px 16px",
  borderBottom: "1px solid rgba(227, 224, 216, 0.5)",
  color: colors.foregroundSecondary,
  verticalAlign: "top",
  whiteSpace: "nowrap",
}

const emptyStateStyle: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
  gap: 4,
  padding: "48px 16px",
  textAlign: "center",
}

const emptyTitleStyle: CSSProperties = {
  margin: 0,
  color: colors.foregroundSecondary,
  fontSize: 14,
  fontWeight: 500,
  lineHeight: 1.4,
}

const emptyHintStyle: CSSProperties = {
  margin: 0,
  color: colors.mutedForeground,
  fontSize: 12,
  lineHeight: 1.45,
}

const textButtonStyle: CSSProperties = {
  padding: 0,
  border: "none",
  color: colors.accent,
  background: "none",
  font: "inherit",
  cursor: "pointer",
  textDecoration: "none",
}

const textLinkStyle: CSSProperties = {
  color: colors.accent,
  textDecoration: "none",
}

const TextButton = forwardRef<
  HTMLButtonElement,
  ButtonHTMLAttributes<HTMLButtonElement>
>(({ style, ...props }, ref) => (
  <button ref={ref} {...props} style={{ ...textButtonStyle, ...style }} />
))
TextButton.displayName = "TextButton"

const TextLink = ({ style, ...props }: ComponentProps<typeof Link>) => (
  <Link {...props} style={{ ...textLinkStyle, ...style }} />
)

export const dependentsColumn: Column<Relation> = {
  name: "Depends",
  width: 1,
  content: (r) => (
    <TextLink href={`/relation_graph/?id=${r.id}`} aria-label="view dependents">
      D
    </TextLink>
  ),
}

export const fragmentsColumn: Column<Relation> = {
  name: "Fragments",
  width: 1,
  content: (r) => (
    <TextLink href={`/fragment_graph/?id=${r.id}`} aria-label="view fragments">
      F
    </TextLink>
  ),
}

export const primaryKeyColumn: Column<RwTable> = {
  name: "Primary Key",
  width: 1,
  content: (r) =>
    r.pk
      .map((order) => order.columnIndex)
      .map((i) => r.columns[i])
      .map((col) => extractColumnInfo(col))
      .join(", "),
}

export const vnodeCountColumn: Column<RwTable> = {
  name: "Vnode Count",
  width: 1,
  // The table catalogs retrieved here are constructed from SQL models,
  // where the `vnode_count` column has already been populated during migration.
  // Therefore, it should always be present and no need to specify a fallback.
  content: (r) => r.maybeVnodeCount ?? "?",
}

// Helper function to format bytes into human readable format
function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined) return "unknown"
  if (bytes === 0) return "0 B"
  const k = 1024
  const sizes = ["B", "KB", "MB", "GB", "TB"]
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + " " + sizes[i]
}

export const dataSizeColumn: Column<Relation> = {
  name: "Data Size",
  width: 2,
  content: (r) => formatBytes(r.totalSizeBytes),
}

export const tableColumns = [primaryKeyColumn, vnodeCountColumn, dataSizeColumn]

export const connectorColumnSource: Column<RwSource> = {
  name: "Connector",
  width: 3,
  content: (r) => r.withProperties.connector ?? "unknown",
}

export const connectorColumnSink: Column<RwSink> = {
  name: "Connector",
  width: 3,
  content: (r) => r.properties.connector ?? "unknown",
}

export const configOverrideColumn: Column<Relation> = {
  name: "Config Override",
  width: 3,
  content: (r) => {
    const override = r.streamingJob?.configOverride?.trim()
    if (!override) {
      return "-"
    }
    return (
      <Popover placement="auto" trigger="click">
        <PopoverTrigger>
          <TextButton aria-label="view config override">C</TextButton>
        </PopoverTrigger>
        <PopoverContent maxW="lg">
          <PopoverArrow />
          <PopoverCloseButton />
          <PopoverBody fontFamily="mono" whiteSpace="pre-wrap">
            {override}
          </PopoverBody>
        </PopoverContent>
      </Popover>
    )
  },
}

export const streamingJobColumns = [
  dependentsColumn,
  fragmentsColumn,
  configOverrideColumn,
]

export function Relations<R extends Relation>(
  title: string,
  getRelations: () => Promise<R[]>,
  extraColumns: Column<R>[],
  options?: {
    withStreamingJobs?: boolean
  }
) {
  const {
    response: relationList,
    refresh,
    loading,
  } = useFetch(async () => {
    const streamingJobsPromise = options?.withStreamingJobs
      ? getStreamingJobs()
      : undefined
    const [relations, users, databases, schemas] = await Promise.all([
      getRelations(),
      getUsers(),
      getDatabases(),
      getSchemas(),
    ])
    const streamingJobs = streamingJobsPromise
      ? await streamingJobsPromise
      : undefined
    const streamingJobMap = streamingJobs?.reduce<Map<number, StreamingJob>>(
      (acc, job) => {
        acc.set(job.id, job)
        return acc
      },
      new Map()
    )

    return relations.map((r) => {
      // Add owner, schema, and database names. It's linear search but the list is small.
      const owner = users.find((u) => u.id === r.owner)
      const ownerName = owner?.name
      const schema = schemas.find((s) => s.id === r.schemaId)
      const schemaName = schema?.name
      const database = databases.find((d) => d.id === r.databaseId)
      const databaseName = database?.name
      const streamingJob = streamingJobMap?.get(r.id)
      return { streamingJob, ...r, ownerName, schemaName, databaseName }
    })
  })
  const [modalData, setModalId] = useCatalogModal(relationList)

  const modal = (
    <CatalogModal modalData={modalData} onClose={() => setModalId(null)} />
  )

  const table = (
    <main style={pageStyle}>
      <div style={headerRowStyle}>
        <div>
          <h1 style={headingStyle}>{title}</h1>
          {relationList && (
            <p style={countTextStyle}>
              Total: {relationList.length}{" "}
              {relationList.length === 1 ? "item" : "items"}
            </p>
          )}
        </div>
        <button
          onClick={refresh}
          disabled={loading}
          aria-label="refresh"
          style={{
            ...refreshButtonStyle,
            ...(loading ? { opacity: 0.6, cursor: "default" } : null),
          }}
        >
          <span
            style={{
              display: "inline-flex",
              animation: loading ? "rw-spin 0.8s linear infinite" : "none",
            }}
          >
            <IconRefresh size={14} />
          </span>
          Refresh
        </button>
      </div>
      <div style={tableCardStyle}>
        {relationList && relationList.length === 0 ? (
          <div style={emptyStateStyle}>
            <p style={emptyTitleStyle}>No {title.toLowerCase()} yet</p>
            <p style={emptyHintStyle}>
              Objects you create will show up here once they exist.
            </p>
          </div>
        ) : (
          <table style={tableStyle}>
            <thead>
              <tr>
                <th style={thStyle}>Id</th>
                <th style={thStyle}>Database</th>
                <th style={thStyle}>Schema</th>
                <th style={thStyle}>Name</th>
                <th style={thStyle}>Owner</th>
                {extraColumns.map((c) => (
                  <th key={c.name} style={thStyle}>
                    {c.name}
                  </th>
                ))}
                <th style={thStyle}>Visible Columns</th>
              </tr>
            </thead>
            <tbody>
              {relationList?.map((r, index) => {
                const rowTdStyle =
                  index === relationList.length - 1
                    ? { ...tdStyle, borderBottom: "none" }
                    : tdStyle

                return (
                  <tr key={r.id}>
                    <td style={rowTdStyle}>
                      <TextButton
                        aria-label="view catalog"
                        onClick={() => setModalId(r.id)}
                      >
                        {r.id}
                      </TextButton>
                    </td>
                    <td style={rowTdStyle}>{r.databaseName}</td>
                    <td style={rowTdStyle}>{r.schemaName}</td>
                    <td style={rowTdStyle}>{r.name}</td>
                    <td style={rowTdStyle}>{r.ownerName}</td>
                    {extraColumns.map((c) => (
                      <td key={c.name} style={rowTdStyle}>
                        {c.content(r)}
                      </td>
                    ))}
                    {r.columns && r.columns.length > 0 && (
                      <td style={{ ...rowTdStyle, whiteSpace: "normal" }}>
                        {r.columns
                          .filter((col) =>
                            "isHidden" in col ? !col.isHidden : true
                          )
                          .map((col) => extractColumnInfo(col))
                          .join(", ")}
                      </td>
                    )}
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </main>
  )

  return (
    <Fragment>
      <Head>
        <title>{title}</title>
      </Head>
      {modal}
      {table}
    </Fragment>
  )
}
