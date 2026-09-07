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

import Head from "next/head"
import NextLink from "next/link"
import { ComponentType } from "react"
import styled from "styled-components"
import {
  IconActivity,
  IconArrowUpRight,
  IconBookOpen,
  IconGitBranch,
  IconListChecks,
  IconProps,
  IconServer,
  IconSettings,
  IconWorkflow,
} from "../components/utils/stroke-icons"
import {
  canvasTexture,
  colors,
  fills,
  fonts,
  motion,
  radii,
  shadows,
} from "../lib/design-tokens"

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

const Content = styled.div`
  max-width: 920px;
`

const Heading = styled.h1`
  margin: 0;
  font-size: 24px;
  font-weight: 600;
  line-height: 1.2;
  letter-spacing: -0.01em;
`

const Introduction = styled.p`
  max-width: 520px;
  margin: 8px 0 0;
  color: ${colors.mutedForeground};
  font-size: 14px;
  line-height: 1.5;
`

const ActionsSection = styled.section`
  margin-top: 40px;
`

const SectionTitle = styled.h2`
  margin: 0 0 12px;
  color: ${colors.foregroundSecondary};
  font-size: 14px;
  font-weight: 500;
  line-height: 1.4;
`

const ActionsGrid = styled.div`
  display: grid;
  grid-template-columns: 1fr;
  gap: 16px;

  @media (min-width: 48rem) {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
`

const ActionCard = styled(NextLink)`
  position: relative;
  display: block;
  overflow: hidden;
  padding: 16px;
  border: 1px solid rgba(227, 224, 216, 0.65);
  border-radius: ${radii.lg};
  color: inherit;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: ${shadows.surfaceCard};
  text-decoration: none;
  transition: background-color ${motion.durationMs.normal}ms ${motion.easeOut};

  &:hover {
    background: ${fills.hover};
    text-decoration: none;
  }
`

const CardContent = styled.div`
  display: flex;
  align-items: flex-start;
  gap: 12px;
`

const IconBadge = styled.div`
  display: flex;
  width: 32px;
  height: 32px;
  flex-shrink: 0;
  align-items: center;
  justify-content: center;
  border-radius: ${radii.md};
  color: ${colors.foregroundSecondary};
  background: ${colors.muted};
`

const CardCopy = styled.div`
  padding-right: 16px;
`

const CardTitle = styled.h3`
  margin: 0;
  color: ${colors.foreground};
  font-size: 14px;
  font-weight: 600;
  line-height: 1.4;
`

const CardDescription = styled.p`
  margin: 4px 0 0;
  color: ${colors.mutedForeground};
  font-size: 12px;
  line-height: 1.45;
`

const CornerIcon = styled.span`
  position: absolute;
  top: 14px;
  right: 14px;
  color: ${colors.mutedForeground};
  opacity: 0.6;
  transition: color ${motion.durationMs.normal}ms ${motion.easeOut},
    opacity ${motion.durationMs.normal}ms ${motion.easeOut};

  ${ActionCard}:hover & {
    color: ${colors.foreground};
    opacity: 1;
  }
`

const Watermark = styled.span`
  position: absolute;
  right: -10px;
  bottom: -14px;
  color: ${colors.foreground};
  opacity: 0.05;
  pointer-events: none;
`

const Notice = styled.aside`
  display: flex;
  align-items: flex-start;
  gap: 12px;
  margin-top: 24px;
  padding: 16px;
  border: 1px solid #bae6fd;
  border-radius: ${radii.lg};
  color: #0c4a6e;
  background: #f0f9ff;
`

const NoticeIcon = styled.div`
  flex-shrink: 0;
  margin-top: 1px;
`

const NoticeTitle = styled.h2`
  margin: 0;
  font-size: 14px;
  font-weight: 600;
  line-height: 1.4;
`

const NoticeText = styled.p`
  margin: 2px 0 0;
  font-size: 12px;
  line-height: 1.45;
`

const DocumentationLink = styled.a`
  color: inherit;
  font-weight: 500;
  text-decoration: underline;
  text-underline-offset: 2px;
`

const quickActions: {
  href: string
  title: string
  description: string
  icon: ComponentType<IconProps>
  external?: boolean
}[] = [
  {
    href: "/cluster/",
    title: "Cluster overview",
    description: "Inspect compute nodes, parallelism, and worker health.",
    icon: IconServer,
  },
  {
    href: "/relation_graph/",
    title: "Relation graph",
    description: "See how sources, tables, and materialized views connect.",
    icon: IconWorkflow,
  },
  {
    href: "/fragment_graph/",
    title: "Fragment graph",
    description: "Trace the distributed streaming execution plan.",
    icon: IconGitBranch,
  },
  {
    href: "/batch_tasks/",
    title: "Batch tasks",
    description: "Monitor ad-hoc and scheduled batch query jobs.",
    icon: IconListChecks,
  },
  {
    href: "/settings/",
    title: "Settings",
    description: "Manage dashboard preferences and cluster parameters.",
    icon: IconSettings,
  },
  {
    href: "/api/monitor/diagnose",
    title: "Diagnose",
    description: "Collect logs and metrics into a diagnostic bundle.",
    icon: IconActivity,
    external: true,
  },
]

function QuickActionCard({
  href,
  title,
  description,
  icon: Icon,
  external,
}: (typeof quickActions)[number]) {
  return (
    <ActionCard
      href={href}
      prefetch={false}
      target={external ? "_blank" : undefined}
      rel={external ? "noreferrer" : undefined}
    >
      <CardContent>
        <IconBadge>
          <Icon size={16} />
        </IconBadge>
        <CardCopy>
          <CardTitle>{title}</CardTitle>
          <CardDescription>{description}</CardDescription>
        </CardCopy>
      </CardContent>
      <CornerIcon>
        <IconArrowUpRight size={15} />
      </CornerIcon>
      <Watermark>
        <Icon size={76} />
      </Watermark>
    </ActionCard>
  )
}

export default function Home() {
  // home
  return (
    <>
      <Head>
        <title>RisingWave Dashboard</title>
      </Head>
      <Page>
        <Content>
          <header>
            <Heading>Welcome to RisingWave Dashboard</Heading>
            <Introduction>
              Monitor streaming jobs, explore the catalog, and debug your
              cluster from one place.
            </Introduction>
          </header>
          <ActionsSection>
            <SectionTitle>Get started</SectionTitle>
            <ActionsGrid>
              {quickActions.map((action) => (
                <QuickActionCard key={action.href} {...action} />
              ))}
            </ActionsGrid>
          </ActionsSection>
          <Notice>
            <NoticeIcon>
              <IconBookOpen size={16} />
            </NoticeIcon>
            <div>
              <NoticeTitle>New to RisingWave?</NoticeTitle>
              <NoticeText>
                The documentation covers SQL references, data ingestion guides,
                and cluster operations.{" "}
                <DocumentationLink
                  href="https://docs.risingwave.com"
                  target="_blank"
                  rel="noreferrer"
                >
                  Read the documentation
                </DocumentationLink>
              </NoticeText>
            </div>
          </Notice>
        </Content>
      </Page>
    </>
  )
}
