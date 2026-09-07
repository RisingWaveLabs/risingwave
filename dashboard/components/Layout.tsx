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
  Box,
  Flex,
  HStack,
  Image,
  Link as ChakraLink,
  Text,
} from "@chakra-ui/react"
import Link from "next/link"
import { useRouter } from "next/router"
import React, { ComponentType, useEffect, useState } from "react"
import {
  IconActivity,
  IconArrowDownToLine,
  IconArrowUpFromLine,
  IconArrowUpRight,
  IconDatabase,
  IconEye,
  IconGitBranch,
  IconHourglass,
  IconLayers,
  IconListChecks,
  IconListTree,
  IconMemoryStick,
  IconNetwork,
  IconProps,
  IconRoute,
  IconRss,
  IconServer,
  IconSettings,
  IconSquareFunction,
  IconTable,
  IconWorkflow,
} from "../components/utils/stroke-icons"
import { colors, fills, fonts, motion, radii } from "../lib/design-tokens"

// App shell: fixed 216px left sidebar + fluid main scroll region.
export const NAVBAR_WIDTH = "216px"

type NavItemData = {
  href: string
  title: string
  icon: ComponentType<IconProps>
  external?: boolean
}

type NavSectionData = {
  label?: string
  items: NavItemData[]
}

const navSections: NavSectionData[] = [
  {
    items: [{ href: "/cluster/", title: "Cluster overview", icon: IconServer }],
  },
  {
    label: "Catalog",
    items: [
      { href: "/sources/", title: "Sources", icon: IconArrowDownToLine },
      { href: "/tables/", title: "Tables", icon: IconTable },
      {
        href: "/materialized_views/",
        title: "Materialized views",
        icon: IconLayers,
      },
      { href: "/indexes/", title: "Indexes", icon: IconListTree },
      {
        href: "/internal_tables/",
        title: "Internal tables",
        icon: IconDatabase,
      },
      { href: "/sinks/", title: "Sinks", icon: IconArrowUpFromLine },
      { href: "/views/", title: "Views", icon: IconEye },
      { href: "/subscriptions/", title: "Subscriptions", icon: IconRss },
      { href: "/functions/", title: "Functions", icon: IconSquareFunction },
    ],
  },
  {
    label: "Streaming",
    items: [
      { href: "/relation_graph/", title: "Relation graph", icon: IconWorkflow },
      {
        href: "/fragment_graph/",
        title: "Fragment graph",
        icon: IconGitBranch,
      },
    ],
  },
  {
    label: "Batch",
    items: [
      { href: "/batch_tasks/", title: "Batch tasks", icon: IconListChecks },
    ],
  },
  {
    label: "Explain",
    items: [
      {
        href: "/explain_distsql/",
        title: "Distributed plan",
        icon: IconNetwork,
      },
    ],
  },
  {
    label: "Debug",
    items: [
      { href: "/await_tree/", title: "Await tree dump", icon: IconHourglass },
      {
        href: "/cpu_profiling/",
        title: "CPU profiling",
        icon: IconActivity,
      },
      {
        href: "/heap_profiling/",
        title: "Heap profiling",
        icon: IconMemoryStick,
      },
      {
        href: "/api/monitor/diagnose",
        title: "Diagnose",
        icon: IconActivity,
        external: true,
      },
      {
        href: "/trace/search",
        title: "Traces",
        icon: IconRoute,
        external: true,
      },
    ],
  },
  {
    label: "Settings",
    items: [{ href: "/settings/", title: "Settings", icon: IconSettings }],
  },
]

function NavItem({ href, title, icon: Icon, external }: NavItemData) {
  const router = useRouter()
  const [match, setMatch] = useState(false)

  useEffect(() => {
    if (external) {
      return
    }
    // Normalize trailing slashes so both "/cluster" and "/cluster/" match.
    const path = `${router.asPath.replace(/\/+$/, "")}/`
    setMatch(path.startsWith(href.toString()))
  }, [href, router.asPath, external])

  return (
    <ChakraLink
      as={Link}
      href={href}
      prefetch={false}
      target={external ? "_blank" : undefined}
      rel={external ? "noreferrer" : undefined}
      display="flex"
      alignItems="center"
      gap={2}
      px={2.5}
      py="7px"
      borderRadius={radii.md}
      fontSize="14px"
      lineHeight={1.5}
      fontWeight={match ? 500 : 400}
      color={match ? colors.foreground : colors.mutedForeground}
      bg={match ? fills.active : "transparent"}
      textDecoration="none"
      transition={`background-color ${motion.durationMs.micro}ms ${motion.easeOut}, color ${motion.durationMs.micro}ms ${motion.easeOut}`}
      _hover={{
        bg: match ? fills.active : fills.hover,
        color: colors.foreground,
        textDecoration: "none",
      }}
    >
      <Box flexShrink={0}>
        <Icon size={15} />
      </Box>
      <Box as="span" flex={1} noOfLines={1}>
        {title}
      </Box>
      {external && (
        <Box flexShrink={0} opacity={0.5}>
          <IconArrowUpRight size={12} />
        </Box>
      )}
    </ChakraLink>
  )
}

function NavSection({ label, items }: NavSectionData) {
  return (
    <Box width="full" mt={label ? 4 : 0}>
      {label && (
        <Text
          px={2.5}
          mb={1}
          fontSize="12px"
          fontWeight={500}
          lineHeight={1.4}
          color={colors.mutedForeground}
        >
          {label}
        </Text>
      )}
      <Flex direction="column" gap="2px">
        {items.map((item) => (
          <NavItem key={item.href.toString()} {...item} />
        ))}
      </Flex>
    </Box>
  )
}

function Layout({ children }: { children: React.ReactNode }) {
  return (
    <Flex bg={colors.background}>
      <Box
        height="100vh"
        overflowY="auto"
        width={NAVBAR_WIDTH}
        minWidth={NAVBAR_WIDTH}
        bg="rgba(245,244,240,0.5)"
        borderRight="1px solid"
        borderColor="rgba(227,224,216,0.5)"
        py={2}
        px={2}
        fontFamily={fonts.body}
        color={colors.foreground}
      >
        <HStack height="52px" spacing={2} px={2.5} mb={1}>
          <Link href="/">
            <Image boxSize="20px" src="/risingwave.svg" alt="RisingWave Logo" />
          </Link>
          <Text fontSize="14px" fontWeight={600} letterSpacing="-0.01em">
            RisingWave
          </Text>
          {/* Quiet neutral badge; the azure accent is rationed elsewhere */}
          <Box
            as="span"
            px={2}
            py="1px"
            borderRadius="full"
            fontSize="12px"
            fontWeight={500}
            lineHeight={1.45}
            color={colors.mutedForeground}
            bg={fills.active}
          >
            Dashboard
          </Box>
        </HStack>
        {navSections.map((section, index) => (
          <NavSection key={section.label ?? index} {...section} />
        ))}
      </Box>
      <Box flex={1} minWidth={0} overflowY="auto" maxHeight="100vh">
        {children}
      </Box>
    </Flex>
  )
}

export default Layout
