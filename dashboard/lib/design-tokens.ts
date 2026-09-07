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

/**
 * Design tokens extracted from the RisingWave Cloud new-portal Design DNA
 * (design-dna.json). Shared constants for restyling the dashboard UI
 * step by step: warm neutral canvas, cool navy ink, one rationed azure
 * accent, and the inset-highlight "machined surface" card material.
 */

export const colors = {
  // Warm neutral surfaces (hue 36-40)
  background: "#FAF8F4",
  secondary: "#F5F4F0",
  muted: "#F2F0EB",
  border: "#E3E0D8",
  // Cool navy text (hue 220-222), three semantic tiers
  mutedForeground: "#515A6B",
  foregroundSecondary: "#3D4452",
  foreground: "#161B26",
  // Brand
  primary: "#171C28", // inverted dark solid, primary actions only
  accent: "#2A9DF4", // brand azure, used sparingly
  // Semantic
  success: "#16A34A",
  warning: "#F59E0B",
  error: "#EF4444",
  info: "#0EA5E9",
} as const

export const fonts = {
  body: "'Avenir Next', 'Segoe UI', 'Helvetica Neue', Arial, sans-serif",
  mono: "'JetBrains Mono', SFMono-Regular, ui-monospace, monospace",
} as const

export const radii = {
  sm: "10px",
  md: "12px",
  lg: "14px",
} as const

export const shadows = {
  // Signature surface: nearly flat ambient + inset top highlight
  surfaceCard:
    "0 1px 2px rgba(15,23,42,0.04), inset 0 1px 0 rgba(255,255,255,0.9)",
  panel: "0 1px 2px rgba(15,23,42,0.05), 0 18px 40px rgba(15,23,42,0.04)",
  float: "0 22px 48px rgba(15,23,42,0.08)",
} as const

export const fills = {
  // Quiet interaction feedback: foreground at 4% / 7% alpha
  hover: "rgba(22,27,38,0.04)",
  active: "rgba(22,27,38,0.07)",
} as const

export const motion = {
  // Signature spring easing for entrances and step transitions
  easeOut: "cubic-bezier(0.16, 1, 0.3, 1)",
  durationMs: { micro: 150, normal: 200, macro: 220 },
} as const

// Sub-perceptual page texture: 24px hairline grid at 8% slate plus two
// static 8% radial gradient tints (azure top-left, amber bottom-right).
export const canvasTexture = {
  backgroundImage: [
    "linear-gradient(rgba(148,163,184,0.08) 1px, transparent 1px)",
    "linear-gradient(90deg, rgba(148,163,184,0.08) 1px, transparent 1px)",
    "radial-gradient(720px 480px at 0% 0%, rgba(59,130,246,0.08), transparent 70%)",
    "radial-gradient(800px 560px at 100% 100%, rgba(245,158,11,0.08), transparent 70%)",
  ].join(", "),
  backgroundSize: "24px 24px, 24px 24px, auto, auto",
} as const
