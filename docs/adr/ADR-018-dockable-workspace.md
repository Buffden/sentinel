# ADR-018: Replace Fixed Dashboard Shell with a Dockable/Resizable Workspace

**Status:** Accepted
**Date:** 2026-08-31

---

## Context

ADR-016 described a fixed dashboard shell: compact top nav, filter/navigation rail, central map workspace, contextual detail/alert panels, and event timeline. This shell is appropriate for a single-domain aviation monitor but is rigid: the operator cannot reorganise panels, cannot open multiple views of the same type, cannot compose a domain-specific workspace, and the layout must be redesigned every time a new domain is added.

Sentinel is evolving toward a general-purpose real-time geospatial workspace (see Product North Star in CLAUDE.md). A customisable workspace is the correct foundation for that direction and removes per-domain layout debt.

Dockable/resizable workspaces are the standard UI pattern for professional operator tools: VS Code, Grafana, Bloomberg Terminal, Palantir Foundry. Operators can resize, move, tab, maximise, close, and restore panels independently.

A registry-driven widget model decouples domain concerns from layout concerns. New domains register new widget types; the workspace engine renders them without modification.

---

## Decision

Replace the fixed dashboard shell with a **registry-driven dockable/resizable workspace** using **Dockview** as the layout engine.

Dockview provides VS Code-style docking, tabbing, floating panels, and drag-to-resize. It is MIT-licensed, React-native, and actively maintained.

Widgets are registered by type. The workspace renders widget instances from the registry. Multiple instances of the same widget type may coexist (for example, two map views with different viewports).

The fixed top nav is retained as the application shell for workspace selection, auth state, and global controls. Dockview occupies the remainder of the viewport.

---

## Reasoning

**The fixed shell cannot accommodate multi-domain growth.** Every new domain would require a layout redesign. A widget registry decouples domain concerns from layout concerns permanently.

**Operator customisation has direct product value.** An operator monitoring maritime traffic wants a different layout from one monitoring aviation. Dockable workspaces are the standard for this class of product.

**Dockview is the strongest React-native option for this use case.**

| Library | Notes |
| --- | --- |
| Dockview | VS Code-style docking + tabbing + floating; MIT; first-class React support |
| react-mosaic | Binary tree splitting only; no tabbing or floating |
| Golden Layout | Mature but complex; React bindings are third-party and unmaintained |
| react-grid-layout | Grid-based; suitable for dashboard builders, not operator workspaces |

**The aviation vertical proves the widget model first.** The Map widget, Alert widget, and Entity Detail widget are the first three widget implementations. Later domains add widget types without modifying the workspace engine.

---

## Alternatives Considered

### Keep the fixed dashboard shell

- Adequate for aviation-only v1
- Creates layout redesign debt with each new domain added
- Does not support operator customisation
- Rejected: establishing the widget model now is low cost and grows cheaper with each domain added

### react-mosaic

- Binary tree splitting only; no tabbing or floating panels
- Less suitable for complex multi-panel operator workspaces

### Golden Layout

- Production-quality docking engine with a long track record
- React bindings are a community package, not officially maintained
- Dockview provides equivalent capability with first-class React support

---

## Consequences

- The fixed shell described in ADR-016 is superseded. `CLAUDE.md` Fixed Stack gains a Workspace layout row: Dockview.
- Widgets are defined in a registry. Each widget definition owns its stable type, component, title/icon metadata, sizing constraints, supported workspace actions, and configuration contract.
- The map is a Dockview widget, not the application shell. MapLibre GL (ADR-017) initialises inside the Map widget component.
- Default layout for the aviation vertical: map widget occupying the majority of the workspace, alert panel and entity detail panel docked to the right. This layout is the starting state, not a fixed constraint.
- Workspace layout state and widget configuration may be persisted by the backend. Persistence mechanism is an implementation choice, deferred until the default layout is working for the aviation vertical.
- ARCHITECTURE.md and the dashboard phase plan must be updated to reflect this model before dashboard implementation begins.
