# Custom Instructions

Custom Instructions personalize Roo's behavior, shaping responses, coding style, and decisions.

## Instruction File Locations

Provide instructions via global rules, workspace rules, or the Prompts tab.

-   **Global Rules:** Apply to all projects.
    -   Linux/macOS: `~/.roo/rules/` and `~/.roo/rules-{modeSlug}/`
    -   Windows: `%USERPROFILE%\.roo\rules\` and `%USERPROFILE%\.roo\rules-{modeSlug}\`
-   **Workspace Rules:** Project-specific, override global rules.
    -   **Preferred:** Directory (`.roo/rules/`)
    -   **Fallback:** Single file (`.roorules`)
-   **Mode-Specific Instructions:** Apply to a specific mode.
    -   **Preferred:** Directory (`.roo/rules-{modeSlug}/`)
    -   **Fallback:** Single file (`.roorules-{modeSlug}`)

Rules are loaded in order: Global, then Workspace.

## What Are Custom Instructions?

Custom Instructions define behaviors, preferences, and constraints like coding style, documentation standards, and workflow guidelines.

## Setting Custom Instructions

### Global Custom Instructions

Apply across all workspaces.

1.  **Open Prompts Tab:** Click the icon in the Roo Code top menu.
2.  **Find Section:** "Custom Instructions for All Modes".
3.  **Enter Instructions:** Add your text.
4.  **Save Changes:** Click "Done".

### Global Rules Directory

Reusable rules for all projects, supporting global and project-specific overrides.

#### Key Benefits

-   Set global coding standards.
-   Override rules per project.
-   Maintain consistency.
-   Update rules for all projects at once.

#### Directory Structure

Fixed locations:
-   **Linux/macOS:** `~/.roo/rules`
-   **Windows:** `%USERPROFILE%\.roo\rules`

#### Setting Up Global Rules

1.  **Create Directory:**
    ```bash
    # Linux/macOS
    mkdir -p ~/.roo/rules
    # Windows
    mkdir %USERPROFILE%\.roo\rules
    ```
2.  **Add General Rules** (e.g., `~/.roo/rules/coding-standards.md`):
    ```markdown
    # Global Coding Standards
    1. Always use TypeScript.
    2. Write unit tests for new functions.
    ```
3.  **Add Mode-Specific Rules** (e.g., `~/.roo/rules-code/typescript.md`):
    ```markdown
    # TypeScript Code Mode Rules
    1. Use strict mode in tsconfig.json.
    2. Prefer interfaces over type aliases.
    ```

#### Available Rule Directories

| Directory                 | Purpose                          |
| ------------------------- | -------------------------------- |
| `rules/`                  | General rules for all modes      |
| `rules-code/`             | Rules for Code mode              |
| `rules-architect/`        | Rules for Architect mode         |
| `rules-debug/`            | Rules for Debug mode             |
| `rules-{mode}/`           | Rules for any custom mode        |

#### Rule Loading Order

1.  Global Rules (`~/.roo/`)
2.  Project Rules (`.roo/`) - can override global.
3.  Legacy Files (`.roorules`, `.clinerules`)

Mode-specific rules load before general rules.

### Workspace-Level Instructions

Apply only within the current workspace.

#### Workspace-Wide Instructions

-   **Preferred:** Directory (`.roo/rules/`). Files are loaded recursively and alphabetically.
-   **Fallback:** Single file (`.roorules`). Used if `.roo/rules/` is empty or missing.

#### Mode-Specific Instructions

Set via the Prompts Tab or rule files.

1.  **Prompts Tab:**
    -   Open the Prompts tab.
    -   Select the mode to customize.
    -   Enter instructions in "Mode-specific Custom Instructions".
    -   Save changes.

2.  **Rule Files/Directories:**
    -   **Preferred:** Directory (`.roo/rules-{modeSlug}/`).
    -   **Fallback:** Single file (`.roorules-{modeSlug}`).

### How Instructions are Combined

Instructions are combined in the system prompt. Global rules load first, then workspace rules (which can override global). Directory-based rules take precedence over file-based fallbacks.

### Rules about `.rules` files

-   **Location:** Preferred: `.roo/rules/` and `.roo/rules-{modeSlug}/`. Fallback: `.roorules` and `.roorules-{modeSlug}` in the workspace root.
-   **Empty Files:** Silently skipped.
-   **Source Headers:** Each rule's content is included with a source header.
-   **Interaction:** Mode-specific rules complement global rules.

### Examples of Custom Instructions

-   "Use 4 spaces for indentation."
-   "Use camelCase for variable names."
-   "Write unit tests for all new functions."
-   "Explain reasoning before providing code."
-   "Ensure new website features are responsive and accessible."

### Pro Tip: Team Standardization

-   **Project Standards:** Use version-controlled `.roo/rules/` for project-specific consistency.
-   **Organization Standards:** Use global `~/.roo/rules/` for organization-wide standards.
-   **Hybrid Approach:** Combine global and workspace rules.

### Combining with Custom Modes

Combine with Custom Modes for specialized environments with specific tools, file restrictions, and instructions.
