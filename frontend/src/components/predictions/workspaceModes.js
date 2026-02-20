export const WORKSPACE_MODE_RESEARCH = "research";
export const WORKSPACE_MODE_BOARD = "board";

export const MLB_WORKSPACE_MODES = [
  {
    id: WORKSPACE_MODE_RESEARCH,
    label: "Player Research",
    hint: "Single-player guided analysis",
  },
  {
    id: WORKSPACE_MODE_BOARD,
    label: "Market Board",
    hint: "Saved props and calendar view",
  },
];

export const NHL_WORKSPACE_MODES = [
  {
    id: WORKSPACE_MODE_RESEARCH,
    label: "Player Research",
    hint: "Evaluate leaders and model confidence",
  },
  {
    id: WORKSPACE_MODE_BOARD,
    label: "Market Board",
    hint: "Search and sort the full slate",
  },
];

export function isWorkspaceMode(value) {
  return value === WORKSPACE_MODE_RESEARCH || value === WORKSPACE_MODE_BOARD;
}
