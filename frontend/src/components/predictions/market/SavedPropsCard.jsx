import React from "react";

import MyPropsPanel from "../MyPropsPanel.jsx";

export default function SavedPropsCard({
  refreshNonce = 0,
  selectedDate = null,
}) {
  return (
    <MyPropsPanel
      refreshNonce={refreshNonce}
      selectedDate={selectedDate}
      title="Saved Props"
      exportPrefix="mlb_saved_props"
    />
  );
}
