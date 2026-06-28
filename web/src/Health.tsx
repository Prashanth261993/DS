import { useState } from "react";
import { GRAFANA } from "./api";

const TABS = [
  { id: "fluxtape-ingestion", label: "Ingestion" },
  { id: "fluxtape-processor", label: "Stream Processor" },
];

export default function Health() {
  const [tab, setTab] = useState(TABS[0].id);
  return (
    <div>
      <div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
        {TABS.map((t) => (
          <button key={t.id} onClick={() => setTab(t.id)}
            style={{ padding: "6px 12px", background: tab === t.id ? "#2a6" : "#333", color: "#fff", border: 0, borderRadius: 4 }}>
            {t.label}
          </button>
        ))}
        <a href={`${GRAFANA}/d/${tab}?kiosk`} target="_blank" style={{ marginLeft: "auto", color: "#6cf" }}>open in Grafana ↗</a>
      </div>
      <iframe title="grafana" src={`${GRAFANA}/d/${tab}?kiosk&refresh=5s`} style={{ width: "100%", height: 720, border: 0 }} />
    </div>
  );
}
