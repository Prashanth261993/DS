import { Link, Route, Routes } from "react-router-dom";
import Live from "./Live";
import Health from "./Health";

export default function App() {
  return (
    <div style={{ fontFamily: "system-ui", background: "#111", color: "#eee", minHeight: "100vh", padding: 16 }}>
      <header style={{ display: "flex", gap: 16, alignItems: "center", marginBottom: 16 }}>
        <h2 style={{ margin: 0 }}>FluxTape</h2>
        <nav style={{ display: "flex", gap: 12 }}>
          <Link to="/" style={{ color: "#6cf" }}>Live</Link>
          <Link to="/health" style={{ color: "#6cf" }}>Dashboards</Link>
        </nav>
      </header>
      <Routes>
        <Route path="/" element={<Live />} />
        <Route path="/health" element={<Health />} />
      </Routes>
    </div>
  );
}
