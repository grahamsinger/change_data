import Publish from "./components/Publish";
import SimToggle from "./components/SimToggle";
import Search from "./components/Search";
import Analytics from "./components/Analytics";
import Articles from "./components/Articles";

export default function App() {
  return (
    <div className="app">
      <div style={{ display: "flex", flexDirection: "column", gap: 12, minHeight: 0 }}>
        <div className="panel">
          <h2>Simulator</h2>
          <SimToggle />
        </div>
        <div className="panel">
          <h2>Publish</h2>
          <Publish />
        </div>
        <div className="panel" style={{ flex: 1, minHeight: 0 }}>
          <h2>Recent articles (from MySQL)</h2>
          <Articles />
        </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 12, minHeight: 0 }}>
        <div className="panel">
          <h2>Full-text search (Meilisearch)</h2>
          <Search />
        </div>
        <div className="panel" style={{ flex: 1, minHeight: 0 }}>
          <h2>Analytics</h2>
          <Analytics />
        </div>
      </div>
    </div>
  );
}
