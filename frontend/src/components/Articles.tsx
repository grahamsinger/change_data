import { useEffect, useState } from "react";
import { Article, jget } from "../api";

export default function Articles() {
  const [items, setItems] = useState<Article[]>([]);
  useEffect(() => {
    const refresh = async () => {
      try {
        setItems(await jget<Article[]>("/articles?limit=15"));
      } catch (_) { /* ignore */ }
    };
    refresh();
    const id = setInterval(refresh, 2000);
    return () => clearInterval(id);
  }, []);
  return (
    <div className="scroll">
      {items.length === 0 && <div style={{ color: "var(--muted)" }}>No articles yet.</div>}
      {items.map((a) => (
        <div key={a.id} className="article">
          <div className="title">#{a.id} {a.title}</div>
          <div className="meta">
            <span className="tag tag-mysql">{a.kind}</span> by <strong>{a.author}</strong>
            {" · "}{new Date(a.published_at).toLocaleTimeString()}
          </div>
          <div className="body">{a.body}</div>
        </div>
      ))}
    </div>
  );
}
